package dk.ku.di.dms.vms.marketplace.test;

import dk.ku.di.dms.vms.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionWorker;
import dk.ku.di.dms.vms.coordinator.vms.IVmsWorker;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.Thread.sleep;

/**
 * Measure the capacity of the transaction worker to generate transaction inputs without having to deploy the full system.
 * This allows verifying the scalability of the batch generation multi-thread algorithm.
 * Ideally should on par with the http server input rate.
 */
public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    private static final List<String> OM_VMSes = Arrays.asList("cart","product","stock","order","payment","shipment","seller");
    private static final List<String> TPCC_VMSes = Arrays.asList("warehouse","inventory","order");

    public static void main(String[] args) throws Exception {

        boolean isTpcc;
        int num_transaction_workers;
        int num_vms_workers;
        int num_max_transactions_batch;
        int batch_window_ms;
        int duration;

        if(args.length == 0) {
            Properties properties = ConfigUtils.loadProperties();
            System.out.println("Properties: \n" + properties);
            String app = properties.getProperty("benchmark");
            if(app == null) {
                throw new Exception("Missing benchmark property");
            }
            isTpcc = app.equalsIgnoreCase("tpcc");
            if(!isTpcc && !app.equalsIgnoreCase("online_marketplace")) {
                throw new Exception("Benchmark type not recognized: "+app);
            }
            // must ideally scale together
            num_transaction_workers = Integer.parseInt( properties.getProperty("num_transaction_workers") );
            num_vms_workers = Integer.parseInt( properties.getProperty("num_vms_workers") );
            // can potentially hide the wait time introduced by the "ring"
            num_max_transactions_batch = Integer.parseInt( properties.getProperty("num_max_transactions_batch") );
            batch_window_ms = Integer.parseInt( properties.getProperty("batch_window_ms"));
            duration = Integer.parseInt( properties.getProperty("duration") );
        } else {
            String app = args[0];
            isTpcc = app.equalsIgnoreCase("tpcc");
            if(!isTpcc && !app.equalsIgnoreCase("online_marketplace")) {
                throw new Exception("Benchmark type not recognized: "+app);
            }
            num_transaction_workers = Integer.parseInt( args[1] );
            num_vms_workers = Integer.parseInt( args[2] );
            num_max_transactions_batch = Integer.parseInt( args[3] );
            batch_window_ms = Integer.parseInt( args[4] );
            duration = Integer.parseInt( args[5] );
        }

        System.out.println("Experiment config: \n"+
                " Benchmark = "+(isTpcc ? "tpcc" : "online_marketplace") +"\n"+
                " Num transaction workers = "+ (num_transaction_workers)+"\n"+
                " Num vms workers = "+ (num_transaction_workers)+"\n"+
                " Num max transactions per batch = "+ (num_max_transactions_batch)+"\n"+
                " Batch window (ms) = "+batch_window_ms+"\n"+
                " Duration (ms) = "+ (duration)+"\n"
        );

        List<Deque<TransactionInput>> txInputQueues = generateTransactionInputs(isTpcc, num_transaction_workers);

        List<String> VMSes;
        Map<String, TransactionDAG> transactionMap;
        if(isTpcc){
            VMSes = TPCC_VMSes;
            transactionMap = buildTpccTransactionDAG();
        } else {
            VMSes = OM_VMSes;
            transactionMap = buildOmTransactionDAGs();
        }

        Map<String, IVmsWorker> vmsWorkers = new HashMap<>();
        for (String vms : VMSes) {
            BaseMicroBenchVmsWorker vmsWorker;
            if (num_vms_workers > 1) {
                vmsWorker = new ComplexMicroBenchVmsWorker(num_vms_workers);
            } else {
                vmsWorker = new SimpleMicroBenchVmsWorker();
            }
            vmsWorkers.put(vms, vmsWorker);
        }

        Deque<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();
        System.out.println("Setting up "+num_transaction_workers+" worker threads");
        List<TransactionWorker> workers = setupTransactionWorkers(num_transaction_workers, num_max_transactions_batch, batch_window_ms,
                VMSes, transactionMap, vmsWorkers, coordinatorQueue, txInputQueues);

        System.out.println("Initializing "+num_transaction_workers+" worker threads");
        ThreadFactory threadFactory = Thread.ofPlatform().factory();
        for (TransactionWorker worker : workers) {
            Thread thread = threadFactory.newThread(worker);
            thread.start();
        }

        // warm up
        sleep(30000);

        for (TransactionWorker worker : workers) {
            worker.clearInputQueue();
        }
        for (IVmsWorker vmsWorker : vmsWorkers.values()) {
            ((BaseMicroBenchVmsWorker)vmsWorker).clearInputQueue();
        }

        System.out.println("Warm up done!");

        // actual run
        sleep(duration);

        for (TransactionWorker worker : workers) {
            worker.clearInputQueue();
            worker.stop();
        }

        long numPayloads = vmsWorkers.values().stream().mapToLong(iVmsWorker -> ((BaseMicroBenchVmsWorker) iVmsWorker).getNumPayloads()).sum();
        long numBatches = coordinatorQueue.size();

        //  add batches per second. count num of batches in the queue
        System.out.println("Experiment finished: \n"+
                " Payloads per second: "+ (numPayloads/((double) duration/1000L))+"\n"+
                " Batches per second: "+ (numBatches/((double) duration/1000L))
        );

        writeResultsToFile(isTpcc, num_transaction_workers, num_vms_workers, num_max_transactions_batch, batch_window_ms, duration, numPayloads, numBatches);

        System.exit(0);
    }

    public static void writeResultsToFile(boolean isTpcc, int num_transaction_workers, int num_vms_workers, int num_max_transactions_batch,  int batch_window_ms, int duration, long numPayloads, long numBatches){
        LocalDateTime time = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd_MM_yy_HH_mm_ss");
        String formattedDate = time.format(formatter);
        String fileName = "microbench_" + formattedDate + ".txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("======= MicroBench vMODB =======");
            writer.newLine();
            writer.write("Bench: " + (isTpcc ? "tpcc" : "online_marketplace"));
            writer.newLine();
            writer.write("Start: " + formattedDate);
            writer.newLine();
            writer.write("Duration (ms): " + duration);
//            writer.newLine();
//            writer.write("Warm up (ms): " + warmUp);
            writer.newLine();
            writer.write("Batch window (ms): " + batch_window_ms);
            writer.newLine();
            writer.write("Max transactions per batch: " +num_max_transactions_batch);
            writer.newLine();
            writer.write("Number of transaction workers: " + num_transaction_workers);
            writer.newLine();
            writer.write("Number of VMS workers: " + num_vms_workers);
            writer.newLine();
            writer.write("Number of completed transactions: "+ numPayloads);
            writer.newLine();
            writer.write("Throughput (tx/sec): "+(numPayloads/((double) duration/1000L)));
            writer.newLine();
            writer.write("Number of completed batches: "+ numBatches);
            writer.newLine();
            writer.write("Throughput (batches/sec): "+(numBatches/((double) duration/1000L)));
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Deque<TransactionInput>> generateTransactionInputs(final boolean tpcc, final int num_transaction_workers) {
        List<Deque<TransactionInput>> txInputQueues = new ArrayList<>(num_transaction_workers);
        for (int i = 1; i <= num_transaction_workers; i++) {
            final Deque<TransactionInput> inputQueue = new TpccInputGenDeque();
            txInputQueues.add(inputQueue);
        }
        return txInputQueues;
    }

    private static List<Deque<TransactionInput>> generateTransactionInputs_(final boolean tpcc, final int num_transaction_workers) {

        int NUM_QUEUED_TRANSACTIONS = 1;
        List<Deque<TransactionInput>> txInputQueues = new ArrayList<>(num_transaction_workers);
        // fill each queue with proper number of transaction inputs
        String defaultPayload = String.valueOf(0);

        ForkJoinPool pool = ForkJoinPool.commonPool();
        Future<?>[] futures = new Future[num_transaction_workers];

        for (int i = 1; i <= num_transaction_workers; i++) {
           final Deque<TransactionInput> inputQueue = new ConcurrentLinkedDeque<>();
           txInputQueues.add(inputQueue);
            int finalI = i;
            futures[i-1] = pool.submit(() -> {
                System.out.println("Generating " + NUM_QUEUED_TRANSACTIONS + " transactions for worker # " + finalI);
                for (int j = 1; j <= NUM_QUEUED_TRANSACTIONS; j++) {
                    if (tpcc) {
                        inputQueue.add(
                                new TransactionInput("new_order",
                                        new TransactionInput.Event("new-order-ware-in",
                                                defaultPayload)));
                        continue;
                    }
                    int idx = ThreadLocalRandom.current().nextInt(1, 101);
                    if (idx <= 73) {
                        inputQueue.add(
                                new TransactionInput(CUSTOMER_CHECKOUT,
                                        new TransactionInput.Event(CUSTOMER_CHECKOUT,
                                                defaultPayload)));
                    } else if (idx <= 86) {
                        inputQueue.add(
                                new TransactionInput(UPDATE_PRODUCT,
                                        new TransactionInput.Event(UPDATE_PRODUCT,
                                                defaultPayload)));
                    } else {
                        inputQueue.add(
                                new TransactionInput(UPDATE_PRICE,
                                        new TransactionInput.Event(UPDATE_PRICE,
                                                defaultPayload)));
                    }
                }
            });
           System.out.println(NUM_QUEUED_TRANSACTIONS+" transactions generated for worker # "+i);
        }

        try {
            for (int i = 1; i <= num_transaction_workers; i++) {
                futures[i-1].get();
            }
        } catch(ExecutionException | InterruptedException e){
            LOGGER.log(ERROR, "Error:\n"+e);
        }

        return txInputQueues;
    }

    private static abstract class BaseMicroBenchVmsWorker implements IVmsWorker {
        abstract long getNumPayloads();
        abstract void clearInputQueue();
    }

    private static class ComplexMicroBenchVmsWorker extends BaseMicroBenchVmsWorker {

        private final LongAdder[] adders;
//        private final List<Queue<TransactionEvent.PayloadRaw>> payloadQueues;
//        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
//        private final Queue<Object> messageQueue = new ConcurrentLinkedDeque<>();
        private final int numVmsWorkers;

        public ComplexMicroBenchVmsWorker(int numVmsWorkers){
            this.numVmsWorkers = numVmsWorkers;
            this.adders = new LongAdder[numVmsWorkers];
            for (int i = 0; i < numVmsWorkers; i++) {
                this.adders[i] = new LongAdder();
            }
        }

        @Override
        long getNumPayloads() {
            long sum = 0;
            for (var adder : this.adders) {
                sum += adder.sum();
            }
            return sum;
        }

        @Override
        void clearInputQueue() {
            for (var adder : this.adders) {
                adder.reset();
            }
        }

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
            int idx = ThreadLocalRandom.current().nextInt(0, this.numVmsWorkers);
            this.adders[idx].increment();
        }

        @Override
        public void queueMessage(Object message) {
//            this.messageQueue.add(message);
        }
    }

    /**
     * Queuing payloads can lead to contention for multiple transaction workers
     */
    private static class SimpleMicroBenchVmsWorker extends BaseMicroBenchVmsWorker {

        private final LongAdder counter = new LongAdder();
//        private final Queue<TransactionEvent.PayloadRaw> payloadQueue = new ConcurrentLinkedDeque<>();
//        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
//        private final Queue<Object> messageQueue = new ConcurrentLinkedDeque<>();

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
            // must add the overhead of adding to vms worker queue at least
            // this.payloadQueue.add(payloadRaw);
            this.counter.increment();
        }
        @Override
        public void queueMessage(Object message) {
            // this.messageQueue.add(message);
        }

        public long getNumPayloads(){
            return this.counter.sum();
        }

        @Override
        void clearInputQueue() {
            this.counter.reset();
        }

    }

    private static List<TransactionWorker> setupTransactionWorkers(int numWorkers, int maxNumTidBatch, int batch_windows_ms,
                                                                   List<String> VMSes,
                                                                   Map<String, TransactionDAG> transactionMap,
                                                                   Map<String,IVmsWorker> workers,
                                                                   Queue<Object> coordinatorQueue,
                                                                   List<Deque<TransactionInput>> txInputQueues){
        List<TransactionWorker> txWorkers = new ArrayList<>();
        var vmsMetadataMap = new HashMap<String, VmsNode>();
        int i = 0;
        for(var vms : VMSes) {
            vmsMetadataMap.put(vms, new VmsNode("localhost", 8080 + i, vms, 0, 0, 0, null, null, null));
            i++;
        }

        Map<String, VmsNode[]> vmsNodePerDAG = new HashMap<>();
        for(var dag : transactionMap.entrySet()) {
            vmsNodePerDAG.put(dag.getKey(), BatchAlgo.buildTransactionDagVmsList(dag.getValue(), vmsMetadataMap));
        }

        // generic algorithm to handle N number of transaction workers
        int idx = 1;
        long initTid = 1;

        var firstPrecedenceInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        var precedenceMapInputQueue = firstPrecedenceInputQueue;
        ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>> precedenceMapOutputQueue;

        Map<String, TransactionWorker.PrecedenceInfo> precedenceMap = new HashMap<>();
        for(var vms : VMSes) {
            precedenceMap.put(vms, new TransactionWorker.PrecedenceInfo(0, 0, 0));
            precedenceMapInputQueue.add(precedenceMap);
        }

        var serdesProxy = VmsSerdesProxyBuilder.build();
        do {
            if(idx < numWorkers){
                precedenceMapOutputQueue = new ConcurrentLinkedDeque<>();
            } else {
                precedenceMapOutputQueue = firstPrecedenceInputQueue;
            }
            var txWorker = TransactionWorker.build(idx, txInputQueues.get(idx-1), initTid, maxNumTidBatch, batch_windows_ms,
                    numWorkers, precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap,
                    vmsNodePerDAG, workers, coordinatorQueue, serdesProxy, false);
            initTid = initTid + maxNumTidBatch;
            precedenceMapInputQueue = precedenceMapOutputQueue;
            idx++;
            txWorkers.add(txWorker);
        } while (idx <= numWorkers);
        return txWorkers;
    }

    private static Map<String, TransactionDAG> buildTpccTransactionDAG(){
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG newOrderDag = TransactionBootstrap.name("new_order")
                .input("a", "warehouse", "new-order-ware-in")
                .internal("b", "inventory", "new-order-ware-out", "a")
                .terminal("c", "order", "b")
                .build();
        transactionMap.put(newOrderDag.name, newOrderDag);
        return transactionMap;
    }

    private static Map<String, TransactionDAG> buildOmTransactionDAGs(){
        Map<String, TransactionDAG> transactionMap = new HashMap<>();

        TransactionDAG updatePriceDag = TransactionBootstrap.name(UPDATE_PRICE)
                .input("a", "product", UPDATE_PRICE)
                .terminal("b", "cart", "a")
                .build();
        transactionMap.put(updatePriceDag.name, updatePriceDag);

        TransactionDAG updateProductDag = TransactionBootstrap.name(UPDATE_PRODUCT)
                .input("a", "product", UPDATE_PRODUCT)
                .terminal("b", "stock", "a")
                .terminal("c", "cart", "a")
                .build();
        transactionMap.put(updateProductDag.name, updateProductDag);

        TransactionDAG checkoutDag = TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                .input("a", "cart", CUSTOMER_CHECKOUT)
                .internal("b", "stock", RESERVE_STOCK, "a")
                .internal("c", "order", STOCK_CONFIRMED, "b")
                .internal("d", "payment", INVOICE_ISSUED, "c")
                .internal("e", "seller", INVOICE_ISSUED, "c")
                .terminal("f", "shipment", "d")
                .build();
        transactionMap.put(checkoutDag.name, checkoutDag);

        return transactionMap;
    }

}
