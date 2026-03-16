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

import java.util.*;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.Thread.sleep;

/**
 * measure the capacity of the transaction worker to generate transaction inputs
 * without having to deploy the full system
 * that allows to verify the scalability of the batch generation multi-thread algorithm
 * ideally should on par with the http server input rate
 */
public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    private static final List<String> OM_VMSes = Arrays.asList("cart","product","stock","order","payment","shipment","seller");
    private static final List<String> TPCC_VMSes = Arrays.asList("warehouse","inventory","order");

    private static final int NUM_QUEUED_TRANSACTIONS = 5000000;

    public static void main(String[] args) throws Exception {

        boolean isTpcc;
        int num_vms_workers;
        int num_transaction_workers;
        int num_max_transactions_batch;
        int max_time;
        int batch_window_ms;

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
            num_vms_workers = Integer.parseInt( properties.getProperty("num_vms_workers") );
            num_transaction_workers = Integer.parseInt( properties.getProperty("num_transaction_workers") );
            // can potentially hide the wait time introduced by the "ring"
            num_max_transactions_batch = Integer.parseInt( properties.getProperty("num_max_transactions_batch") );
            batch_window_ms = Integer.parseInt( properties.getProperty("batch_window_ms"));
            max_time = Integer.parseInt( properties.getProperty("max_time") );
        } else {
            String app = args[0];
            isTpcc = app.equalsIgnoreCase("tpcc");
            if(!isTpcc && !app.equalsIgnoreCase("online_marketplace")) {
                throw new Exception("Benchmark type not recognized: "+app);
            }
            num_vms_workers = Integer.parseInt( args[1] );
            num_transaction_workers = Integer.parseInt( args[2] );
            num_max_transactions_batch = Integer.parseInt( args[3] );
            batch_window_ms = Integer.parseInt( args[4] );
            max_time = Integer.parseInt( args[5] );
        }

        if(batch_window_ms > max_time) {
            throw new Exception("Batch window time is larger than max time");
        }

        System.out.println("Experiment config: \n"+
                " Benchmark = "+(isTpcc ? "tpcc" : "online_marketplace") +"\n"+
                " Num vms workers = "+ (num_transaction_workers)+"\n"+
                " Num transaction workers = "+ (num_transaction_workers)+"\n"+
                " Num max transaction batch = "+ (num_max_transactions_batch)+"\n"+
                " Batch window (ms) = "+batch_window_ms+"\n"+
                " Max time (ms) = "+ (max_time)+"\n"
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
        for (var vms : VMSes) {
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
        List<Thread> threads = new ArrayList<>();
        for (TransactionWorker worker : workers) {
            Thread thread = threadFactory.newThread(worker);
            threads.add(thread);
            thread.start();
        }

        sleep(max_time);

        for (var thread : threads) {
            thread.interrupt();
        }

        long numPayloads = vmsWorkers.values().stream().mapToInt(iVmsWorker -> ((BaseMicroBenchVmsWorker) iVmsWorker).getNumPayloads()).sum();
        long numBatches = coordinatorQueue.size();

        for(var worker : workers){
            worker.stop();
        }

        //  add batches per second. count num of batches in the queue
        System.out.println("Experiment finished: \n"+
                " Payloads per second: "+ (numPayloads/(max_time/1000))+"\n"+
                " Batches per second: "+ (numBatches/(max_time/1000))
        );

        System.exit(0);
    }

    private static List<Deque<TransactionInput>> generateTransactionInputs(final boolean tpcc, final int num_transaction_workers) {
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
        abstract int getNumPayloads();
    }

    private static class ComplexMicroBenchVmsWorker extends BaseMicroBenchVmsWorker {

        private final List<Queue<TransactionEvent.PayloadRaw>> payloadQueues;
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private final Queue<Object> messageQueue = new ConcurrentLinkedDeque<>();
        private final int numVmsWorkers;

        public ComplexMicroBenchVmsWorker(int numVmsWorkers){
            this.numVmsWorkers = numVmsWorkers;
            this.payloadQueues = new ArrayList<>(numVmsWorkers);
            for (int i = 0; i < numVmsWorkers; i++) {
                this.payloadQueues.add(new ConcurrentLinkedDeque<>());
            }
        }

        @Override
        int getNumPayloads() {
            int sum = 0;
            for (Queue<TransactionEvent.PayloadRaw> queue: this.payloadQueues) {
                sum += queue.size();
            }
            return sum;
        }

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
            int idx = ThreadLocalRandom.current().nextInt(0, this.numVmsWorkers);
            this.payloadQueues.get(idx).add(payloadRaw);
        }

        @Override
        public void queueMessage(Object message) {
            this.messageQueue.add(message);
        }
    }

    /**
     * Queuing payloads can lead to contention for multiple transaction workers
     */
    private static class SimpleMicroBenchVmsWorker extends BaseMicroBenchVmsWorker {

        private final Queue<TransactionEvent.PayloadRaw> payloadQueue = new ConcurrentLinkedDeque<>();
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        private final Queue<Object> messageQueue = new ConcurrentLinkedDeque<>();

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
            // must add the overhead of adding to vms worker queue at least
            this.payloadQueue.add(payloadRaw);
        }
        @Override
        public void queueMessage(Object message) {
            this.messageQueue.add(message);
        }

        public int getNumPayloads(){
            return this.payloadQueue.size();
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
                    vmsNodePerDAG, workers, coordinatorQueue, serdesProxy);
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
