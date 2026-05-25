package dk.ku.di.dms.vms.tpcc.proxy.experiment;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.tpcc.common.events.delivery.DeliveryIn;
import dk.ku.di.dms.vms.tpcc.common.events.new_order.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.order_status.OrderStatusIn;
import dk.ku.di.dms.vms.tpcc.common.events.payment.PaymentIn;
import dk.ku.di.dms.vms.tpcc.common.events.stock_level.StockLevelWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

public final class ExperimentUtils {

    private static final System.Logger LOGGER = System.getLogger(ExperimentUtils.class.getName());

    private static boolean CONSUMER_REGISTERED = false;

    private static int PREV_EXP_LAST_TID = 0;

    private static Semaphore[] SEMAPHORES;

    public static ExperimentStats runExperiment(Coordinator coordinator, Tuple<Integer, String>[] txRatio, List<Map<String, Iterator<Object>>> input, int runTime, int warmUp, int numTerminals, int pipelineSize) {

        // provide a consumer to avoid depending on the coordinator
        Function<Object, Long> inputResolverFunc = tpccInputBuilder(coordinator);

        if(CONSUMER_REGISTERED) {
            // clean up possible entries from previous run
            BATCH_TO_FINISHED_TS_MAP.keySet().stream().max(Long::compareTo).ifPresent(
                    highestKey -> PREV_EXP_LAST_TID = (int) BATCH_TO_FINISHED_TS_MAP.get(highestKey).lastTid);
            BATCH_TO_FINISHED_TS_MAP.clear();
        } else {
            coordinator.registerBatchCommitConsumer((batchId, lastTid) -> BATCH_TO_FINISHED_TS_MAP.put(
                    batchId, new BatchStats(batchId, lastTid, System.currentTimeMillis())));

            SEMAPHORES = new Semaphore[numTerminals];
            for (int i = 0; i < numTerminals; i++) {
                final Semaphore semaphore = new Semaphore(0);
                SEMAPHORES[i] = semaphore;
                coordinator.registerBatchCommitConsumer((_, _) -> semaphore.release());
            }

            CONSUMER_REGISTERED = true;
        }

        int fullRuntime = runTime + warmUp;
        WorkloadUtils.WorkloadStats workloadStats = WorkloadUtils.submitWorkload(txRatio, input, inputResolverFunc, fullRuntime, numTerminals, pipelineSize, SEMAPHORES);

        // avoid submitting after experiment termination
        coordinator.clearTransactionInputs();
        LOGGER.log(INFO,"Transaction input queue(s) cleared.");

        long numTIDsSubmitted = coordinator.getNumTIDsSubmitted(PREV_EXP_LAST_TID + 1);
        long actualRunTime = workloadStats.actualEndTs() - workloadStats.initTs();
        double txSubPerSec = numTIDsSubmitted / ((double) actualRunTime / 1000L);

        if(BATCH_TO_FINISHED_TS_MAP.isEmpty()) {
            LOGGER.log(WARNING, "No batch of transactions completed!");
            return new ExperimentStats(workloadStats.initTs(), runTime, 0, numTIDsSubmitted, txSubPerSec, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        long endTs = workloadStats.initTs() + fullRuntime;
        long initTs = workloadStats.initTs() + warmUp;
        int numCompletedWithWarmUp;
        int numCompletedDuringWarmUp = 0;
        int numCompleted;
        List<Long> allLatencies = new ArrayList<>();

        // find first batch that runs transactions after warm up
        BatchStats prevBatchStats = null;
        for(Map.Entry<Long, ExperimentUtils.BatchStats> batchStat : BATCH_TO_FINISHED_TS_MAP.entrySet()){
            if(batchStat.getValue().endTs < initTs) {
                prevBatchStats = batchStat.getValue();
                numCompletedDuringWarmUp = (int) prevBatchStats.lastTid - PREV_EXP_LAST_TID;
                continue;
            }
            break;
        }

        // if none, consider the first batch as the warmup, unless warmup is 0
        if(prevBatchStats == null) {
            Long lowestKey = BATCH_TO_FINISHED_TS_MAP.keySet().stream().min(Long::compareTo).orElse(null);
            prevBatchStats = BATCH_TO_FINISHED_TS_MAP.get(lowestKey);
        }

        BatchStats firstBatchStats = prevBatchStats;

        // calculate latency based on the batch
        while(BATCH_TO_FINISHED_TS_MAP.containsKey(prevBatchStats.batchId+1)){
            BatchStats currBatchStats = BATCH_TO_FINISHED_TS_MAP.get(prevBatchStats.batchId+1);
            if(currBatchStats.endTs > endTs) break;
            allLatencies.add(currBatchStats.endTs - prevBatchStats.endTs);
            prevBatchStats = currBatchStats;
        }

        numCompletedWithWarmUp = (int) prevBatchStats.lastTid - PREV_EXP_LAST_TID;
        numCompleted = numCompletedWithWarmUp - numCompletedDuringWarmUp;
        long usefulRuntime = prevBatchStats.endTs - firstBatchStats.endTs;

        double average = allLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        allLatencies.sort(null);
        double percentile_50 = calculatePercentile(allLatencies, 0.50);
        double percentile_75 = calculatePercentile(allLatencies, 0.75);
        double percentile_90 = calculatePercentile(allLatencies, 0.90);
        double percentile_99 = calculatePercentile(allLatencies, 0.99);
        // considering fixed experiment time
        double txPerSec = numCompleted / ((double) runTime / 1000L);
        // considering first received batch result
        double txPerSecUseful = numCompleted / ((double) usefulRuntime / 1000L);

        System.out.println("Number of submitted transactions: "+ numTIDsSubmitted);
        System.out.println("Transaction submission throughput (tx/sec): "+ txSubPerSec);
        System.out.println();

        System.out.println("Average latency: "+ average);
        System.out.println("Latency at 50th percentile: "+ percentile_50);
        System.out.println("Latency at 75th percentile: "+ percentile_75);
        System.out.println("Latency at 90th percentile: "+ percentile_90);
        System.out.println("Latency at 99th percentile: "+ percentile_99);

        System.out.println();
        System.out.println("Number of completed transactions (during warm up): "+ numCompletedDuringWarmUp);
        System.out.println("Number of completed transactions (after warm up): "+ numCompleted);
        System.out.println("Number of completed transactions (total): "+ numCompletedWithWarmUp);
        System.out.println("Total runtime (ms): "+ runTime);
        System.out.println("Transactions per second: "+txPerSec);

        System.out.println();
        // useful work: from first actual batch (the first after warm up)
        System.out.println("Useful work runtime (ms): "+ usefulRuntime);
        System.out.println("Transactions per second (useful work): "+txPerSecUseful);
        System.out.println();

        return new ExperimentStats(workloadStats.initTs(), runTime, usefulRuntime, numTIDsSubmitted, txSubPerSec, numCompletedWithWarmUp, numCompleted, txPerSec, txPerSecUseful, average, percentile_50, percentile_75, percentile_90, percentile_99);
    }

    public record ExperimentStats(long initTs, int runTime, long usefulRuntime, long numSubmitted, double txSubPerSec, int numCompletedWithWarmUp, int numCompleted, double txPerSec, double txPerSecUseful, double average, double percentile_50, double percentile_75, double percentile_90, double percentile_99){}

    public static void writeResultsToFile(int numWare, ExperimentStats expStats, int runTime, int warmUp, int numTransactionWorkers, int batchWindow, int maxTransactionsPerBatch, Tuple<Integer, String>[] txRatio, Map<String, Integer> numTxInputPerType, String logging, String checkpointing){
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(expStats.initTs),  ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd_MM_yy_HH_mm_ss");
        String formattedDate = time.format(formatter);
        String fileName = "tpcc_" + formattedDate + ".txt";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("======= TPC-C in vMODB =======");
            writer.newLine();
            writer.write("Start: " + formattedDate);
            writer.newLine();
            writer.write("Duration (ms): " + runTime);
            writer.newLine();
            writer.write("Warm up (ms): " + warmUp);
            writer.newLine();
            writer.write("Useful work (ms): " + expStats.usefulRuntime);
            writer.newLine();
            writer.write("Batch window (ms): " + batchWindow);
            writer.newLine();
            writer.write("Max transactions per batch: " +maxTransactionsPerBatch);
            writer.newLine();
            writer.write("Number of transaction workers: " + numTransactionWorkers);
            writer.newLine();
            writer.write("Number of warehouses: " + numWare);
            writer.newLine();

            writer.write("Transaction input size: ");
            writer.newLine();
            for(Map.Entry<String, Integer> tx : numTxInputPerType.entrySet()){
                if(tx.getValue() <= 0) continue;
                writer.write("  "+tx.getKey()+"=" + tx.getValue());
                writer.newLine();
            }

            writer.write("Transaction ratio: ");
            writer.newLine();
            for(Tuple<Integer, String> tx : txRatio){
                writer.write("  "+tx.t2+"=" + tx.t1);
                writer.newLine();
            }

            writer.newLine();
            writer.write("Logging: "+logging);
            writer.newLine();
            writer.write("Checkpointing: "+checkpointing);
            writer.newLine();
            writer.newLine();

            writer.write("Number of submitted transactions: "+ expStats.numSubmitted);
            writer.newLine();
            writer.write("Transaction submission throughput (tx/sec): "+ expStats.txSubPerSec);
            writer.newLine();

            writer.newLine();
            writer.write("Average latency: "+ expStats.average);
            writer.newLine();
            writer.write("Latency at 50th percentile: "+ expStats.percentile_50);
            writer.newLine();
            writer.write("Latency at 75th percentile: "+ expStats.percentile_75);
            writer.newLine();
            writer.write("Latency at 90th percentile: "+ expStats.percentile_90);
            writer.newLine();
            writer.write("Latency at 99th percentile: "+ expStats.percentile_99);
            writer.newLine();
            writer.write("Number of completed transactions (during warm up): "+ (expStats.numCompletedWithWarmUp - expStats.numCompleted));
            writer.newLine();
            writer.write("Number of completed transactions (after warm up): "+ expStats.numCompleted);
            writer.newLine();
            writer.write("Number of completed transactions (total): "+ expStats.numCompletedWithWarmUp);
            writer.newLine();
            writer.write("Transaction throughput (tx/sec): "+expStats.txPerSec);
            writer.newLine();
            writer.write("Useful transaction throughput (tx/sec): "+expStats.txPerSecUseful);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<Long, BatchStats> BATCH_TO_FINISHED_TS_MAP = new ConcurrentHashMap<>();

    private record BatchStats(long batchId, long lastTid, long endTs){}

    private static Function<Object, Long> tpccInputBuilder(final Coordinator coordinator) {
        return input -> {
            TransactionInput.Event eventPayload;
            String txIdentifier;
            switch (input) {
                case NewOrderWareIn newOrderInput -> {
                    txIdentifier = "new_order";
                    eventPayload = new TransactionInput.Event("new-order-ware-in", newOrderInput.toString());
                }
                case PaymentIn paymentInput -> {
                    txIdentifier = "payment";
                    eventPayload = new TransactionInput.Event("payment-in", paymentInput.toString());
                }
                case OrderStatusIn orderStatusInput -> {
                    txIdentifier = "order_status";
                    eventPayload = new TransactionInput.Event("order-status-in", orderStatusInput.toString());
                }
                case DeliveryIn deliveryInput -> {
                    txIdentifier = "delivery";
                    eventPayload = new TransactionInput.Event("delivery-in", deliveryInput.toString());
                }
                case StockLevelWareIn stockLevelWareInput -> {
                    txIdentifier = "stock_level";
                    eventPayload = new TransactionInput.Event("stock-level-ware-in", stockLevelWareInput.toString());
                }
                case null, default -> throw new IllegalStateException("Invalid input type: " + input);
            }
            TransactionInput txInput = new TransactionInput(txIdentifier, eventPayload);
            coordinator.queueTransactionInput(txInput);
            return (long) BATCH_TO_FINISHED_TS_MAP.size() + 1;
        };
    }

    public static Coordinator loadCoordinator(Properties properties) {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        // new order
        TransactionDAG newOrderDag = TransactionBootstrap.name("new_order")
                .input("a", "warehouse", "new-order-ware-in")
                .internal("b", "inventory", "new-order-ware-out", "a")
                .terminal("c", "order", "b")
                .build();
        transactionMap.put(newOrderDag.name, newOrderDag);

        // payment
        TransactionDAG paymentDag = TransactionBootstrap.name("payment")
                .input("a", "warehouse", "payment-in")
                .terminal("b", "order", "a")
                .build();
        transactionMap.put(paymentDag.name, paymentDag);

        // order status
        TransactionDAG orderStatusDag = TransactionBootstrap.name("order_status")
                .input("a", "warehouse", "order-status-in")
                .terminal("b", "order", "a")
                .build();
        transactionMap.put(orderStatusDag.name, orderStatusDag);

        // stock level
        TransactionDAG stockLevelDag = TransactionBootstrap.name("stock_level")
                .input("a", "warehouse", "stock-level-ware-in")
                .internal("b", "order", "stock-level-ware-out", "a")
                .terminal("c", "inventory", "b")
                .build();
        transactionMap.put(stockLevelDag.name, stockLevelDag);

        // delivery
        TransactionDAG deliveryDag = TransactionBootstrap.name("delivery")
                .input("a", "order", "delivery-in")
                .terminal("b", "warehouse", "a")
                .build();
        transactionMap.put(deliveryDag.name, deliveryDag);

        Map<String, IdentifiableNode> starterVMSs = getVmsMap(properties);
        Coordinator coordinator = Coordinator.build(properties, starterVMSs, transactionMap, (ignored1) -> IHttpHandler.DEFAULT);
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
        return coordinator;
    }

    private static Map<String, IdentifiableNode> getVmsMap(Properties properties) {
        String warehouseHost = properties.getProperty("warehouse_host");
        String inventoryHost = properties.getProperty("inventory_host");
        String orderHost = properties.getProperty("order_host");
        if(warehouseHost == null) throw new RuntimeException("Warehouse host is null");
        if(inventoryHost == null) throw new RuntimeException("Inventory host is null");
        if(orderHost == null) throw new RuntimeException("Order host is null");
        IdentifiableNode warehouseAddress = new IdentifiableNode("warehouse", warehouseHost, 8001);
        IdentifiableNode inventoryAddress = new IdentifiableNode("inventory", inventoryHost, 8002);
        IdentifiableNode orderAddress = new IdentifiableNode("order", orderHost, 8003);
        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();
        starterVMSs.putIfAbsent(warehouseAddress.identifier, warehouseAddress);
        starterVMSs.putIfAbsent(inventoryAddress.identifier, inventoryAddress);
        starterVMSs.putIfAbsent(orderAddress.identifier, orderAddress);
        return starterVMSs;
    }

    /**
     * The data must be sorted
     */
    public static double calculatePercentile(List<Long> data, double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Percentile must be between 0 and 1.");
        }
        if (data == null || data.isEmpty()) {
            return 0;
        }

        double rank = percentile * (data.size() - 1);

        int lowerIndex = (int) Math.floor(rank);
        int upperIndex = (int) Math.ceil(rank);

        if (lowerIndex == upperIndex) {
            return data.get(lowerIndex);
        }
        double weight = rank - lowerIndex;
        return data.get(lowerIndex) * (1 - weight) + data.get(upperIndex) * weight;
    }

}
