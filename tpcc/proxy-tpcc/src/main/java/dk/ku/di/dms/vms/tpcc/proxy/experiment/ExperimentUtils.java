package dk.ku.di.dms.vms.tpcc.proxy.experiment;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;
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
import java.util.function.Function;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

public final class ExperimentUtils {

    private static final System.Logger LOGGER = System.getLogger(ExperimentUtils.class.getName());

    private static boolean CONSUMER_REGISTERED = false;

    private static int lastExperimentLastTID = 0;

    public static ExperimentStats runExperiment(Coordinator coordinator, Tuple<Integer, String>[] txRatio, List<Map<String, Iterator<Object>>> input, int runTime, int warmUp) {

        // provide a consumer to avoid depending on the coordinator
        Function<Object, Long> func = tpccInputBuilder(coordinator);

        if(CONSUMER_REGISTERED) {
            // clean up possible entries from previous run
            BATCH_TO_FINISHED_TS_MAP.keySet().stream().max(Long::compareTo).ifPresent(
                    highestKey -> lastExperimentLastTID = (int) BATCH_TO_FINISHED_TS_MAP.get(highestKey).lastTid);
            BATCH_TO_FINISHED_TS_MAP.clear();
        } else {
            coordinator.registerBatchCommitConsumer((batchId, tid) -> BATCH_TO_FINISHED_TS_MAP.put(
                    batchId,
                    new BatchStats(batchId, tid, System.currentTimeMillis())));
            CONSUMER_REGISTERED = true;
        }

        int newRuntime = runTime + warmUp;
        WorkloadUtils.WorkloadStats workloadStats = WorkloadUtils.submitWorkload(txRatio, input, func, newRuntime);

        // avoid submitting after experiment termination
        // coordinator.clearTransactionInputs();
        // LOGGER.log(INFO,"Transaction input queue(s) cleared.");

        if(BATCH_TO_FINISHED_TS_MAP.isEmpty()) {
            LOGGER.log(WARNING, "No batch of transactions completed!");
            return new ExperimentStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        long endTs = workloadStats.initTs() + newRuntime;
        long initTs = workloadStats.initTs() + warmUp;
        int numCompletedWithWarmUp;
        int numCompletedDuringWarmUp = 0;
        int numCompleted;
        List<Long> allLatencies = new ArrayList<>();

        // find first batch that runs transactions after warm up
        BatchStats prevBatchStats = null;
        for(var batchStat : BATCH_TO_FINISHED_TS_MAP.entrySet()){
            if(batchStat.getValue().endTs < initTs) {
                prevBatchStats = batchStat.getValue();
                numCompletedDuringWarmUp = (int) prevBatchStats.lastTid - lastExperimentLastTID;
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

        numCompletedWithWarmUp = (int) prevBatchStats.lastTid - lastExperimentLastTID;
        numCompleted = numCompletedWithWarmUp - numCompletedDuringWarmUp;
        long usefulRuntime = prevBatchStats.endTs - firstBatchStats.endTs;

        double average = allLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        allLatencies.sort(null);
        double percentile_50 = PercentileCalculator.calculatePercentile(allLatencies, 0.50);
        double percentile_75 = PercentileCalculator.calculatePercentile(allLatencies, 0.75);
        double percentile_90 = PercentileCalculator.calculatePercentile(allLatencies, 0.90);
        double percentile_99 = PercentileCalculator.calculatePercentile(allLatencies, 0.99);
        // considering fixed experiment time
        double txPerSec = numCompleted / ((double) runTime / 1000L);
        // considering first received batch result
        double txPerSecUseful = numCompleted / ((double) usefulRuntime / 1000L);

        System.out.println("Average latency: "+ average);
        System.out.println("Latency at 50th percentile: "+ percentile_50);
        System.out.println("Latency at 75th percentile: "+ percentile_75);
        System.out.println("Latency at 90th percentile: "+ percentile_90);
        System.out.println("Number of completed transactions (during warm up): "+ numCompletedDuringWarmUp);
        System.out.println("Number of completed transactions (after warm up): "+ numCompleted);
        System.out.println("Number of completed transactions (total): "+ numCompletedWithWarmUp);
        System.out.println("Total runtime (ms): "+ runTime);
        System.out.println("Transactions per second: "+txPerSec);

        // useful work: from first useful batch (the first after warm up)
        System.out.println("Useful work runtime (ms): "+ usefulRuntime);
        System.out.println("Transactions per second (useful work): "+txPerSecUseful);
        System.out.println();

        return new ExperimentStats(workloadStats.initTs(), runTime, usefulRuntime, numCompletedWithWarmUp, numCompleted, txPerSec, txPerSecUseful, average, percentile_50, percentile_75, percentile_90, percentile_99);
    }

    public record ExperimentStats(long initTs, int runTime, long usefulRuntime, int numCompletedWithWarmUp, int numCompleted, double txPerSec, double txPerSecUseful, double average, double percentile_50, double percentile_75, double percentile_90, double percentile_99){}

    public static void writeResultsToFile(int numWare, ExperimentStats expStats, int runTime, int warmUp, int numTransactionWorkers, int batchWindow, int maxTransactionsPerBatch, Tuple<Integer, String>[] txRatio){
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(expStats.initTs),
                ZoneId.systemDefault()
        );
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
            writer.write("Transaction ratio: ");
            writer.newLine();
            for(Tuple<Integer, String> tx : txRatio){
                writer.write("  "+tx.t2+"=" + tx.t1);
                writer.newLine();
            }
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
            writer.write("Throughput (tx/sec): "+expStats.txPerSec);
            writer.newLine();
            writer.write("Useful Throughput (tx/sec): "+expStats.txPerSecUseful);
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
            if(input instanceof NewOrderWareIn newOrderInput){
                txIdentifier = "new_order";
                eventPayload = new TransactionInput.Event("new-order-ware-in", newOrderInput.toString());
            } else if(input instanceof PaymentIn paymentInput){
                txIdentifier = "payment";
                eventPayload = new TransactionInput.Event("payment-in", paymentInput.toString());
            } else {
                txIdentifier = "order_status";
                eventPayload = new TransactionInput.Event("order-status-in", input.toString());
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

}
