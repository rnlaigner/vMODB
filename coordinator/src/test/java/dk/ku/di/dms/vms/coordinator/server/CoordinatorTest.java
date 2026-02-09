package dk.ku.di.dms.vms.coordinator.server;

import dk.ku.di.dms.vms.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionWorker;
import dk.ku.di.dms.vms.coordinator.vms.IVmsWorker;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.sleep;

/**
 * 1. test starters VMSs with active and non-active VMSs
 * 2. test VMS inactive after the first barrier. what to do with the metadata?
 * 3.
 */
public final class CoordinatorTest {

    private static final System.Logger LOGGER = System.getLogger("CoordinatorTest");

    private static final int MAX_NUM_TID_BATCH = 10;

    private static class StorePayloadVmsWorker implements IVmsWorker {
        private final IVmsSerdesProxy serdesProxy;
        public final ConcurrentLinkedDeque<Map<String,Long>> queue;

        public StorePayloadVmsWorker(IVmsSerdesProxy serdesProxy) {
            this.serdesProxy = serdesProxy;
            this.queue = new ConcurrentLinkedDeque<>();
        }

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
            String str = new String(payloadRaw.precedenceMap(), StandardCharsets.UTF_8);
            var map = this.serdesProxy.deserializeDependenceMap(str);
            this.queue.add(map);
        }
    }

    @SuppressWarnings("BusyWait")
    @Test
    public void testComplexWorkflow() throws InterruptedException {
        HashMap<String, VmsNode> vmsMetadataMap = new HashMap<>();
        vmsMetadataMap.put( "product", new VmsNode("localhost", 8080, "product", 0, 0, 0, null, null, null));
        vmsMetadataMap.put( "cart", new VmsNode("localhost", 8081, "cart", 0, 0, 0, null, null, null));
        vmsMetadataMap.put( "stock", new VmsNode("localhost", 8082, "stock", 0, 0, 0, null, null, null));

        Map<String,IVmsWorker> workers = new HashMap<>();
        var serdes = VmsSerdesProxyBuilder.build();
        var cartWorker = new StorePayloadVmsWorker(serdes);
        var productWorker = new StorePayloadVmsWorker(serdes);
        workers.put("product", productWorker);
        workers.put("cart", cartWorker);
        workers.put("stock", new IVmsWorker() {});

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG checkoutDag = TransactionBootstrap.name("customer_checkout")
                .input("a", "cart", "customer_checkout")
                .terminal("b", "stock", "a")
                .build();
        transactionMap.put(checkoutDag.name, checkoutDag);
        TransactionDAG updateProductDag = TransactionBootstrap.name("update_product")
                .input("a", "product", "update_product")
                .terminal("b", "stock", "a")
                .terminal("c", "cart", "a")
                .build();
        transactionMap.put(updateProductDag.name, updateProductDag);
        TransactionDAG updatePriceDag = TransactionBootstrap.name("update_price")
                .input("a", "product", "update_price")
                .terminal("b", "cart", "a")
                .build();
        transactionMap.put(updatePriceDag.name, updatePriceDag);

        var vmsNodePerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);

        var txInputQueue = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        var precedenceMapOutputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();

        Map<String, TransactionWorker.PrecedenceInfo> precedenceMap = new HashMap<>();
        precedenceMap.put("product", new TransactionWorker.PrecedenceInfo(0, 0, 0));
        precedenceMap.put("cart", new TransactionWorker.PrecedenceInfo(0, 0, 0));
        precedenceMap.put("stock", new TransactionWorker.PrecedenceInfo(0, 0, 0));
        precedenceMapInputQueue.add(precedenceMap);

        var txWorker = TransactionWorker.build(1, txInputQueue, 1, MAX_NUM_TID_BATCH, 1000,
                1, precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap, vmsNodePerDAG,
                workers, new ConcurrentLinkedQueue<>(), serdes );

        var input1 = new TransactionInput("customer_checkout", new TransactionInput.Event("customer_checkout", ""));
        txInputQueue.add(input1);

        var input2 = new TransactionInput("update_price", new TransactionInput.Event("update_price", ""));
        txInputQueue.add(input2);

        var input3 = new TransactionInput("update_product", new TransactionInput.Event("update_product", ""));
        txInputQueue.add(input3);

        var txWorkerThread = Thread.ofPlatform().factory().newThread(txWorker);
        txWorkerThread.start();

        // enough time to process all 3 in the same batch
        sleep(3000);

        var input4 = new TransactionInput("customer_checkout", new TransactionInput.Event("customer_checkout", ""));
        txInputQueue.add(input4);

        Map<String, TransactionWorker.PrecedenceInfo> precedenceInfo;
        while((precedenceInfo = precedenceMapOutputQueue.poll()) == null){
            // do nothing
            sleep(10);
        }

        // adding to own transaction worker
        precedenceMapInputQueue.add(precedenceInfo);

        Assert.assertTrue( precedenceInfo.get("product").lastTid() == 3 &&
                precedenceInfo.get("cart").lastTid() == 3 &&
                precedenceInfo.get("stock").lastTid() == 3 );

        Assert.assertTrue( precedenceInfo.get("product").lastBatch() == 1 &&
                precedenceInfo.get("cart").lastBatch() == 1 &&
                precedenceInfo.get("stock").lastBatch() == 1 );

        Assert.assertTrue( precedenceInfo.get("product").previousToLastBatch() == 0 &&
                precedenceInfo.get("cart").previousToLastBatch() == 0 &&
                precedenceInfo.get("stock").previousToLastBatch() == 0 );

        Map<String, Long> depMap = cartWorker.queue.poll();
        Assert.assertTrue( depMap != null && depMap.get("cart") == 0 &&
                !depMap.containsKey("product") && depMap.get("stock") == 0);

        depMap = productWorker.queue.poll();
        Assert.assertTrue( depMap != null && depMap.get("cart") == 2 &&
                depMap.get("product") == 2 && depMap.get("stock") == 1);

        depMap = productWorker.queue.poll();
        Assert.assertTrue( depMap != null && depMap.get("cart") == 1 &&
                !depMap.containsKey("stock") && depMap.get("product") == 0);

        // stock is not an input vms. no need to check anything in this test
        sleep(3000);

        // check correctness of the next
        precedenceInfo = precedenceMapOutputQueue.poll();
        Assert.assertNotNull(precedenceInfo);

        Assert.assertTrue( precedenceInfo.get("product").lastTid() == 3 &&
                precedenceInfo.get("cart").lastTid() == 4 &&
                precedenceInfo.get("stock").lastTid() == 4 );

        Assert.assertTrue( precedenceInfo.get("product").lastBatch() == 1 &&
                precedenceInfo.get("cart").lastBatch() == 2 &&
                precedenceInfo.get("stock").lastBatch() == 2 );

        Assert.assertTrue( precedenceInfo.get("product").previousToLastBatch() == 0 &&
                precedenceInfo.get("cart").previousToLastBatch() == 1 &&
                precedenceInfo.get("stock").previousToLastBatch() == 1 );
    }

    @SuppressWarnings("BusyWait")
    @Test
    public void testTwoPendingTransactionInput() throws InterruptedException {
        HashMap<String, VmsNode> vmsMetadataMap = new HashMap<>();
        vmsMetadataMap.put( "product", new VmsNode("localhost", 8080, "product", 0, 0, 0, null, null, null));
        vmsMetadataMap.put( "cart", new VmsNode("localhost", 8081, "cart", 0, 0, 0, null, null, null));
        vmsMetadataMap.put( "stock", new VmsNode("localhost", 8082, "stock", 0, 0, 0, null, null, null));

        Map<String,IVmsWorker> workers = new HashMap<>();
        var productWorker = new StorePayloadVmsWorker(VmsSerdesProxyBuilder.build());
        workers.put("product", productWorker);
        workers.put("cart", new IVmsWorker() { });
        workers.put("stock", new IVmsWorker() { });

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG updateProductDag = TransactionBootstrap.name("update_product")
                .input("a", "product", "update_product")
                    .terminal("b", "stock", "a")
                    .terminal("c", "cart", "a")
                .build();
        transactionMap.put(updateProductDag.name, updateProductDag);
        TransactionDAG updatePriceDag = TransactionBootstrap.name("update_price")
                .input("a", "product", "update_price")
                .terminal("b", "cart", "a")
                .build();
        transactionMap.put(updatePriceDag.name, updatePriceDag);

        var vmsNodePerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);

        var txInputQueue = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        var precedenceMapOutputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        Map<String, TransactionWorker.PrecedenceInfo> precedenceMap = new HashMap<>();
        precedenceMap.put("product", new TransactionWorker.PrecedenceInfo(10, 1, 0));
        precedenceMap.put("cart", new TransactionWorker.PrecedenceInfo(10, 1, 0));
        precedenceMap.put("stock", new TransactionWorker.PrecedenceInfo(1, 1, 0));
        precedenceMapInputQueue.add(precedenceMap);

        var txWorker = TransactionWorker.build(1, txInputQueue, 11, MAX_NUM_TID_BATCH, 1000,
                1, precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap, vmsNodePerDAG,
                workers, new ConcurrentLinkedQueue<>(), VmsSerdesProxyBuilder.build() );

        // queue two pending inputs
        var input1 = new TransactionInput("update_price", new TransactionInput.Event("update_price", ""));
        txInputQueue.add(input1);

        var input2 = new TransactionInput("update_product", new TransactionInput.Event("update_product", ""));
        txInputQueue.add(input2);

        var txWorkerThread = Thread.ofPlatform().factory().newThread(txWorker);
        txWorkerThread.start();

        // check if the last tid was generated correctly
        Map<String, TransactionWorker.PrecedenceInfo> precedenceInfo;
        while((precedenceInfo = precedenceMapOutputQueue.poll()) == null){
            // do nothing
            sleep(10);
        }

        Assert.assertTrue( precedenceInfo.get("product").lastTid() == 12 &&
                precedenceInfo.get("cart").lastTid() == 12 &&
                precedenceInfo.get("stock").lastTid() == 12 );

        Assert.assertTrue( precedenceInfo.get("product").lastBatch() == 2 &&
                precedenceInfo.get("cart").lastBatch() == 2 &&
                precedenceInfo.get("stock").lastBatch() == 2 );

        Assert.assertTrue( precedenceInfo.get("product").previousToLastBatch() == 1 &&
                precedenceInfo.get("cart").previousToLastBatch() == 1 &&
                precedenceInfo.get("stock").previousToLastBatch() == 1 );

        Map<String,Long> map = productWorker.queue.poll();
        Assert.assertTrue( map != null && map.get("cart") == 10 &&
                map.get("product") == 10 &&
                !map.containsKey("stock"));

        map = productWorker.queue.poll();
        Assert.assertTrue( map != null && map.get("cart") == 11 &&
                map.get("product") == 11 &&
                map.get("stock") == 1);
    }

    @Test
    public void testParamTransactionWorkers() throws InterruptedException {
        int numWorkers = 2;

        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsNodesPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        // generic algorithm to handle N number of transaction workers
        int idx = 1;
        long initTid = 1;

        var firstPrecedenceInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        var precedenceMapInputQueue = firstPrecedenceInputQueue;
        ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>> precedenceMapOutputQueue;
        buildAndQueueStarterPrecedenceMap(precedenceMapInputQueue);
        var serdesProxy = VmsSerdesProxyBuilder.build();
        List<Tuple<TransactionWorker,Thread>> txWorkers = new ArrayList<>();
        Queue<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();
        do {
            if(idx < numWorkers){
                precedenceMapOutputQueue = new ConcurrentLinkedDeque<>();
            } else {
                precedenceMapOutputQueue = firstPrecedenceInputQueue;
            }

            var txInputQueue = new ConcurrentLinkedDeque<TransactionInput>();

            for(int i = 1; i <= 10; i++){
                var input = new TransactionInput("test", new TransactionInput.Event("test", ""));
                txInputQueue.add(input);
            }

            var txWorker = TransactionWorker.build(idx, txInputQueue, initTid, MAX_NUM_TID_BATCH, 1000,
                    numWorkers, precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap,
                    vmsNodesPerDAG, workers, coordinatorQueue, serdesProxy);
            var txWorkerThread = Thread.ofPlatform().factory().newThread(txWorker);

            initTid = initTid + MAX_NUM_TID_BATCH;

            precedenceMapInputQueue = precedenceMapOutputQueue;

            idx++;

            txWorkers.add(Tuple.of( txWorker, txWorkerThread ));
        } while (idx <= numWorkers);

        // in this test check whether the last tids in each batch are correct
        txWorkers.get(0).t2().start();
        txWorkers.get(1).t2().start();

        sleep(100);

        var lastTid1 = safePoll(coordinatorQueue).lastTid;
        var lastTid2 = safePoll(coordinatorQueue).lastTid;

        txWorkers.get(0).t1().stop();
        txWorkers.get(1).t1().stop();

        Assert.assertTrue( lastTid1 == 10 && lastTid2 == 20 );
    }

    private static BatchContext safePoll(Queue<Object> coordinatorQueue) {
        return (BatchContext) coordinatorQueue.poll();
    }

    @Test
    public void testTwoTransactionWorkersWithOneHalfMaxBatch() throws InterruptedException {

        int batchWindow = 100;

        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsNodesPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        var txInputQueue1 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue1 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();

        var txInputQueue2 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue2 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();

        Queue<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();

        var txWorker1 = TransactionWorker.build(1, txInputQueue1, 1, MAX_NUM_TID_BATCH, batchWindow,
                2, precedenceMapQueue1, precedenceMapQueue2, transactionMap, vmsNodesPerDAG, workers,
                coordinatorQueue, VmsSerdesProxyBuilder.build() );

        var txWorker2 = TransactionWorker.build(2, txInputQueue2, 11, MAX_NUM_TID_BATCH, batchWindow,
                2, precedenceMapQueue2, precedenceMapQueue1, transactionMap, vmsNodesPerDAG, workers,
                coordinatorQueue, VmsSerdesProxyBuilder.build() );

        buildAndQueueStarterPrecedenceMap(precedenceMapQueue1);

        for(int i = 1; i <= 10; i++){
            var input = new TransactionInput("test", new TransactionInput.Event("test", ""));
            if(i<=5) txInputQueue1.add(input);
            txInputQueue2.add(input);
        }

        var txWorkerThread1 = Thread.ofPlatform().factory().newThread(txWorker1);
        txWorkerThread1.start();
        var txWorkerThread2 = Thread.ofPlatform().factory().newThread(txWorker2);
        txWorkerThread2.start();

        // must be higher than window because worker 1 will only close batch in batch window timeout
        sleep(batchWindow*2);

        // could measure how long it takes for the tid to move on...
        long txWorker1Tid;
        do{
            txWorker1Tid = safePoll(coordinatorQueue).lastTid;
        } while(txWorker1Tid == 0);

        long txWorker2Tid;
        do {
            txWorker2Tid = safePoll(coordinatorQueue).lastTid;
        } while(txWorker2Tid == 0);

        txWorker1.stop();
        txWorker2.stop();

        LOGGER.log(System.Logger.Level.INFO, " Tx worker #1 TID: "+txWorker1Tid);
        LOGGER.log(System.Logger.Level.INFO, " Tx worker #2 TID: "+txWorker2Tid);

        Assert.assertTrue(txWorker1Tid == 5 && txWorker2Tid == 20);
    }

    @Test
    public void testTwoTransactionWorkers() throws InterruptedException {
        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsNodesPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        var txInputQueue1 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue1 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();

        var txInputQueue2 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue2 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();

        Queue<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();

        var txWorker1 = TransactionWorker.build(1, txInputQueue1, 1, MAX_NUM_TID_BATCH, 1000,
                2, precedenceMapQueue1, precedenceMapQueue2, transactionMap, vmsNodesPerDAG, workers,
                coordinatorQueue, VmsSerdesProxyBuilder.build() );

        var txWorker2 = TransactionWorker.build(2, txInputQueue2, 11, MAX_NUM_TID_BATCH, 1000,
                2, precedenceMapQueue2, precedenceMapQueue1, transactionMap, vmsNodesPerDAG, workers,
                coordinatorQueue, VmsSerdesProxyBuilder.build() );

        buildAndQueueStarterPrecedenceMap(precedenceMapQueue1);

        var txWorkerThread1 = Thread.ofPlatform().factory().newThread(txWorker1);
        txWorkerThread1.start();
        var txWorkerThread2 = Thread.ofPlatform().factory().newThread(txWorker2);
        txWorkerThread2.start();

        for(int i = 1; i <= 10; i++){
            var input = new TransactionInput("test", new TransactionInput.Event("test", ""));
            txInputQueue1.add(input);
            txInputQueue2.add(input);
        }

        sleep(100);

        // could measure how long it takes for the tid to move on...
        long txWorker1Tid;
        do{
            txWorker1Tid = safePoll(coordinatorQueue).lastTid;
        } while(txWorker1Tid == 0);

        long txWorker2Tid;
        do {
            txWorker2Tid = safePoll(coordinatorQueue).lastTid;
        } while(txWorker2Tid == 0);

        txWorker1.stop();
        txWorker2.stop();

        LOGGER.log(System.Logger.Level.INFO, " Tx worker #1 TID: "+txWorker1Tid);
        LOGGER.log(System.Logger.Level.INFO, " Tx worker #2 TID: "+txWorker2Tid);

        Assert.assertTrue(txWorker1Tid == 10 && txWorker2Tid == 20);
    }

    @Test
    public void testTwoTxnWorkersOnlyOneProcessTxn() throws InterruptedException {
        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsNodesPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        var txInputQueue1 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue1 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();

        var txInputQueue2 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue2 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();

        Queue<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();

        var txWorker1 = TransactionWorker.build(1, txInputQueue1, 1, MAX_NUM_TID_BATCH, 1000,
                2, precedenceMapQueue1, precedenceMapQueue2, transactionMap, vmsNodesPerDAG, workers,
                coordinatorQueue, VmsSerdesProxyBuilder.build() );

        var txWorker2 = TransactionWorker.build(2, txInputQueue2, 11, MAX_NUM_TID_BATCH, 1000,
                2, precedenceMapQueue2, precedenceMapQueue1, transactionMap, vmsNodesPerDAG, workers,
                coordinatorQueue, VmsSerdesProxyBuilder.build() );

        buildAndQueueStarterPrecedenceMap(precedenceMapQueue1);

        var txWorkerThread1 = Thread.ofPlatform().factory().newThread(txWorker1);
        txWorkerThread1.start();
        var txWorkerThread2 = Thread.ofPlatform().factory().newThread(txWorker2);
        txWorkerThread2.start();

        var input = new TransactionInput("test", new TransactionInput.Event("test", ""));
        txInputQueue2.add(input);

        sleep(3000);

        long txWorker1BatchId = safePoll(coordinatorQueue).batchOffset;
        long txWorker2BatchId = safePoll(coordinatorQueue).batchOffset;

        txWorker1.stop();
        txWorker2.stop();

        LOGGER.log(System.Logger.Level.INFO, " Tx worker #1 Batch ID: "+txWorker1BatchId);
        LOGGER.log(System.Logger.Level.INFO, " Tx worker #2 Batch ID: "+txWorker2BatchId);

        Assert.assertTrue(txWorker1BatchId == 1 && txWorker2BatchId == 2);
    }

    private static void buildAndQueueStarterPrecedenceMap(ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>> precedenceMapQueue1) {
        Map<String, TransactionWorker.PrecedenceInfo> precedenceMap = new HashMap<>();
        precedenceMap.put("product", new TransactionWorker.PrecedenceInfo(0, 0, 0));
        precedenceMapQueue1.add(precedenceMap);
    }

    @Test
    public void testSingleTransactionWorker() throws InterruptedException {
        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsNodesPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        var txInputQueue = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        Queue<Object> coordinatorQueue = new ConcurrentLinkedDeque<>();
        var txWorker = TransactionWorker.build(1, txInputQueue, 1, MAX_NUM_TID_BATCH, 1000,
                1, precedenceMapQueue, precedenceMapQueue, transactionMap, vmsNodesPerDAG, workers,
                coordinatorQueue, VmsSerdesProxyBuilder.build() );

        buildAndQueueStarterPrecedenceMap(precedenceMapQueue);

        var txWorkerThread = Thread.ofPlatform().factory().newThread(txWorker);
        txWorkerThread.start();

        for(int i = 1; i <= 10; i++){
            txInputQueue.add(new TransactionInput("test", new TransactionInput.Event("test", "")));
        }

        sleep(100);

        txWorker.stop();

        long txWorker1Tid;
        do{
            txWorker1Tid = safePoll(coordinatorQueue).lastTid;
        } while(txWorker1Tid == 0);
        LOGGER.log(System.Logger.Level.INFO, " Tx worker #1 TID: "+txWorker1Tid);

        Assert.assertEquals(10, txWorker1Tid);
    }

    private static Map<String, IVmsWorker> buildTestVmsWorker() {
        Map<String,IVmsWorker> workers = new HashMap<>();
        workers.put("product", new IVmsWorker(){});
        return workers;
    }

    private static HashMap<String, VmsNode> buildTestVmsMetadataMap() {
        var vmsMetadataMap = new HashMap<String, VmsNode>();
        vmsMetadataMap.put( "product", new VmsNode("localhost", 8080, "product", 0, 0, 0, null, null, null));
        return vmsMetadataMap;
    }

    private static Map<String, VmsNode[]> buildTestVmsPerDagMap(Map<String, TransactionDAG> transactionMap,
                                                                HashMap<String, VmsNode> vmsMetadataMap) {
        Map<String, VmsNode[]> vmsNodePerDAG = new HashMap<>();
        for(var dag : transactionMap.entrySet()) {
            vmsNodePerDAG.put(dag.getKey(), BatchAlgo.buildTransactionDagVmsList(dag.getValue(), vmsMetadataMap));
        }
        return vmsNodePerDAG;
    }

    private static Map<String, TransactionDAG> buildTestTransactionDAGMap() {
        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        TransactionDAG updateProductDag = TransactionBootstrap.name("test")
                .input("a", "product", "test")
                .terminal("b", "product", "a")
                .build();
        transactionMap.put(updateProductDag.name, updateProductDag);
        return transactionMap;
    }

    /**
     * In this test, given a transaction DAG and a set of previous
     * transactions from the participating VMSs, decides the
     * dependence map (map of VMS to corresponding lastTid)
     */
    @Test
    public void testSimpleDependenceMap(){
        // build VMSs
        VmsNode vms1 =  new VmsNode("",0,"vms1",1,1,0,null,null,null);
        VmsNode vms2 =  new VmsNode("",0,"vms2",2,2,1,null,null,null);

        Map<String, VmsNode> vmsMetadataMap = new HashMap<>(2);
        vmsMetadataMap.put(vms1.identifier, vms1);
        vmsMetadataMap.put(vms2.identifier, vms2);

        // build DAG
        TransactionDAG dag = TransactionBootstrap.name("test")
                .input("a", "vms1", "input1")
                .terminal("b","vms2","a").build();

        Map<String, Long> dependenceMap = BatchAlgo.buildPrecedenceMap( dag.inputEvents.get("input1"), dag, vmsMetadataMap );

        assert dependenceMap.get("vms1") == 1 && dependenceMap.get("vms2") == 2;
    }

    @Test
    public void testComplexDependenceMap(){

        // build VMSs
        Map<String, VmsNode> vmsMetadataMap = getStringVmsNodeMap();

        // new order transaction
        TransactionDAG dag =  TransactionBootstrap.name("new-order")
                .input( "a", "customer", "customer-new-order-in" )
                .input("b", "item","item-new-order-in" )
                .input( "c", "stock","stock-new-order-in" )
                .input( "d", "warehouse", "waredist-new-order-in" )
                .internal( "e", "customer","customer-new-order-out",  "a" )
                .internal( "f", "item","item-new-order-out", "b" )
                .internal( "g", "stock", "stock-new-order-out", "c" )
                .internal( "h", "warehouse","waredist-new-order-out", "d" )
                // signals the end of the transaction. However, it does not mean it generates an output event
                .terminal("i", "order", "b", "e", "f", "g", "h" )
                .build();

        Map<String, Long> dependenceMap = BatchAlgo.buildPrecedenceMap( dag, vmsMetadataMap );

        assert dependenceMap.get("customer") == 1 && dependenceMap.get("item") == 2 && dependenceMap.get("stock") == 3
                && dependenceMap.get("warehouse") == 4 && dependenceMap.get("order") == 5;
    }

    private static Map<String, VmsNode> getStringVmsNodeMap() {
        VmsNode vms1 =  new VmsNode("",0,"customer",1,1,0,null,null,null);
        VmsNode vms2 =  new VmsNode("",0,"item",2,2,1,null,null,null);
        VmsNode vms3 =  new VmsNode("",0,"stock",3,3,2,null,null,null);
        VmsNode vms4 =  new VmsNode("",0,"warehouse",4,4,3,null,null,null);
        VmsNode vms5 =  new VmsNode("",0,"order",5,5,4,null,null,null);

        Map<String, VmsNode> vmsMetadataMap = new HashMap<>(5);
        vmsMetadataMap.put(vms1.identifier, vms1);
        vmsMetadataMap.put(vms2.identifier, vms2);
        vmsMetadataMap.put(vms3.identifier, vms3);
        vmsMetadataMap.put(vms4.identifier, vms4);
        vmsMetadataMap.put(vms5.identifier, vms5);
        return vmsMetadataMap;
    }

    // test correctness of a batch... are all dependence maps correct?

    /**
     * test the batch protocol. with a simple dag, 1 source, one terminal
     *
     */
    @Test
    public void testBasicCommitProtocol(){
        // need a transaction dag and corresponding VMSs
        // need a producer of transaction inputs (a separate thread or this thread)
        // need the coordinator to assemble the batch
        // need vms workers
        // no need of a scheduler
        // need of custom VMSs to respond to the batch protocol correctly
        // it would be nice to decouple the network from the batch algorithm...
    }

    // with a source, an internal, and a terminal

    // a source and two terminals

    // two sources, a terminal

}
