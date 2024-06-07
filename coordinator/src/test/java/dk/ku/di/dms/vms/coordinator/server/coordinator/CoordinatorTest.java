package dk.ku.di.dms.vms.coordinator.server.coordinator;

import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.IVmsWorker;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.TransactionWorker;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static java.lang.Thread.sleep;

/**
 * 1. test starters VMSs with active and non-active VMSs
 * 2. test VMS inactive after the first barrier. what to do with the metadata?
 * 3.
 */
public final class CoordinatorTest {

    private static final System.Logger LOGGER = System.getLogger("CoordinatorTest");

    private static final int MAX_NUM_TID_BATCH = 10;

    private static class NoOpVmsWorker implements IVmsWorker {
        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) { }
        @Override
        public void queueMessage(Object message) { }
    }

    @Test
    public void testParamTransactionWorkers() throws InterruptedException {
        int param = 3;

        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsIdentifiersPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        // generic algorithm to handle N number of transaction workers
        int idx = 1;
        long initTid = 1;
        var txInputQueue = new ConcurrentLinkedDeque<TransactionInput>();
        var firstPrecedenceInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>>();
        var precedenceMapInputQueue = firstPrecedenceInputQueue;
        ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>> precedenceMapOutputQueue;
        buildAndQueueStarterPrecedenceMap(precedenceMapInputQueue);
        var serdesProxy = VmsSerdesProxyBuilder.build();
        do {
            if(idx < param){
                precedenceMapOutputQueue = new ConcurrentLinkedDeque<>();
            } else {
                precedenceMapOutputQueue = firstPrecedenceInputQueue;
            }

            var txWorker = TransactionWorker.build(idx, txInputQueue, initTid, MAX_NUM_TID_BATCH, 1000, param, precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap, vmsIdentifiersPerDAG, workers, serdesProxy);

            var txWorkerThread = Thread.ofPlatform().factory().newThread(txWorker);
            txWorkerThread.start();

            initTid = initTid + MAX_NUM_TID_BATCH;

            precedenceMapInputQueue = precedenceMapOutputQueue;

            idx++;
        } while (idx <= param);

        // TODO in this test, switch the noopworker to a worker that stores the batch complete messages received
        //  then, check whether the last tids in each batch are correct

    }

    @Test
    public void testTwoTransactionWorkersWithOneHalfMaxBatch() throws InterruptedException {
        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsIdentifiersPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        var txInputQueue1 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue1 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>>();

        var txInputQueue2 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue2 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>>();

        var txWorker1 = TransactionWorker.build(1, txInputQueue1, 1, MAX_NUM_TID_BATCH, 1000, 2, precedenceMapQueue1, precedenceMapQueue2, transactionMap, vmsIdentifiersPerDAG, workers, VmsSerdesProxyBuilder.build() );

        var txWorker2 = TransactionWorker.build(2, txInputQueue2, 11, MAX_NUM_TID_BATCH, 1000, 2, precedenceMapQueue2, precedenceMapQueue1, transactionMap, vmsIdentifiersPerDAG, workers, VmsSerdesProxyBuilder.build() );

        buildAndQueueStarterPrecedenceMap(precedenceMapQueue1);

        var txWorkerThread1 = Thread.ofPlatform().factory().newThread(txWorker1);
        txWorkerThread1.start();
        var txWorkerThread2 = Thread.ofPlatform().factory().newThread(txWorker2);
        txWorkerThread2.start();

        for(int i = 1; i <= 10; i++){
            var input = new TransactionInput("test", new TransactionInput.Event("test", ""));
            if(i<=5) txInputQueue1.add(input);
            txInputQueue2.add(input);
        }

        sleep(100);

        // could measure how long it takes for the tid to move on...
        long txWorker1Tid;
        do{
            txWorker1Tid = txWorker1.getLastTidCompleted();
        } while(txWorker1Tid == 0);

        long txWorker2Tid;
        do {
            txWorker2Tid = txWorker2.getLastTidCompleted();
        } while(txWorker2Tid == 0);

        txWorker1.stop();
        txWorker2.stop();

        LOGGER.log(System.Logger.Level.INFO, " Tx worker #1 TID: "+txWorker1Tid);
        LOGGER.log(System.Logger.Level.INFO, " Tx worker #2 TID: "+txWorker2Tid);

        assert txWorker1Tid == 5;
        assert txWorker2Tid == 20;
    }

    @Test
    public void testTwoTransactionWorkers() throws InterruptedException {
        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsIdentifiersPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        var txInputQueue1 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue1 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>>();

        var txInputQueue2 = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue2 = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>>();

        var txWorker1 = TransactionWorker.build(1, txInputQueue1, 1, MAX_NUM_TID_BATCH, 1000, 2, precedenceMapQueue1, precedenceMapQueue2, transactionMap, vmsIdentifiersPerDAG, workers, VmsSerdesProxyBuilder.build() );

        var txWorker2 = TransactionWorker.build(2, txInputQueue2, 11, MAX_NUM_TID_BATCH, 1000, 2, precedenceMapQueue2, precedenceMapQueue1, transactionMap, vmsIdentifiersPerDAG, workers, VmsSerdesProxyBuilder.build() );

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
            txWorker1Tid = txWorker1.getLastTidCompleted();
        } while(txWorker1Tid == 0);

        long txWorker2Tid;
        do {
            txWorker2Tid = txWorker2.getLastTidCompleted();
        } while(txWorker2Tid == 0);

        txWorker1.stop();
        txWorker2.stop();

        LOGGER.log(System.Logger.Level.INFO, " Tx worker #1 TID: "+txWorker1Tid);
        LOGGER.log(System.Logger.Level.INFO, " Tx worker #2 TID: "+txWorker2Tid);

        assert txWorker1Tid == 10;
        assert txWorker2Tid == 20;
    }

    private static void buildAndQueueStarterPrecedenceMap(ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>> precedenceMapQueue1) {
        Map<String, TransactionWorker.PrecendenceInfo> precedenceMap = new HashMap<>();
        precedenceMap.put("product", new TransactionWorker.PrecendenceInfo(0,0,0));
        precedenceMapQueue1.add(precedenceMap);
    }

    @Test
    public void testSingleTransactionWorker() throws InterruptedException {
        var vmsMetadataMap = buildTestVmsMetadataMap();
        Map<String, TransactionDAG> transactionMap = buildTestTransactionDAGMap();
        Map<String, VmsNode[]> vmsIdentifiersPerDAG = buildTestVmsPerDagMap(transactionMap, vmsMetadataMap);
        Map<String, IVmsWorker> workers = buildTestVmsWorker();

        var txInputQueue = new ConcurrentLinkedDeque<TransactionInput>();
        var precedenceMapQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecendenceInfo>>();

        var txWorker = TransactionWorker.build(1, txInputQueue, 1, MAX_NUM_TID_BATCH, 1000, 1, precedenceMapQueue, precedenceMapQueue, transactionMap, vmsIdentifiersPerDAG, workers, VmsSerdesProxyBuilder.build() );

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
            txWorker1Tid = txWorker.getLastTidCompleted();
        } while(txWorker1Tid == 0);
        LOGGER.log(System.Logger.Level.INFO, " Tx worker #1 TID: "+txWorker1Tid);

        assert txWorker1Tid == 10;
    }

    private static Map<String, IVmsWorker> buildTestVmsWorker() {
        Map<String,IVmsWorker> workers = new HashMap<>();
        workers.put("product", new NoOpVmsWorker());
        return workers;
    }

    private static HashMap<String, VmsNode> buildTestVmsMetadataMap() {
        var vmsMetadataMap = new HashMap<String, VmsNode>();
        vmsMetadataMap.put( "product", new VmsNode("localhost", 8080, "product", 0, 0, 0, null, null, null));
        return vmsMetadataMap;
    }

    private static Map<String, VmsNode[]> buildTestVmsPerDagMap(Map<String, TransactionDAG> transactionMap, HashMap<String, VmsNode> vmsMetadataMap) {
        Map<String, VmsNode[]> vmsIdentifiersPerDAG = new HashMap<>();
        for(var dag : transactionMap.entrySet()) {
            vmsIdentifiersPerDAG.put(dag.getKey(), BatchAlgo.buildTransactionDagVmsList(dag.getValue(), vmsMetadataMap));
        }
        return vmsIdentifiersPerDAG;
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
        vmsMetadataMap.put(vms1.getIdentifier(), vms1);
        vmsMetadataMap.put(vms2.getIdentifier(), vms2);

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
        vmsMetadataMap.put(vms1.getIdentifier(), vms1);
        vmsMetadataMap.put(vms2.getIdentifier(), vms2);
        vmsMetadataMap.put(vms3.getIdentifier(), vms3);
        vmsMetadataMap.put(vms4.getIdentifier(), vms4);
        vmsMetadataMap.put(vms5.getIdentifier(), vms5);
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
