package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.composite.TripleCompositeKey;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.entities.District;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.HttpUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class TPCcWorkflowTest {

    private static final int NUM_WARE = 1;

    private static final int RUN_TIME = 10000;

    private static final int WARM_UP = 0;

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    private static VmsApplication WAREHOUSE_SVC;
    private static VmsApplication INVENTORY_SVC;
    private static VmsApplication ORDER_SVC;

    private static final StorageUtils.EntityMetadata METADATA;

    static {
        try {
            METADATA = StorageUtils.loadEntityMetadata();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void startTPCcServices(){
         // clean up
        File orderLineFile = dk.ku.di.dms.vms.modb.utils.StorageUtils.buildFile("order", "order_line");
        File ordersFile = dk.ku.di.dms.vms.modb.utils.StorageUtils.buildFile("order", "orders");
        File newOrdersFile = dk.ku.di.dms.vms.modb.utils.StorageUtils.buildFile("order", "new_orders");

        if(orderLineFile.delete() && ordersFile.delete() && newOrdersFile.delete()){
            System.out.println("Order VMS records deleted.");
        }

        String basePathStr = dk.ku.di.dms.vms.modb.utils.StorageUtils.getBasePath("");
        Path basePath = Paths.get(basePathStr);
        try(var paths = Files
                // retrieve all files in the folder
                .walk(basePath)
                // find the log files
                .filter(path -> path.toString().contains(".llog"))) {
            for(var path : paths.toList()){
                if(path.toFile().delete()){
                    System.out.println("Logical log file deleted: "+path);
                }
            }
        } catch (IOException ignored){ }

        PROPERTIES.setProperty("logging", "true");
        PROPERTIES.setProperty("checkpointing", "true");

        try {
            WAREHOUSE_SVC = dk.ku.di.dms.vms.tpcc.warehouse.Main.build();
            INVENTORY_SVC = dk.ku.di.dms.vms.tpcc.inventory.Main.build();
            ORDER_SVC = dk.ku.di.dms.vms.tpcc.order.Main.build();
        } catch (Exception e){
            throw new RuntimeException(e);
        }

        WAREHOUSE_SVC.start();
        INVENTORY_SVC.start();
        ORDER_SVC.start();
    }

    @AfterClass
    public static void closeTPCcServices(){
        ORDER_SVC.close();
        INVENTORY_SVC.close();
        WAREHOUSE_SVC.close();
    }

    @Test
    public void test_A_create_data() {
        StorageUtils.createTables(METADATA, NUM_WARE);
    }

    @Test
    public void test_B_create_workload() throws IOException {
        Map<String, Integer> numTxPerType = new HashMap<>(1);
        numTxPerType.put("new_order", 10);
        WorkloadUtils.createWorkload(NUM_WARE, true, numTxPerType);
        List<Map<String,Iterator<Object>>> iteratorMap = WorkloadUtils.mapWorkloadInputFiles(NUM_WARE, numTxPerType);
        Assert.assertFalse(iteratorMap.isEmpty());
        var iterator = iteratorMap.getFirst().get("new_order");
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    @Test
    public void test_C_test_hash_entry() {
        // generate entries for district for different warehouses, compare the hashes generated. try to find a way to avoid duplicate hashes
        Map<Integer, List<TripleCompositeKey>> map = new HashMap<>();
        for(int w_id = 1; w_id <= NUM_WARE; w_id++){
            for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                for(int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++) {
                    var key = TripleCompositeKey.of(c_id, d_id, w_id);
                    map.computeIfAbsent(key.hashCode(), ignored -> new ArrayList<>()).add(key);
                    Assert.assertFalse(map.get(key.hashCode()).size() > 1);
                }
            }
        }
    }

    @Test
    public void test_D_load_and_ingest() throws IOException {
        var tableToIndexMap = StorageUtils.mapTablesInDisk(METADATA, NUM_WARE);
        var tableInputMap = DataLoadUtils.mapTablesFromDisk(tableToIndexMap, METADATA.entityHandlerMap());
        var vmsToHostMap = DataLoadUtils.mapVmsToHost(PROPERTIES);
        DataLoadUtils.ingestData(tableInputMap, vmsToHostMap, Runtime.getRuntime().availableProcessors());
        hasDataBeenIngested();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_E_submit_workload() throws IOException {
        Map<String, Integer> numTxPerType = new HashMap<>(1);
        numTxPerType.put("new_order", 10);
        // mapping all the workload data is not a good idea, it overloads the Java heap
        //  perhaps it is better to iteratively load from memory. instead of list, pass an iterator to the worker
        //  link a file/worker to a warehouse, so there is no need to partition the file among workers
        var input = WorkloadUtils.mapWorkloadInputFiles(NUM_WARE, numTxPerType);
        Assert.assertFalse(input.isEmpty());

        Coordinator coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
        int numConnected;
        do {
            numConnected = coordinator.getConnectedVMSs().size();
        } while (numConnected < 3);

        Tuple<Integer, String>[] txRatio = new Tuple[]{ Tuple.of(100, "new_order") };
        ExperimentUtils.ExperimentStats expStats = ExperimentUtils.runExperiment(coordinator, txRatio, input, RUN_TIME, WARM_UP);

        coordinator.stop();

        ExperimentUtils.writeResultsToFile(NUM_WARE, expStats, RUN_TIME, WARM_UP,
                coordinator.getOptions().getNumTransactionWorkers(),
                coordinator.getOptions().getBatchWindow(),
                coordinator.getOptions().getMaxTransactionsPerBatch(), txRatio);

        String host = PROPERTIES.getProperty("warehouse_host");
        int port = TPCcConstants.VMS_TO_PORT_MAP.get("warehouse");
        IVmsSerdesProxy serdesProxy = VmsSerdesProxyBuilder.build();
        // query get some items and assert correctness
        int districtUpdated = 0;
        try(MinimalHttpClient httpClient = new MinimalHttpClient(host, port)){
            for(int i = 1; i <= TPCcConstants.NUM_DIST_PER_WARE; i++) {
                String resp2 = httpClient.sendGetRequest("district/"+i+"/1");
                var parsedResp = HttpUtils.parseRequest(resp2);
                District district = serdesProxy.deserialize(parsedResp.body(), District.class);
                // not all districts are updated
                if(district.d_next_o_id > 3001){
                    districtUpdated++;
                }
            }
        }
        Assert.assertTrue(districtUpdated > 0);
    }

    private static void hasDataBeenIngested() throws IOException {
        String host = PROPERTIES.getProperty("warehouse_host");
        int port = TPCcConstants.VMS_TO_PORT_MAP.get("warehouse");
        var serdesProxy = VmsSerdesProxyBuilder.build();
        try(MinimalHttpClient httpClient = new MinimalHttpClient(host, port)){
            String resp1 = httpClient.sendGetRequest("warehouse/1");
            var parsedResp1 = HttpUtils.parseRequest(resp1);
            var ware = serdesProxy.deserialize(parsedResp1.body(), Warehouse.class);
            Assert.assertEquals(1, ware.w_id);
            for(int i = 1 ; i <= TPCcConstants.NUM_DIST_PER_WARE; i++) {
                String resp2 = httpClient.sendGetRequest("district/"+i+"/1");
                var parsedResp2 = HttpUtils.parseRequest(resp2);
                var district = serdesProxy.deserialize(parsedResp2.body(), District.class);
                Assert.assertEquals(3001, district.d_next_o_id);
            }
        }
    }

}
