package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class AppTest {

    private static final int NUM_WARE = 2;

    @Test
    public void testLoadAndIngest() throws Exception {
        StorageUtils.EntityMetadata metadata = StorageUtils.loadEntityMetadata();
        StorageUtils.createTables(metadata, NUM_WARE);
        var tableToIndexMap = StorageUtils.mapTablesInDisk(metadata, NUM_WARE);
        int numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
        Assert.assertEquals(NUM_WARE, numWare);
        // init stub warehouse service
        var vms = new TestService().buildAndStart();
        // submit data to warehouse stub
        Assert.assertNotNull(DataLoadUtils.mapTablesFromDisk(tableToIndexMap, metadata.entityHandlerMap()));
        vms.close();
    }

    @Test
    public void testWorkload() throws IOException {
        Map<String, Integer> numTxPerType = new HashMap<>(3);
        numTxPerType.put("new_order", 10);
        // create
        WorkloadUtils.createWorkload(NUM_WARE, false, numTxPerType);
        // load
        List<Map<String, Iterator<Object>>> loaded = WorkloadUtils.mapWorkloadInputFiles(NUM_WARE, numTxPerType);
        Assert.assertEquals(NUM_WARE, loaded.size());
    }

    @Test
    public void testPaymentWorkload() throws IOException {
        Map<String, Integer> numTxPerType = new HashMap<>(3);
        numTxPerType.put("payment", 10);
         WorkloadUtils.createWorkload(1, false, numTxPerType);
        List<Map<String, Iterator<Object>>> loaded = WorkloadUtils.mapWorkloadInputFiles(1, numTxPerType);
        Iterator<Object> paymentIt = loaded.getFirst().get("payment");
        while(paymentIt.hasNext()){
            System.out.println(paymentIt.next());
        }
    }

}
