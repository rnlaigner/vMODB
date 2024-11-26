package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.Assert;
import org.junit.Test;

public final class AppTest {

    private static final int NUM_WARE = 2;

    @Test
    public void testLoadAndIngest() throws Exception {
        StorageUtils.EntityMetadata metadata = StorageUtils.loadEntityMetadata();

        StorageUtils.createTables(metadata, NUM_WARE);

        var tableToIndexMap = StorageUtils.loadTables(metadata,NUM_WARE);

        int numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");

        Assert.assertEquals(NUM_WARE, numWare);

        // init test service
        new TestService().run();

        // ingest data in warehouse
        Assert.assertTrue(DataLoader.load(tableToIndexMap, metadata.entityHandlerMap()));

    }

    @Test
    public void testWorkload() {

        // create
        var created = WorkloadUtils.createWorkload(1, 100000);

        // load
        var loaded = WorkloadUtils.loadWorkloadData();

        Assert.assertEquals(created.size(), loaded.size());

        for(int i = 0; i < created.size(); i++){
            Assert.assertEquals(created.get(i), loaded.get(i));
        }

    }
}
