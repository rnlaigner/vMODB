package dk.ku.di.dms.vms.modb.persistence;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.query.TestCommon;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.planner.SimplePlanner;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import org.junit.Test;

import java.nio.ByteBuffer;

public class BulkLoadTest {

    @Test
    public void overflowTest(){

        var map = TestCommon.getDefaultCatalog();
        Table table = map.get("item");

        int maxItems = 20;
        int bufferSize = table.getSchema().getRecordSizeWithoutHeader() * maxItems;
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer( bufferSize );

        for(int i = 1; i <= maxItems; i++){
            produceItem(buffer, i);
        }

        SimplePlanner planner = new SimplePlanner();
        Analyzer analyzer = new Analyzer(map);
        TransactionFacade.build(map, replicaSchema, planner, analyzer).bulkInsert(table, buffer, maxItems);

        long resAddress = table.primaryKeyIndex().underlyingIndex().address(SimpleKey.of(7));

        var unsafe = MemoryUtils.UNSAFE;
        assert unsafe.getInt(resAddress + Schema.RECORD_HEADER) == 7;
        assert unsafe.getFloat( resAddress + Schema.RECORD_HEADER + Integer.BYTES ) == 7f;

    }

    @Test
    public void test(){

        var map = TestCommon.getDefaultCatalog();
        Table table = map.get("item");
//        String[] itemColumns = { "i_id", "i_price", "i_name", "i_data" };
        int maxItems = 10;
        int bufferSize = table.getSchema().getRecordSizeWithoutHeader() * maxItems;
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer( bufferSize );

        for(int i = 1; i <= maxItems; i++){
            produceItem(buffer, i);
        }

        SimplePlanner planner = new SimplePlanner();
        Analyzer analyzer = new Analyzer(map);
        TransactionFacade.build(map, replicaSchema, planner, analyzer).bulkInsert(table, buffer, maxItems);

        long resAddress = table.primaryKeyIndex().underlyingIndex().address(SimpleKey.of(7));

        var unsafe = MemoryUtils.UNSAFE;
        assert unsafe.getInt(resAddress + Schema.RECORD_HEADER) == 7;
        assert unsafe.getFloat( resAddress + Schema.RECORD_HEADER + Integer.BYTES ) == 7f;

    }

    private void produceItem(ByteBuffer buffer, int i){
        buffer.putInt(i);
        buffer.putFloat(i);
        /*
         * TODO would be good embrace length of string attributes
         * {@link javax.persistence.Column#length()}
          */
        DataTypeUtils.callWriteFunction(buffer, DataType.STRING, String.valueOf(i));
        DataTypeUtils.callWriteFunction(buffer, DataType.STRING, String.valueOf(i));
    }

}
