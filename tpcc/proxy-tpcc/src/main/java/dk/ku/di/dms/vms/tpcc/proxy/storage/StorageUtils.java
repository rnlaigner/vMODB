package dk.ku.di.dms.vms.tpcc.proxy.storage;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.entities.*;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class StorageUtils {

    private static final System.Logger LOGGER = System.getLogger(StorageUtils.class.getName());

    private static final String PACKAGE = "dk.ku.di.dms.vms.tpcc.proxy";

    private static final Map<Class<?>, String> ENTITY_TO_VMS_MAP;
    static {
        ENTITY_TO_VMS_MAP = new HashMap<>();
        ENTITY_TO_VMS_MAP.put(Warehouse.class, "proxy");
        ENTITY_TO_VMS_MAP.put(District.class, "proxy");
        ENTITY_TO_VMS_MAP.put(Customer.class, "proxy");
        ENTITY_TO_VMS_MAP.put(Item.class, "proxy");
        ENTITY_TO_VMS_MAP.put(Stock.class, "proxy");
    }

    public static int getNumWarehousesInDiskTable(){
        String basePathStr = dk.ku.di.dms.vms.modb.utils.StorageUtils.getBasePath("proxy");
        Path basePath = Paths.get(basePathStr);
        try(Stream<Path> paths = Files.walk(basePath)){
            List<Path> workloadInputFiles = paths.filter(path -> path.toString().contains("stock")).toList();
            return workloadInputFiles.size();
        } catch (IOException e){
            LOGGER.log(ERROR, "Error captured while trying to access base path: \n"+e);
            return 0;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static EntityMetadata loadEntityMetadata() throws NoSuchFieldException, IllegalAccessException {
        Reflections reflections = VmsMetadataLoader.configureReflections(new String[]{ PACKAGE });
        Map<Class<?>, String> entityToTableNameMap = VmsMetadataLoader.loadVmsTableNames(reflections);

        // for cases of multiple VMSes deployed in the same process
        Map<Class<?>, String> filteredProxyEntities = entityToTableNameMap.entrySet().stream()
                .filter(clazz -> clazz.getKey().getPackage().getName().contains(PACKAGE))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, VmsDataModel> vmsDataModelMap = VmsMetadataLoader.buildVmsDataModel(ENTITY_TO_VMS_MAP, filteredProxyEntities);
        Map<String, EntityHandler> entityHandlerMap = new HashMap<>();
        Map<String, Schema> entityToSchemaMap = new HashMap<>();
        for (var entry : filteredProxyEntities.entrySet()) {
            VmsDataModel vmsDataModel = vmsDataModelMap.get(entry.getValue());
            Class<?> entityClazz = entry.getKey();
            Type[] types = ((ParameterizedType) entityClazz.getGenericInterfaces()[0]).getActualTypeArguments();
            Class<?> pkClazz = (Class<?>) types[0];
            final Schema schema = EmbedMetadataLoader.buildEntitySchema(vmsDataModel, entityClazz);
            entityToSchemaMap.put(entry.getValue(), schema);
            EntityHandler entityHandler = new EntityHandler(pkClazz, entityClazz, schema);
            entityHandlerMap.put(entry.getValue(), entityHandler);
        }
        return new EntityMetadata(filteredProxyEntities, vmsDataModelMap, entityHandlerMap, entityToSchemaMap);
    }

    @SuppressWarnings({"rawtypes"})
    public record EntityMetadata(Map<Class<?>, String> entityToTableNameMap, Map<String, VmsDataModel> vmsDataModelMap, Map<String, EntityHandler> entityHandlerMap, Map<String,Schema> entityToSchemaMap){}

    /**
     * Generate data and create tables in disk
     */

    public static Map<String, UniqueHashBufferIndex> createTables(EntityMetadata metadata, int numWare) {
        Map<String, UniqueHashBufferIndex> tableToIndexMap = new HashMap<>();
        for (String tableName : metadata.entityToTableNameMap.values()) {
            switch (tableName){
                case "stock" -> {
                    var stockTableMap = createStockTables(metadata, numWare);
                    tableToIndexMap.putAll(stockTableMap);
                }
                case "customer" -> {
                    var stockTableMap = createCustomerTables(metadata, numWare);
                    tableToIndexMap.putAll(stockTableMap);
                }
                default -> {
                    UniqueHashBufferIndex idx = createTable(metadata, numWare, tableName);
                    tableToIndexMap.put(tableName, idx);
                }
            }
        }
        return tableToIndexMap;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, UniqueHashBufferIndex> createStockTables(EntityMetadata metadata, int numWare) {
        Map<String, UniqueHashBufferIndex> tables = new HashMap<>(numWare);
        final Schema schema = metadata.entityToSchemaMap.get("stock");
        var entityHandler = metadata.entityHandlerMap.get("stock");
        int maxRecords = numWare * TPCcConstants.NUM_ITEMS;
        LOGGER.log(INFO, "Creating "+maxRecords+" stock records...");
        int overflowDisk = MemoryUtils.nextPowerOfTwo(TPCcConstants.NUM_ITEMS);
        if(numWare > 16) overflowDisk = overflowDisk * 2;
        long initTs = System.currentTimeMillis();
        for(int w_id = 1; w_id <= numWare; w_id++) {
            String tableName = "stock_"+w_id;
            UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, true);
            long internalInitTs = System.currentTimeMillis();
            for (int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++) {
                Stock stock = DataGenerator.generateStockItem(w_id, i_id);
                Object[] record = entityHandler.extractFieldValuesFromEntityObject(stock);
                IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                idx.insert(key, record);
            }
            LOGGER.log(INFO, "Finished creating "+TPCcConstants.NUM_ITEMS+" stock records for warehouse "+w_id+" in "+(System.currentTimeMillis()-internalInitTs)+" ms");
            idx.flush();
            tables.put(tableName, idx);
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished creating "+maxRecords+" stock records in "+(endTs-initTs)+" ms");
        return tables;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, UniqueHashBufferIndex> createCustomerTables(EntityMetadata metadata, int numWare) {
        Map<String, UniqueHashBufferIndex> tables = new HashMap<>(numWare);
        final Schema schema = metadata.entityToSchemaMap.get("customer");
        var entityHandler = metadata.entityHandlerMap.get("customer");
        int maxRecordsPerWarehouse = TPCcConstants.NUM_DIST_PER_WARE * TPCcConstants.NUM_CUST_PER_DIST;
        int maxRecords = numWare * maxRecordsPerWarehouse;
        LOGGER.log(INFO, "Creating "+maxRecords+" customer records...");
        int overflowDisk = MemoryUtils.nextPowerOfTwo(maxRecordsPerWarehouse);
        if(numWare > 4) overflowDisk = overflowDisk * 2;
        if(numWare == 32) overflowDisk = overflowDisk * 2;
        long initTs = System.currentTimeMillis();
        for(int w_id = 1; w_id <= numWare; w_id++){
            String tableName = "customer_"+w_id;
            UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, true);
            long internalInitTs = System.currentTimeMillis();
            for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                for(int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++) {
                    Customer customer = DataGenerator.generateCustomer(c_id, d_id, w_id);
                    Object[] record = entityHandler.extractFieldValuesFromEntityObject(customer);
                    IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                    idx.insert(key, record);
                }
            }
            LOGGER.log(INFO, "Finished creating "+(maxRecordsPerWarehouse)+" customer records for warehouse "+w_id+" in "+(System.currentTimeMillis()-internalInitTs)+" ms");
            idx.flush();
            tables.put(tableName, idx);
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished creating "+maxRecords+" customer records in "+(endTs-initTs)+" ms");
        return tables;
    }

    @SuppressWarnings("unchecked")
    public static UniqueHashBufferIndex createTable(EntityMetadata metadata, int numWare, String tableName) {
        final Schema schema = metadata.entityToSchemaMap.get(tableName);
        var entityHandler = metadata.entityHandlerMap.get(tableName);
        // generate data and store in respective indexes
        switch (tableName){
            case "warehouse" -> {
                int overflowDisk = MemoryUtils.nextPowerOfTwo(numWare);
                if(numWare == 32) overflowDisk = overflowDisk * 2;
                LOGGER.log(INFO, "Creating "+ numWare +" warehouse records...");
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, true);
                for(int w_id = 1; w_id <= numWare; w_id++){
                    Warehouse warehouse = DataGenerator.generateWarehouse(w_id);
                    Object[] record = entityHandler.extractFieldValuesFromEntityObject(warehouse);
                    IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                    idx.insert(key, record);
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+ numWare +" warehouse records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case "district" -> {
                int maxRecords = numWare * TPCcConstants.NUM_DIST_PER_WARE;
                LOGGER.log(INFO, "Creating "+maxRecords+" district records...");
                int overflowDisk = MemoryUtils.nextPowerOfTwo(maxRecords);
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, true);
                for(int w_id = 1; w_id <= numWare; w_id++){
                    for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                        District district = DataGenerator.generateDistrict(d_id, w_id);
                        Object[] record = entityHandler.extractFieldValuesFromEntityObject(district);
                        IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                        idx.insert(key, record);
                    }
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+maxRecords+" district records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case "item" -> {
                int overflowDisk = MemoryUtils.nextPowerOfTwo(TPCcConstants.NUM_ITEMS);
                LOGGER.log(INFO, "Creating "+TPCcConstants.NUM_ITEMS+" item records...");
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, true);
                for(int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++){
                    Item item = DataGenerator.generateItem(i_id);
                    Object[] record = entityHandler.extractFieldValuesFromEntityObject(item);
                    IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                    idx.insert(key, record);
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+TPCcConstants.NUM_ITEMS+" item records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case null, default -> throw new RuntimeException("Table name "+tableName+" not identified!");
        }
    }

    public static UniqueHashBufferIndex buildHashIndex(String tableName, Schema schema, int maxRecords, boolean truncate) {
        RecordBufferContext recordBufferContext = dk.ku.di.dms.vms.modb.utils.StorageUtils.loadRecordBuffer("proxy", maxRecords, schema.getRecordSizeWithHeader(), tableName, truncate);
        return new UniqueHashBufferIndex(recordBufferContext, schema, schema.getPrimaryKeyColumns(), maxRecords);
    }

    /**
     * Map already created tables in disk
     */
    public static Map<String, UniqueHashBufferIndex> mapTablesInDisk(EntityMetadata metadata, int numWare) {
        Map<String, UniqueHashBufferIndex> tableToIndexMap = new HashMap<>();
        for (var entry : metadata.entityToTableNameMap.entrySet()) {
            final Schema schema = metadata.entityToSchemaMap.get(entry.getValue());
            switch (entry.getValue()){
                case "warehouse" -> {
                    int overflowDisk = MemoryUtils.nextPowerOfTwo(numWare);
                    if(numWare == 32) overflowDisk = overflowDisk * 2;
                    LOGGER.log(INFO, "Loading "+numWare+" warehouses...");
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, overflowDisk, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
                case "district" -> {
                    int maxRecords = numWare * TPCcConstants.NUM_DIST_PER_WARE;
                    int overflowDisk = MemoryUtils.nextPowerOfTwo(maxRecords);
                    LOGGER.log(INFO, "Loading "+maxRecords+" districts...");
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, overflowDisk, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
                case "customer" -> {
                    int maxRecordsPerWarehouse = TPCcConstants.NUM_DIST_PER_WARE * TPCcConstants.NUM_CUST_PER_DIST;
                    int maxRecords = numWare * maxRecordsPerWarehouse;
                    LOGGER.log(INFO, "Loading "+maxRecords+" customers...");
                    int overflowDisk = MemoryUtils.nextPowerOfTwo(maxRecordsPerWarehouse);
                    if(numWare > 4) overflowDisk = overflowDisk * 2;
                    if(numWare == 32) overflowDisk = overflowDisk * 2;
                    for(int ware_id = 1; ware_id <= numWare; ware_id++) {
                        var tableName = entry.getValue()+"_"+ware_id;
                        UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, false);
                        tableToIndexMap.put(tableName, idx);
                    }
                }
                case "item" -> {
                    int overflowDisk = MemoryUtils.nextPowerOfTwo(TPCcConstants.NUM_ITEMS);
                    LOGGER.log(INFO, "Loading "+TPCcConstants.NUM_ITEMS+" items...");
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, overflowDisk, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
                case "stock" -> {
                    int maxRecords = numWare * TPCcConstants.NUM_ITEMS;
                    int overflowDisk = MemoryUtils.nextPowerOfTwo(TPCcConstants.NUM_ITEMS);
                    LOGGER.log(INFO, "Loading "+maxRecords+" stock items...");
                    if(numWare > 16) overflowDisk = overflowDisk * 2;
                    for(int ware_id = 1; ware_id <= numWare; ware_id++) {
                        var tableName = entry.getValue()+"_"+ware_id;
                        UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, false);
                        tableToIndexMap.put(tableName, idx);
                    }
                }
            }
        }
        return tableToIndexMap;
    }

}
