package dk.ku.di.dms.vms.sdk.embed.metadata;

import dk.ku.di.dms.vms.modb.common.constraint.ForeignKeyReference;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsReplicatedTableSchema;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsTableSchema;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.planner.SimplePlanner;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.pk.PrimaryKeyGeneratorBuilder;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.facade.EmbedRepositoryFacade;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbedMetadataLoader {

    private static final Logger logger = LoggerFactory.getLogger(EmbedMetadataLoader.class);

    public static VmsRuntimeMetadata loadRuntimeMetadata(String... packages) {
        try {
            @SuppressWarnings("unchecked")
            Constructor<IVmsRepositoryFacade> constructor = (Constructor<IVmsRepositoryFacade>) EmbedRepositoryFacade.class.getConstructors()[0];
            return VmsMetadataLoader.load(packages, constructor);
        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("Cannot start VMs, error loading metadata: "+e.getMessage());
        }
    }

    public static TransactionFacade loadTransactionFacadeAndInjectIntoRepositories(VmsRuntimeMetadata vmsRuntimeMetadata) {
        return loadTransactionFacadeAndInjectIntoRepositories(vmsRuntimeMetadata, null);
    }

    public static TransactionFacade loadTransactionFacadeAndInjectIntoRepositories(
            VmsRuntimeMetadata vmsRuntimeMetadata, Set<String> entitiesToExclude) {

        Map<String, Table> tableSchema = createTables(vmsRuntimeMetadata, entitiesToExclude);
        Map<String, UniqueHashIndex> replicaSchema = createReplicatedTables(vmsRuntimeMetadata);

        SimplePlanner planner = new SimplePlanner();
        Analyzer analyzer = new Analyzer(tableSchema);
        TransactionFacade transactionFacade = TransactionFacade.build(tableSchema, replicaSchema, planner, analyzer);

        for(Map.Entry<String, IVmsRepositoryFacade> facadeEntry : vmsRuntimeMetadata.repositoryFacades().entrySet()){
            ((EmbedRepositoryFacade)facadeEntry.getValue()).setDynamicDatabaseModules(transactionFacade, tableSchema.get(facadeEntry.getKey()));
        }

        return transactionFacade;

    }

    private static Map<String, UniqueHashIndex> createReplicatedTables(VmsRuntimeMetadata vmsRuntimeMetadata){

    }

    private static Map<String, Table> createTables(VmsRuntimeMetadata vmsRuntimeMetadata, Set<String> entitiesToExclude) {

        Map<String, Table> catalog = new HashMap<>(vmsRuntimeMetadata.tableSchema().size());
        Map<VmsTableSchema, Tuple<Schema, Map<String, int[]>>> dataSchemaToPkMap = new HashMap<>(vmsRuntimeMetadata.tableSchema().size());

        /*
         * Build primary key index and map the foreign keys (internal to this VMS)
         */
        for (VmsTableSchema vmsDataSchema : vmsRuntimeMetadata.tableSchema().values()) {

            if(entitiesToExclude != null && !entitiesToExclude.contains(vmsDataSchema.tableName)) continue;

            final Schema schema = new Schema(vmsDataSchema.columnNames, vmsDataSchema.columnDataTypes,
                    vmsDataSchema.primaryKeyColumns, vmsDataSchema.constraintReferences);

            if(vmsDataSchema.foreignKeyReferences != null && vmsDataSchema.foreignKeyReferences.length > 0){
                // build
                Map<String, List<ForeignKeyReference>> res = Stream.of( vmsDataSchema.foreignKeyReferences )
                                .sorted( (x,y) -> schema.columnPosition( x.columnName() ) <= schema.columnPosition( y.columnName() ) ? -1 : 1 )
                        .collect( Collectors.groupingBy(ForeignKeyReference::vmsTableName ) );

                Map<String, int[]> definitiveMap = buildSchemaForeignKeyMap( schema, vmsRuntimeMetadata.tableSchema(), res );

                dataSchemaToPkMap.put(vmsDataSchema, Tuple.of(schema, definitiveMap));

            } else {
                dataSchemaToPkMap.put(vmsDataSchema, Tuple.of(schema, null));
            }

        }

        Map<String, PrimaryIndex> vmsDataSchemaToIndexMap = new HashMap<>(dataSchemaToPkMap.size());

        Map<String, List<NonUniqueHashIndex>> vmsDataSchemaToSecondaryIndexMap = new HashMap<>();

        // page size in bytes for non unique index bucket
        int pageSize = MemoryUtils.DEFAULT_PAGE_SIZE;
        VmsTableSchema vmsDataSchema;

        // mount vms data schema to consistent index map
        for (var entry : dataSchemaToPkMap.entrySet()) {

            Schema schema = entry.getValue().t1();

            // map this to a file, so whenever a batch commit event arrives, it can trigger logging the entire file
            RecordBufferContext recordBufferContext = loadRecordBuffer(10, schema.getRecordSize(), entry.getKey().tableName);
            UniqueHashIndex pkIndex = new UniqueHashIndex(recordBufferContext, schema, schema.getPrimaryKeyColumns());

            vmsDataSchema = entry.getKey();

            PrimaryIndex consistentIndex;
            if(vmsDataSchema.pkAutoGenerated) {
                Class<?> type = DataTypeUtils.getJavaTypeFromDataType(vmsDataSchema.columnDataTypes[vmsDataSchema.primaryKeyColumns[0]]);
                var gen = PrimaryKeyGeneratorBuilder.build(type);
                consistentIndex = new PrimaryIndex(pkIndex, gen);
            } else {
                consistentIndex = new PrimaryIndex(pkIndex);
            }

            vmsDataSchemaToIndexMap.put( entry.getKey().tableName, consistentIndex );

            List<NonUniqueHashIndex> listSecIdxs = new ArrayList<>(entry.getValue().t2().size());
            vmsDataSchemaToSecondaryIndexMap.put(entry.getKey().tableName, listSecIdxs);

            // now create the secondary index (a - based on foreign keys and b - based on non-foreign keys)
            for(var secIdx : entry.getValue().t2().entrySet()) {
                NonUniqueHashIndex nuhi = createNonUniqueIndex(schema, secIdx.getValue(), pageSize, 10,
                        entry.getKey().tableName + "_" + secIdx.getKey());
                listSecIdxs.add(nuhi);
            }

        }

        // now I have the pk indexes and the fks, build the table
        for (var entry : dataSchemaToPkMap.entrySet()) {

            vmsDataSchema = entry.getKey();
            Tuple<Schema, Map<String, int[]>> tupleSchemaFKs = entry.getValue();

            Map<PrimaryIndex, int[]> fks = new HashMap<>(tupleSchemaFKs.t2().size());

            // build fks
            for(var fk : tupleSchemaFKs.t2().entrySet()){
                fks.put( vmsDataSchemaToIndexMap.get( fk.getKey() ), fk.getValue() );
            }

            List<NonUniqueSecondaryIndex> list = new ArrayList<>();

            PrimaryIndex primaryIndex = vmsDataSchemaToIndexMap.get( vmsRuntimeMetadata.virtualMicroservice() );

            // build secondary indexes (for foreign keys)
            for(var idx : vmsDataSchemaToSecondaryIndexMap.get(vmsDataSchema.tableName)){
                list.add( new NonUniqueSecondaryIndex( primaryIndex, idx ) );
            }

            Table table = new Table(vmsDataSchema.tableName, tupleSchemaFKs.t1(), primaryIndex, fks, list);
            catalog.put( vmsDataSchema.tableName, table );
        }

        return catalog;
    }

    private static NonUniqueHashIndex createNonUniqueIndex(Schema schema, int[] columnsIndex, int bucketSize, int numBuckets, String fileName){
        OrderedRecordBuffer[] buffers = loadOrderedBuffers(numBuckets, bucketSize, fileName);
        return new NonUniqueHashIndex(buffers, schema,columnsIndex);
    }

    private static OrderedRecordBuffer[] loadOrderedBuffers(int numBuckets, int bucketSize, String fileName){

        long sizeInBytes = (long) numBuckets * bucketSize;

        Cleaner cleaner = Cleaner.create();
        ResourceScope scope = ResourceScope.newSharedScope(cleaner);
        MemorySegment segment;
        try {
            segment = mapFileIntoMemorySegment(sizeInBytes, fileName);
        } catch (Exception e){
            segment = MemorySegment.allocateNative(sizeInBytes, scope);
        }

        long address = segment.address().toRawLongValue();

        OrderedRecordBuffer[] buffers = new OrderedRecordBuffer[numBuckets];

        for(int i = 0; i < numBuckets; i++){
            buffers[i] = loadOrderedRecordBuffer(address, bucketSize);
            address = address + bucketSize;
        }

        return buffers;

    }

    private static Map<String, int[]> buildSchemaForeignKeyMap(Schema schema, Map<String, VmsTableSchema> dataSchemaMap, Map<String, List<ForeignKeyReference>> map) {
        Map<String, int[]> res = new HashMap<>();
        for( var entry : map.entrySet() ){
            VmsTableSchema dataSchema = dataSchemaMap.get( entry.getKey() );
            var list = entry.getValue();
            int[] intArray = list.stream().mapToInt(p-> schema.columnPosition( p.columnName() ) ).toArray();
            res.put( dataSchema.tableName, intArray );
        }
        return res;
    }

    private static OrderedRecordBuffer loadOrderedRecordBuffer(long address, int size){
        AppendOnlyBuffer appendOnlyBuffer = new AppendOnlyBuffer(address, size);
        return new OrderedRecordBuffer(appendOnlyBuffer);
    }

    private static RecordBufferContext loadRecordBuffer(int maxNumberOfRecords, int recordSize, String append){

        Cleaner cleaner = Cleaner.create();
        ResourceScope scope = ResourceScope.newSharedScope(cleaner);
        long sizeInBytes = (long) maxNumberOfRecords * recordSize;
        try {
            MemorySegment segment = mapFileIntoMemorySegment(sizeInBytes, append);
            return new RecordBufferContext(segment, maxNumberOfRecords);
        } catch (Exception e){
            logger.warn("Could not map file. Resorting to direct memory allocation attempt: "+e.getMessage());
            MemorySegment segment = MemorySegment.allocateNative(sizeInBytes, scope);
            return new RecordBufferContext(segment, maxNumberOfRecords);
        }

    }

    private static MemorySegment mapFileIntoMemorySegment(long bytes, String append) {

        String userHome = System.getProperty("user.home");

        if(userHome == null){
            logger.warn("User home directory is not set in the environment. Resorting to /usr/local/lib");
            userHome = "/usr/local/lib";
        }

        String filePath = userHome + "/vms/" + append;

        logger.info("Attempt to delete existing file in directory: "+filePath);

        File file = new File(filePath);
        if (file.exists()) {
            if(!file.delete()) throw new IllegalStateException("File can not be deleted");
        }

        logger.info("Attempt to create new file in directory: "+filePath);

        if(file.getParentFile().mkdirs()){
            logger.info("Parent directory required being created.");
        } else {
            logger.info("Parent directory don't need to be created.");
        }

        try {

            if(file.createNewFile()) {
                logger.info("Attempt to create new file in directory: "+filePath+" completed successfully.");
                return MemorySegment.mapFile(
                        file.toPath(),
                        0,
                        bytes,
                        FileChannel.MapMode.READ_WRITE,
                        ResourceScope.newSharedScope());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        throw new IllegalStateException("File could not be created");

    }

}
