package dk.ku.di.dms.vms.sdk.embed.metadata;

import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.planner.Planner;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.facade.EmbedRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.facade.ModbModules;
import dk.ku.di.dms.vms.sdk.embed.ingest.BulkDataLoader;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

public class EmbedMetadataLoader {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    public static VmsRuntimeMetadata loadRuntimeMetadata(String... packages) {

        try {

            @SuppressWarnings("unchecked")
            Constructor<IVmsRepositoryFacade> constructor = (Constructor<IVmsRepositoryFacade>) EmbedRepositoryFacade.class.getConstructors()[0];

            return VmsMetadataLoader.load(packages, constructor);


        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            logger.warning("Cannot start VMs, error loading metadata: "+e.getMessage());
        }

        return null;

    }

    public static ModbModules loadModbModulesIntoRepositories(VmsRuntimeMetadata vmsRuntimeMetadata) throws NoSuchFieldException, IllegalAccessException {
        return loadModbModulesIntoRepositories(vmsRuntimeMetadata, new HashSet<>());
    }

    public static ModbModules loadModbModulesIntoRepositories(VmsRuntimeMetadata vmsRuntimeMetadata, Set<String> entitiesToExclude) throws NoSuchFieldException, IllegalAccessException {

        ModbModules modbModules = loadModbModules(vmsRuntimeMetadata, entitiesToExclude);

        for(Map.Entry<String, IVmsRepositoryFacade> facadeEntry : vmsRuntimeMetadata.repositoryFacades().entrySet()){
            ((EmbedRepositoryFacade)facadeEntry.getValue()).setModbModules(modbModules);
        }

        // instantiate loader
        BulkDataLoader loader = new BulkDataLoader( vmsRuntimeMetadata.repositoryFacades(), vmsRuntimeMetadata.entityToTableNameMap(), VmsSerdesProxyBuilder.build() );

        vmsRuntimeMetadata.loadedVmsInstances().put("data_loader", loader);

        return modbModules;

    }

    private static ModbModules loadModbModules(VmsRuntimeMetadata vmsRuntimeMetadata, Set<String> entitiesToExclude){

        Catalog catalog = loadCatalog(vmsRuntimeMetadata, entitiesToExclude);

        Analyzer analyzer = new Analyzer(catalog);
        Planner planner = new Planner();

        return new ModbModules(vmsRuntimeMetadata, catalog, analyzer, planner);
    }

    private static Catalog loadCatalog(VmsRuntimeMetadata vmsRuntimeMetadata, Set<String> entitiesToExclude) {

        Catalog catalog = new Catalog();

        for (VmsDataSchema vmsDataSchema : vmsRuntimeMetadata.dataSchema().values()) {

            if(!entitiesToExclude.contains(vmsDataSchema.tableName)) {

                Schema schema = new Schema(vmsDataSchema.columnNames, vmsDataSchema.columnDataTypes,
                        vmsDataSchema.primaryKeyColumns, null);

                // map this to a file, so whenever a batch commit arrives i can make the file durable

                RecordBufferContext recordBufferContext = loadMemoryBuffer(10, schema.getRecordSize(), vmsDataSchema.tableName);

                UniqueHashIndex pkIndex = new UniqueHashIndex(recordBufferContext, schema, schema.getPrimaryKeyColumns());

                Table table = new Table(vmsDataSchema.tableName, schema, pkIndex);

                // TODO create secondary indexes

                catalog.insertTable(table);
                catalog.insertIndex(pkIndex.key(), pkIndex);
            }

        }

        return catalog;
    }

    private static RecordBufferContext loadMemoryBuffer(int maxNumberOfRecords, int recordSize, String append){

        Cleaner cleaner = Cleaner.create();
        ResourceScope scope = ResourceScope.newSharedScope(cleaner);
        long sizeInBytes = (long) maxNumberOfRecords * recordSize;
        try {

            MemorySegment segment = mapFileIntoMemorySegment(sizeInBytes, append);
            return new RecordBufferContext(segment, maxNumberOfRecords, recordSize);

        } catch (Exception e){

            logger.warning("Could not map file. Resorting to direct memory allocation attempt: "+e.getMessage());

            MemorySegment segment = MemorySegment.allocateNative(sizeInBytes, scope);
            return new RecordBufferContext(segment, maxNumberOfRecords, recordSize);

        }

    }

    private static MemorySegment mapFileIntoMemorySegment(long bytes, String append) {

        String userHome = System.getProperty("user.home");

        if(userHome == null){
            logger.warning("User home directory is not set in the environment. Resorting to /usr/local/lib");
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
