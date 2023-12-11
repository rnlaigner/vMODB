package dk.ku.di.dms.vms.sdk.embed.ingest;

import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.annotations.Loader;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;

import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;

/**
 * In the future we can define whether the bulk data load has transactional guarantees or not.
 */
@Loader("data_loader")
public class BulkDataLoader {

    private static final Logger logger = Logger.getLogger("BulkDataLoader");

    private final Map<String, IVmsRepositoryFacade> repositoryFacades;
    private final Map<String, Class<?>> tableNameToEntityClazzMap;

    private final IVmsSerdesProxy serdes;

    public BulkDataLoader(Map<String, IVmsRepositoryFacade> repositoryFacades, Map<Class<?>, String> entityToTableNameMap, IVmsSerdesProxy serdes){
        this.repositoryFacades = repositoryFacades;

        this.tableNameToEntityClazzMap =
                entityToTableNameMap.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        this.serdes = serdes;
    }

    public void init(String tableName, LockConnectionMetadata connectionMetadata){
        new BulkDataLoaderProtocol( tableNameToEntityClazzMap.get(tableName), repositoryFacades.get(tableName) ).init( connectionMetadata );
    }

    private class BulkDataLoaderProtocol {

        private final Class<?> type;
        private final IVmsRepositoryFacade repositoryFacade;

        protected BulkDataLoaderProtocol(Class<?> type, IVmsRepositoryFacade repositoryFacade){
            this.type = type;
            this.repositoryFacade = repositoryFacade;
        }

        public void init(LockConnectionMetadata connectionMetadata){
            connectionMetadata.channel.read( connectionMetadata.readBuffer,
                    connectionMetadata, new ReadCompletionHandler() );
        }

        private class ReadCompletionHandler implements CompletionHandler<Integer, LockConnectionMetadata> {

            @Override
            public void completed(Integer result, LockConnectionMetadata connectionMetadata) {

                logger.info("New bulk data received. Result =="+result);

                if(result == -1) {
                    try {
                        connectionMetadata.channel.close();
                    } catch (IOException ignored) {}
                    return;
                }

                // should be a batch of events
                connectionMetadata.readBuffer.position(0);
                byte messageType = connectionMetadata.readBuffer.get();

                assert (messageType == BATCH_OF_EVENTS);

                int count = connectionMetadata.readBuffer.getInt();

                logger.info(count+" records to be ingested!");

                List<Object> entities = new ArrayList<>(count);
                for(int i = 0; i < count; i++){
                    int size = connectionMetadata.readBuffer.getInt();
                    String objStr = ByteUtils.extractStringFromByteBuffer(connectionMetadata.readBuffer, size);
                    Object object = serdes.deserialize( objStr, type );
                    entities.add(object);
                }

                repositoryFacade.insertAll( entities );

                connectionMetadata.readBuffer.clear();
                logger.info("Ingestion finished!");

                // set up read handler again without waiting for tx facade
                connectionMetadata.channel.read( connectionMetadata.readBuffer,
                        connectionMetadata, this );

            }

            @Override
            public void failed(Throwable exc, LockConnectionMetadata connectionMetadata) { }

        }

    }


}
