package dk.ku.di.dms.vms.sdk.embed.ingest;

import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.annotations.Loader;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static dk.ku.di.dms.vms.modb.common.schema.Constants.BATCH_OF_TRANSACTION_EVENTS;

/**
 * This class has been previously created to allow for fast data ingestion and support some tests.
 * In the future we can define whether the bulk data load has transactional guarantees or not.
 */
@Loader("data_loader")
public class BulkDataLoader {

    private static final Logger logger = LoggerFactory.getLogger("BulkDataLoader");
    private final Map<String, IVmsRepositoryFacade> repositoryFacades;
    private final Map<String, Class<?>> tableNameToEntityClazzMap;
    private final IVmsSerdesProxy serdes;

    public BulkDataLoader(Map<String, IVmsRepositoryFacade> repositoryFacades, Map<String, Class<?>> tableNameToEntityClazzMap, IVmsSerdesProxy serdes){
        this.repositoryFacades = repositoryFacades;
        this.tableNameToEntityClazzMap = tableNameToEntityClazzMap;
        this.serdes = serdes;
    }

    public void init(String tableName, ConnectionMetadata connectionMetadata){
        new BulkDataLoaderProtocol( tableNameToEntityClazzMap.get(tableName), repositoryFacades.get(tableName) ).init( connectionMetadata );
    }

    private class BulkDataLoaderProtocol {
        private final Class<?> type;
        private final IVmsRepositoryFacade repositoryFacade;

        protected BulkDataLoaderProtocol(Class<?> type, IVmsRepositoryFacade repositoryFacade){
            this.type = type;
            this.repositoryFacade = repositoryFacade;
        }

        public void init(ConnectionMetadata connectionMetadata){
            connectionMetadata.channel().read( connectionMetadata.buffer(),
                    connectionMetadata, new ReadCompletionHandler() );
        }

        private class ReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

            @Override
            public void completed(Integer result, ConnectionMetadata connectionMetadata) {

                logger.info("New bulk data received. Result =="+result);

                if(result == -1) {
                    try {
                        connectionMetadata.channel().close();
                    } catch (IOException ignored) {}
                    return;
                }

                // should be a batch of events
                connectionMetadata.buffer().position(0);
                byte messageType = connectionMetadata.buffer().get();

                assert (messageType == BATCH_OF_TRANSACTION_EVENTS);

                int count = connectionMetadata.buffer().getInt();

                logger.info(count+" records to be ingested!");

                List<Object> entities = new ArrayList<>(count);
                for(int i = 0; i < count; i++){
                    int size = connectionMetadata.buffer().getInt();
                    String objStr = ByteUtils.extractStringFromByteBuffer(connectionMetadata.buffer(), size);
                    Object object = serdes.deserialize( objStr, type );
                    entities.add(object);
                }

                repositoryFacade.insertAll( entities );

                connectionMetadata.buffer().clear();
                logger.info("Ingestion finished!");

                // set up read handler again without waiting for tx facade
                connectionMetadata.channel().read( connectionMetadata.buffer(),
                        connectionMetadata, this );

            }

            @Override
            public void failed(Throwable exc, ConnectionMetadata connectionMetadata) { }

        }

    }


}
