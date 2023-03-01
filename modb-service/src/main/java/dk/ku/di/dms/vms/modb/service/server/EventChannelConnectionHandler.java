package dk.ku.di.dms.vms.modb.service.server;

import dk.ku.di.dms.vms.modb.common.schema.meta.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Handler might be better to avoid the handlers from consuming CPU...
 */
public class EventChannelConnectionHandler implements CompletionHandler<AsynchronousSocketChannel, String> {

    private static final Logger logger = LoggerFactory.getLogger("EventChannelConnectionHandler");

    private final CountDownLatch latch;
    private final IVmsSerdesProxy serdes;

    private Map<String,VmsEventSchema> eventSchemaMap = null;

    private final ByteBuffer writeBuffer;

    public EventChannelConnectionHandler(IVmsSerdesProxy serdes, ByteBuffer writeBuffer){
        this.latch = new CountDownLatch(1);
        this.serdes = serdes;
        this.writeBuffer = writeBuffer;
    }

    @Override
    public void completed(AsynchronousSocketChannel socketChannel, String attachment) {

        // handshake
        eventSchemaMap = serdes.deserializeEventSchema(attachment);

        latch.countDown();

//        writeBuffer.put( serdes.serializeSystemEvent( new SystemEvent(1) ) );
//
//        // write ack to framework/sdk/runtime
//        socketChannel.write( writeBuffer );

        // FIXME what if the write does not succeed? can i somehow return this future together with the result to the server thread?
    }

    /**
     * The vms server will block after calling this method
     */
    public Map<String,VmsEventSchema> get(){
        try {
            latch.await();
            return eventSchemaMap;
        } catch (InterruptedException e) {
            logger.info("Error on the synchronization mechanism used to retrieve the event schema.");
            return null;
        }
    }

    @Override
    public void failed(Throwable exc, String attachment) {
        latch.countDown();
    }

}