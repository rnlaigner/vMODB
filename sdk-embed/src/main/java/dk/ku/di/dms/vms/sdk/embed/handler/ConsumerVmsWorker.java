package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

/**
 * This thread encapsulates the batch of events sending task
 * that should occur periodically. Once set up, it schedules itself
 * after each run, thus avoiding duplicate runs of the same task.
 * -
 * I could get the connection from the vms...
 * But in the future, one output event will no longer map to a single vms
 * So it is better to make the event sender task complete enough
 * FIXME what happens if the connection fails? then put the list of events
 *  in the batch to resend or return to the original location. let the
 *  main loop schedule the timer again. set the network node to off
 */
final class ConsumerVmsWorker extends StoppableRunnable {

    private final VmsNode me;

    private final Logger logger;
    private final ConsumerVms consumerVms;
    private final ConnectionMetadata connectionMetadata;

    private final Deque<ByteBuffer> writeBufferPool;

    private final BlockingQueue<Byte> WRITE_SYNCHRONIZER = new ArrayBlockingQueue<>(1);

    private static final Byte DUMB = 1;

    private final int networkBufferSize;

    public ConsumerVmsWorker(VmsNode me, ConsumerVms consumerVms, ConnectionMetadata connectionMetadata, int networkBufferSize){
        this.me = me;
        this.consumerVms = consumerVms;
        this.connectionMetadata = connectionMetadata;
        this.logger = Logger.getLogger("consumer-vms-worker-"+consumerVms.identifier);
        this.logger.setUseParentHandlers(true);
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(networkBufferSize) );
        this.writeBufferPool.addFirst( MemoryManager.getTemporaryDirectBuffer(networkBufferSize) );

        // to allow the first thread to write
        this.WRITE_SYNCHRONIZER.add(DUMB);

        this.networkBufferSize = networkBufferSize;
    }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    @Override
    public void run() {

        this.logger.info(this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);

        List<TransactionEvent.PayloadRaw> events = new ArrayList<>(1000);
        while(this.isRunning()){

            try {
                events.add(this.consumerVms.transactionEvents.take());
            } catch (InterruptedException ignored) {
                continue;
            }

            this.consumerVms.transactionEvents.drainTo(events);

            if(events.size() == 1){
                this.logger.info(this.me.identifier+ ": Submitting 1 event to "+this.consumerVms.identifier);

                ByteBuffer writeBuffer = this.retrieveByteBuffer();
                TransactionEvent.write( writeBuffer, events.getFirst() );
                writeBuffer.flip();

                try {
                    this.WRITE_SYNCHRONIZER.take();
                    this.connectionMetadata.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
                } catch (InterruptedException ignored) {
                    this.consumerVms.transactionEvents.offerFirst(events.getFirst());
                }
                events.clear();
                continue;
            }

            int remaining = events.size();
            int count = remaining;
            ByteBuffer writeBuffer;
            while(remaining > 0){
                try {
                    writeBuffer = this.retrieveByteBuffer();
                    remaining = BatchUtils.assembleBatchPayload(remaining, events, writeBuffer);

                    this.logger.info(this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                    count = remaining;

                    writeBuffer.flip();

                    this.WRITE_SYNCHRONIZER.take();
                    this.connectionMetadata.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);

                    // prevent from error in consumer
//                    if(remaining > 0)
//                        sleep_();
                } catch (Exception e) {
                    this.logger.severe(this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier);
                    // return non-processed events to original location or what?
                    if (!this.connectionMetadata.channel.isOpen()) {
                        this.logger.warning("The "+this.consumerVms.identifier+" VMS is offline");
                    }
                    // return events to the deque
                    for (TransactionEvent.PayloadRaw event : events) {
                        this.consumerVms.transactionEvents.offerFirst(event);
                    }
                }
            }

            events.clear();

        }

    }

    private final Random random = new Random();

    /**
     * For some reason without sleeping, the bytebuffer gets corrupted in the consumer
     */
    private void sleep_(){
        // logger.info("Leader: Preparing another submission to: "+vmsNode.vmsIdentifier+" by thread "+Thread.currentThread().getId());
        // necessary to avoid buggy behavior: corrupted byte buffer. reason is unknown. maybe something related to operating system?
        try { sleep(this.random.nextInt(100)); } catch (InterruptedException ignored) {}
    }

    private ByteBuffer retrieveByteBuffer(){
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(this.networkBufferSize);
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        this.writeBufferPool.add(bb);
    }

    private class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer attachment) {
            logger.info(me.identifier+ ": Batch with size "+result+" has been sent to: "+consumerVms.identifier);
            WRITE_SYNCHRONIZER.add(DUMB);
            returnByteBuffer(attachment);
        }
        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            logger.severe(me.identifier+": ERROR on writing batch of events to: "+consumerVms.identifier);
            WRITE_SYNCHRONIZER.add(DUMB);
            returnByteBuffer(attachment);
        }
    }
}