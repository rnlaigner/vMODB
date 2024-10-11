package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.compressing.CompressingUtils;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.ProducerWorker;
import dk.ku.di.dms.vms.web_common.channel.IChannel;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.COMPRESSED_BATCH_OF_EVENTS;
import static dk.ku.di.dms.vms.sdk.embed.handler.ConsumerVmsWorker.State.*;
import static java.lang.System.Logger.Level.*;

/**
 * This thread encapsulates the batch of events sending task
 * that should occur periodically. Once set up, it schedules itself
 * after each run, thus avoiding duplicate runs of the same task.
 * -
 * I could get the connection from the vms...
 * But in the future, one output event will no longer map to a single vms
 * So it is better to make the event sender task complete enough
 * what happens if the connection fails? then put the list of events
 *  in the batch to resend or return to the original location. let the
 *  main loop schedule the timer again. set the network node to off
 */
public final class ConsumerVmsWorker extends ProducerWorker implements IVmsContainer {

    private static final System.Logger LOGGER = System.getLogger(ConsumerVmsWorker.class.getName());

    private final VmsNode me;

    private final IChannel channel;

    private final WriteCompletionHandler writeCompletionHandler;

    private final VmsEventHandler.VmsHandlerOptions options;

    private final MpscBlockingConsumerArrayQueue<TransactionEvent.PayloadRaw> transactionEventQueue;

    private State state;

    protected enum State { NEW, CONNECTED, PRESENTATION_SENT, READY }

    public static ConsumerVmsWorker build(
                                  VmsNode me,
                                  IdentifiableNode consumerVms,
                                  Supplier<IChannel> channelSupplier,
                                  VmsEventHandler.VmsHandlerOptions options,
                                  IVmsSerdesProxy serdesProxy) {
        return new ConsumerVmsWorker(me, consumerVms, channelSupplier.get(), options, serdesProxy);
    }

    private ConsumerVmsWorker(VmsNode me,
                             IdentifiableNode consumerVms,
                             IChannel channel,
                             VmsEventHandler.VmsHandlerOptions options,
                             IVmsSerdesProxy serdesProxy) {
        super(me.identifier, consumerVms, serdesProxy, options.logging());
        this.me = me;
        this.channel = channel;
        this.options = options;
        this.writeCompletionHandler = new WriteCompletionHandler();
        this.transactionEventQueue = new MpscBlockingConsumerArrayQueue<>(1024*100);
        this.state = NEW;
    }

    @Override
    public void run() {
        LOGGER.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);
        if(!this.connect()) {
            LOGGER.log(WARNING, this.me.identifier+ ": Finishing prematurely worker for consumer VMS "+this.consumerVms.identifier+" because connection failed");
            return;
        }
        if(this.options.logging()){
            this.eventLoopLogging();
        } else {
            if(this.options.compressing()){
                this.eventLoopNoLoggingCompressing();
            } else {
                this.eventLoopNoLogging();
            }
        }
        LOGGER.log(INFO, this.me.identifier+ ": Finishing worker for consumer VMS: "+this.consumerVms.identifier);
    }

    private void eventLoopNoLogging() {
        while(this.isRunning()){
            try {
                this.drained.add(this.transactionEventQueue.take());
                this.transactionEventQueue.drain(this.drained::add);
                if(this.drained.size() == 1){
                    this.sendEventNonBlocking(this.drained.removeFirst());
                } else {
                     this.sendBatchOfEventsNonBlocking();
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop (no logging) \n"+e);
            }
        }
    }

    private void eventLoopLogging() {
        while(this.isRunning()){
            try {
                if(this.loggingWriteBuffers.isEmpty()){
                    this.drained.add(this.transactionEventQueue.take());
                }
                this.transactionEventQueue.drain(this.drained::add);
                if(this.drained.isEmpty()){
                    this.processPendingLogging();
                } else {
                    this.sendBatchOfEventsNonBlockingWithLogging();
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop (logging) \n"+e);
            }
        }
    }

    private void eventLoopNoLoggingCompressing() {
        while(this.isRunning()){
            try {
                if(this.pendingWritesBuffer.isEmpty()) {
                    this.drained.add(this.transactionEventQueue.take());
                    this.transactionEventQueue.drain(this.drained::add);
                    if(this.drained.size() == 1){
                        this.sendEventNonBlocking(this.drained.removeFirst());
                    } else if(this.drained.size() < 10){
                        this.sendBatchOfEventsNonBlockingWithLogging();
                    } else {
                        this.sendCompressedBatchOfEvents();
                    }
                } else {
                    this.processPendingWrites();
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop (no logging, compressing) \n"+e);
            }
        }
    }

    private void sendEventNonBlocking(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize());
        TransactionEvent.write(writeBuffer, payload);
        writeBuffer.flip();
        this.acquireLock();
        this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
    }

    /**
     * Responsible for making sure the handshake protocol is successfully performed with a consumer VMS
     */
    private boolean connect() {
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(this.options.networkBufferSize());
        try{
            NetworkUtils.configure(this.channel.getNetworkChannel(), this.options.osBufferSize());
            this.channel.connect(this.consumerVms.asInetSocketAddress()).get();
            this.state = CONNECTED;
            LOGGER.log(DEBUG,this.me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);
            String dataSchema = this.serdesProxy.serializeDataSchema(this.me.dataSchema);
            String inputEventSchema = this.serdesProxy.serializeEventSchema(this.me.inputEventSchema);
            String outputEventSchema = this.serdesProxy.serializeEventSchema(this.me.outputEventSchema);
            buffer.clear();
            Presentation.writeVms( buffer, this.me, this.me.identifier, this.me.batch, 0, this.me.previousBatch, dataSchema, inputEventSchema, outputEventSchema );
            buffer.flip();
            this.channel.write(buffer);
            this.state = PRESENTATION_SENT;
            LOGGER.log(DEBUG,me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);
            returnByteBuffer(buffer);
            LOGGER.log(INFO,me.identifier+ ": Setting up worker to send transactions to consumer VMS: "+this.consumerVms.identifier);
        } catch (Exception e) {
            // check if connection is still online. if so, try again
            // otherwise, retry connection in a few minutes
            LOGGER.log(ERROR, me.identifier + ": Caught an error while trying to connect to consumer VMS: " + this.consumerVms.identifier);
            return false;
        } finally {
            buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(buffer);
        }
        this.state = READY;
        return true;
    }

    private void processPendingLogging(){
        ByteBuffer writeBuffer;
        if((writeBuffer = this.loggingWriteBuffers.poll())!= null){
            try {
                writeBuffer.position(0);
                this.loggingHandler.log(writeBuffer);
                returnByteBuffer(writeBuffer);
            } catch (Exception e) {
                LOGGER.log(ERROR, me.identifier + ": Error on writing byte buffer to logging file: "+e.getMessage());
                e.printStackTrace(System.out);
                this.loggingWriteBuffers.add(writeBuffer);
            }
        }
    }

    private void processPendingWrites() {
        // do we have pending writes?
        ByteBuffer bb = this.pendingWritesBuffer.pollFirst();
        if (bb == null) {
            return;
        }
        LOGGER.log(INFO, me.identifier+": Retrying sending failed buffer to "+consumerVms.identifier);
        try {
            // sleep with the intention to let the OS flush the previous buffer
            // try { sleep(100); } catch (InterruptedException ignored) { }
            this.acquireLock();
            this.channel.write(bb, options.networkSendTimeout(), TimeUnit.MILLISECONDS, bb, this.writeCompletionHandler);
        } catch (Exception e) {
            LOGGER.log(ERROR, me.identifier+": ERROR on retrying to send failed buffer to "+consumerVms.identifier+": \n"+e);
            if(e instanceof IllegalStateException){
                LOGGER.log(INFO, me.identifier+": Connection to "+consumerVms.identifier+" is open? "+this.channel.isOpen());
                // probably comes from the class {@AsynchronousSocketChannelImpl}:
                // "Writing not allowed due to timeout or cancellation"
                this.stop();
            }
            this.releaseLock();
            bb.position(0);
            this.pendingWritesBuffer.addFirst(bb);
        }
    }

    private void sendBatchOfEventsNonBlockingWithLogging() {
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        boolean lockAcquired = false;
        while(remaining > 0){
            try {
                writeBuffer = retrieveByteBuffer(this.options.networkBufferSize());
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                // maximize useful work
                while(!this.tryAcquireLock()){
                    this.processPendingLogging();
                }
                lockAcquired = true;
                this.channel.write(writeBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                remaining = 0;
                if(lockAcquired) {
                    this.releaseLock();
                }
            }
        }
        this.drained.clear();
    }

    private void sendEventBlocking(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize());
        try {
            TransactionEvent.write(writeBuffer, payload);
            writeBuffer.flip();
            this.channel.write(writeBuffer);
        } catch (Exception e){
            LOGGER.log(ERROR, "Error caught on sending single event: "+e);
            this.transactionEventQueue.offer(payload);
        } finally {
            returnByteBuffer(writeBuffer);
        }
    }

    public final class MultiBufferCompletionHandler implements CompletionHandler<Long, ByteBuffer[]> {
        @Override
        public void completed(Long result, ByteBuffer[] srcs) {
            int offset = -1;
            for (int i = 0; i < srcs.length; i++) {
                if(srcs[i].hasRemaining()){
                    offset = i;
                    break;
                }
            }
            if (offset != -1) {
                channel.write(srcs, offset, srcs,this);
            } else {
                releaseLock();
                for (ByteBuffer src : srcs){
                    returnByteBuffer(src);
                }
            }
        }
        @Override
        public void failed(Throwable exc, ByteBuffer[] srcs) {
            LOGGER.log(ERROR, "Error caught on sending multiple buffers: "+exc);
            releaseLock();
            for (ByteBuffer src : srcs) {
                if(src.hasRemaining()){
                    pendingWritesBuffer.add(src);
                } else {
                    returnByteBuffer(src);
                }
            }
        }
    }

    private final MultiBufferCompletionHandler multiBufferWriteCompletionHandler = new MultiBufferCompletionHandler();

    private void sendBatchOfEventsNonBlocking() {
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = retrieveByteBuffer(this.options.networkBufferSize());
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                if(this.tryAcquireLock()){
                    if(this.pendingWritesBuffer.isEmpty()) {
                        this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
                    } else {
                        ByteBuffer bb;
                        ByteBuffer[] srcs = new ByteBuffer[this.pendingWritesBuffer.size() + 1];
                        srcs[0] = writeBuffer;
                        int idx = 1;
                        while ((bb = this.pendingWritesBuffer.poll()) != null) {
                            srcs[idx] = bb;
                            idx++;
                            if(idx == srcs.length) break;
                        }
                        this.channel.write(srcs, 0, srcs, this.multiBufferWriteCompletionHandler);
                    }
                } else {
                    this.pendingWritesBuffer.addLast(writeBuffer);
                }
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                // force loop exit
                remaining = 0;
            }
        }
        this.drained.clear();
    }

    private void sendBatchOfEventsBlocking() {
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = retrieveByteBuffer(this.options.networkBufferSize());
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                this.channel.write(writeBuffer);
                returnByteBuffer(writeBuffer);
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                // force loop exit
                remaining = 0;
            }
        }
        this.drained.clear();
    }

    private void failSafe(Exception e, ByteBuffer writeBuffer) {
        LOGGER.log(ERROR, this.me.identifier+ ": Error submitting events to "+this.consumerVms.identifier+"\n"+ e);
        // return non-processed events to original location or what?
        if (!this.channel.isOpen()) {
            LOGGER.log(WARNING, "The "+this.consumerVms.identifier+" VMS is offline");
        }
        // return events to the deque
        this.transactionEventQueue.addAll(this.drained);
        if(writeBuffer != null) {
            returnByteBuffer(writeBuffer);
        }
    }

    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            LOGGER.log(DEBUG, me.identifier + ": Batch with size " + result + " has been sent to: " + consumerVms.identifier);
            if(byteBuffer.hasRemaining()){
                LOGGER.log(WARNING, me.identifier + ": Remaining bytes will be sent to: " + consumerVms.identifier);
                // keep the lock and send the remaining
                channel.write(byteBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                if(options.logging()){
                    loggingWriteBuffers.add(byteBuffer);
                } else {
                    returnByteBuffer(byteBuffer);
                }
                releaseLock();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            releaseLock();
            LOGGER.log(ERROR, me.identifier+": ERROR on writing batch of events to "+consumerVms.identifier+": \n"+exc);
            byteBuffer.position(0);
            pendingWritesBuffer.addFirst(byteBuffer);
            LOGGER.log(INFO, me.identifier + ": Byte buffer added to pending queue. #pending: "+ pendingWritesBuffer.size());
        }
    }

    @Override
    public boolean queue(TransactionEvent.PayloadRaw eventPayload){
        return this.transactionEventQueue.offer(eventPayload);
    }

    @Override
    public String identifier() {
        return this.consumerVms.identifier;
    }

    private void sendCompressedBatchOfEvents() {
        int remaining = this.drained.size();
        ByteBuffer writeBuffer = null;
        boolean lockAcquired = false;
        while(remaining > 0){
            try {
                writeBuffer = retrieveByteBuffer(this.options.networkBufferSize());
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer, CUSTOM_HEADER);
                writeBuffer.flip();
                int maxLength = writeBuffer.limit() - CUSTOM_HEADER;
                writeBuffer.position(CUSTOM_HEADER);

                ByteBuffer compressedBuffer = retrieveByteBuffer(this.options.networkBufferSize());
                compressedBuffer.position(CUSTOM_HEADER);
                CompressingUtils.compress(writeBuffer, compressedBuffer);
                compressedBuffer.flip();

                int limit = compressedBuffer.limit();
                compressedBuffer.put(0, COMPRESSED_BATCH_OF_EVENTS);
                compressedBuffer.putInt(1, limit);
                compressedBuffer.putInt(5, writeBuffer.getInt(5));
                compressedBuffer.putInt(9, maxLength);

                writeBuffer.clear();
                returnByteBuffer(writeBuffer);

                // maximize useful work
                lockAcquired = this.tryAcquireLock();
                if(lockAcquired){
                    // check if there is a pending one
                    ByteBuffer pendingBuffer = this.pendingWritesBuffer.poll();
                    if(pendingBuffer != null) {
                        this.channel.write(pendingBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, pendingBuffer, this.writeCompletionHandler);
                        this.pendingWritesBuffer.addLast(compressedBuffer);
                    } else {
                        this.channel.write(compressedBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, compressedBuffer, this.writeCompletionHandler);
                    }
                } else {
                    this.pendingWritesBuffer.addLast(compressedBuffer);
                }
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                remaining = 0;
                if(lockAcquired) {
                    this.releaseLock();
                }
            }
        }
        this.drained.clear();
    }

}