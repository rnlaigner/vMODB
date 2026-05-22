package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.IChannel;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

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
public final class ConsumerVmsWorker extends StoppableRunnable implements IVmsContainer {

    private static final System.Logger LOGGER = System.getLogger(ConsumerVmsWorker.class.getName());

    private final VmsNode me;

    private final IdentifiableNode consumerVms;
    
    private final IChannel channel;

    private final IVmsSerdesProxy serdesProxy;

    private static final Deque<ByteBuffer> WRITE_BUFFER_POOL = new ConcurrentLinkedDeque<>();

    private final VmsEventHandler.VmsHandlerOptions options;

    private final MpscBlockingConsumerArrayQueue<TransactionEvent.PayloadRaw> transactionEventQueue;

    private final List<TransactionEvent.PayloadRaw> drained = new ArrayList<>(1024 * 10);

    private State state;

    protected enum State { NEW, CONNECTED, PRESENTATION_SENT, READY }

    public static ConsumerVmsWorker build(
                                  VmsNode me,
                                  IdentifiableNode consumerVms,
                                  Supplier<IChannel> channelSupplier,
                                  VmsEventHandler.VmsHandlerOptions options,
                                  IVmsSerdesProxy serdesProxy) {
        return new ConsumerVmsWorker(me, consumerVms,
                channelSupplier.get(), options, serdesProxy);
    }

    private ConsumerVmsWorker(VmsNode me,
                             IdentifiableNode consumerVms,
                             IChannel channel,
                             VmsEventHandler.VmsHandlerOptions options,
                             IVmsSerdesProxy serdesProxy) {
        this.me = me;
        this.consumerVms = consumerVms;
        this.channel = channel;
        this.serdesProxy = serdesProxy;
        this.options = options;
        this.transactionEventQueue = new MpscBlockingConsumerArrayQueue<>(1024 * 1500);
        this.state = NEW;
    }

    @Override
    public void run() {
        LOGGER.log(INFO, this.me.identifier+ ": Starting worker for consumer VMS: "+this.consumerVms.identifier);
        if(!this.connect()) {
            LOGGER.log(WARNING, this.me.identifier+ ": Finishing prematurely worker for consumer VMS "+this.consumerVms.identifier+" because connection failed");
            return;
        }
        this.eventLoop();
        LOGGER.log(INFO, this.me.identifier+ ": Finishing worker for consumer VMS: "+this.consumerVms.identifier);
    }

    /**
     * Responsible for making sure the handshake protocol is
     * successfully performed with a consumer VMS
     */
    private boolean connect() {
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());
        try{
            NetworkUtils.configure(this.channel, options.soBufferSize());
            try {
                this.channel.connect(this.consumerVms.asInetSocketAddress()).get();
            } catch (Exception e){
                // fallback to check if consumer is localhost
                LOGGER.log(WARNING,this.me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" cannot be reached. Falling back to localhost attempt");
                var localHostSocketAddress = new InetSocketAddress("localhost",  this.consumerVms.port);
                this.channel.connect(localHostSocketAddress).get();
            }
            this.state = CONNECTED;
            LOGGER.log(DEBUG,this.me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);
            String dataSchema = this.serdesProxy.serializeDataSchema(this.me.dataSchema);
            String inputEventSchema = this.serdesProxy.serializeEventSchema(this.me.inputEventSchema);
            String outputEventSchema = this.serdesProxy.serializeEventSchema(this.me.outputEventSchema);
            buffer.clear();
            Presentation.writeVms( buffer, this.me, this.me.identifier, this.me.batch, 0, this.me.previousBatch, dataSchema, inputEventSchema, outputEventSchema );
            buffer.flip();
            this.channel.write(buffer).get();
            this.state = PRESENTATION_SENT;
            LOGGER.log(DEBUG,me.identifier+ ": The node "+ this.consumerVms.host+" "+ this.consumerVms.port+" status = "+this.state);
            this.returnByteBuffer(buffer);
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

    private void eventLoop() {
        while(this.isRunning()){
            try {
                this.drained.add(this.transactionEventQueue.take());
                this.transactionEventQueue.drain(this.drained::add);
                if(this.drained.size() == 1) {
                    this.sendEventBlocking(this.drained.removeFirst());
                } else {
                    this.sendBatchOfEventsBlocking();
                }
            } catch (Exception e) {
                LOGGER.log(ERROR, this.me.identifier+ ": Error captured in event loop (no logging) \n"+e);
            }
        }
    }

    private void sendEventBlocking(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        try {
            TransactionEvent.write( writeBuffer, payload );
            writeBuffer.flip();
            do {
                this.channel.write(writeBuffer).get();
            } while (writeBuffer.hasRemaining());
        } catch (Exception e){
            if(this.isRunning()) {
                LOGGER.log(ERROR, "Error caught on sending single event: " + e);
                this.transactionEventQueue.offer(payload);
            }
            writeBuffer.clear();
        }
        finally {
            this.returnByteBuffer(writeBuffer);
        }
    }

    private void sendBatchOfEventsBlocking() {
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0){
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchPayload(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, this.me.identifier+ ": Submitting ["+(count - remaining)+"] event(s) to "+this.consumerVms.identifier);
                count = remaining;
                do {
                    this.channel.write(writeBuffer).get();
                } while(writeBuffer.hasRemaining());
                this.returnByteBuffer(writeBuffer);
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
            this.returnByteBuffer(writeBuffer);
        }
    }

    private ByteBuffer retrieveByteBuffer(){
        ByteBuffer bb = WRITE_BUFFER_POOL.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(this.options.networkBufferSize());
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        WRITE_BUFFER_POOL.add(bb);
    }

    @Override
    public void queue(TransactionEvent.PayloadRaw eventPayload){
        if(!this.transactionEventQueue.offer(eventPayload)){
            System.out.println(me.identifier +": cannot add event in the input queue");
        }
    }

    @Override
    public String identifier() {
        return this.consumerVms.identifier;
    }

    @Override
    public void stop(){
        super.stop();
        this.channel.close();
    }

}