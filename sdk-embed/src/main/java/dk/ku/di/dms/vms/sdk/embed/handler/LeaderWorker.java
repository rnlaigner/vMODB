package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.modb.common.schema.control.Presentation.YES;
import static dk.ku.di.dms.vms.sdk.embed.handler.EmbeddedVmsEventHandler.DEFAULT_DELAY_FOR_BATCH_SEND;

/**
 * This class is responsible for all writes to the leader.
 * For now the methods are not inserting the same message again in the queue because
 * still not sure how leader is logging state after a crash
 * If so, may need to reinsert to continue the protocol from the same point
 */
final class LeaderWorker extends StoppableRunnable {

    private final static Logger logger = LoggerFactory.getLogger(LeaderWorker.class);

    // defined after presentation
    private ServerIdentifier leader;

    private final VmsNode vmsNode;
    
    private final AsynchronousSocketChannel channel;

    private final ByteBuffer writeBuffer;

    private final BlockingDeque<TransactionEvent.Payload> eventsToSendToLeader;

    private final BlockingQueue<Message> leaderWorkerQueue;

    private final Set<String> queuesLeaderSubscribesTo;

    private final IVmsSerdesProxy serdesProxy;

    // a leader worker starts with a leader presentation message
    private HandshakeState handshakeState = HandshakeState.PRESENTATION_RECEIVED;

    private enum HandshakeState {
        PRESENTATION_RECEIVED,
        PRESENTATION_PROCESSED,
        PRESENTATION_SENT
    }

    /**
     * Messages that correspond to operations that can only be
     * spawned when a set of asynchronous messages arrive
     */
    enum Command {
        SEND_BATCH_COMPLETE, // inform batch completion
        SEND_BATCH_COMMIT_ACK, // inform commit completed
        SEND_TRANSACTION_ABORT // inform that a tid aborted
    }

    record Message(Command type, Object object){

        public BatchCommitAck.Payload asBatchCommitAck() {
            return (BatchCommitAck.Payload)object;
        }

        public BatchComplete.Payload asBatchComplete(){
            return (BatchComplete.Payload)object;
        }

        public TransactionAbort.Payload asTransactionAbort(){
            return (TransactionAbort.Payload)object;
        }

    }

    public LeaderWorker(
            VmsNode vmsNode,
            AsynchronousSocketChannel channel,
            ByteBuffer writeBuffer,
            BlockingDeque<TransactionEvent.Payload> eventsToSendToLeader,
            Set<String> queuesLeaderSubscribesTo,
            BlockingQueue<Message> leaderWorkerQueue,
            IVmsSerdesProxy serdesProxy){
        this.vmsNode = vmsNode;
        this.channel = channel;
        this.writeBuffer = writeBuffer;
        this.eventsToSendToLeader = eventsToSendToLeader;
        this.queuesLeaderSubscribesTo = queuesLeaderSubscribesTo;
        this.leaderWorkerQueue = leaderWorkerQueue;
        this.serdesProxy = serdesProxy;
    }

    @Override
    public void run() {

        logger.info("Leader worker started!");

        // complete handshake first
        Future<Integer> writePresentationRes = this.processLeaderPresentation();
        try {
            writePresentationRes.get();
            this.writeBuffer.clear();
            logger.info("Leader presentation processed");
        } catch (InterruptedException | ExecutionException e) {
            logger.warn("Could not finish handshake protocol. Turning leader worker off.");
            this.stop();
            this.writeBuffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.writeBuffer);
            return;
        }

        while (this.isRunning()){
            try {

                TimeUnit.of(ChronoUnit.MILLIS).sleep(DEFAULT_DELAY_FOR_BATCH_SEND);
                this.batchEventsToLeader();

                // drain the queue
                while(true) {
                    Message msg = this.leaderWorkerQueue.poll();
                    if (msg == null) break;
                    switch (msg.type()) {
                        case SEND_BATCH_COMPLETE -> this.sendBatchComplete(msg.asBatchComplete());
                        case SEND_BATCH_COMMIT_ACK -> this.sendBatchCommitAck(msg.asBatchCommitAck());
                        case SEND_TRANSACTION_ABORT -> this.sendTransactionAbort(msg.asTransactionAbort());
                    }
                }

            } catch (InterruptedException e) {
                logger.warn("Error on taking message from worker queue: "+e.getMessage());
            }
        }
    }

    public Future<Integer> processLeaderPresentation() {

        boolean includeMetadata = this.writeBuffer.get() == YES;

        // leader has disconnected, or new leader
        leader = Presentation.readServer(this.writeBuffer);

        // read queues leader is interested
        boolean hasQueuesToSubscribe = this.writeBuffer.get() == YES;
        if(hasQueuesToSubscribe){
            var receivedQueues = Presentation.readQueuesToSubscribeTo(this.writeBuffer, serdesProxy);
            this.queuesLeaderSubscribesTo.addAll(receivedQueues);
        }

        // only connects to all VMSs on first leader connection
        leader.on();
        this.writeBuffer.clear();

        if(includeMetadata) {
            String vmsDataSchemaStr = serdesProxy.serializeDataSchema(vmsNode.tableSchema);
            String vmsInputEventSchemaStr = serdesProxy.serializeEventSchema(vmsNode.inputEventSchema);
            String vmsOutputEventSchemaStr = serdesProxy.serializeEventSchema(vmsNode.outputEventSchema);
            Presentation.writeVms(this.writeBuffer, vmsNode, vmsNode.name, vmsNode.batch, vmsNode.lastTidOfBatch, vmsNode.previousBatch, vmsDataSchemaStr, vmsInputEventSchemaStr, vmsOutputEventSchemaStr);
            // the protocol requires the leader to wait for the vms metadata in order to start sending vms messages
        } else {
            Presentation.writeVms(this.writeBuffer, vmsNode, vmsNode.name, vmsNode.batch, vmsNode.lastTidOfBatch, vmsNode.previousBatch);
        }

        this.writeBuffer.flip();
        return this.channel.write( this.writeBuffer );

    }

    private void write() {
        this.writeBuffer.flip();
        try {
            this.channel.write(this.writeBuffer).get();
        } catch (InterruptedException | ExecutionException e){
            this.writeBuffer.clear();
            if(!this.channel.isOpen()) {
                this.leader.off();
                this.stop();
            }
        } finally {
            this.writeBuffer.clear();
        }
    }

    private final List<TransactionEvent.Payload> events = new ArrayList<>();

    /**
     * No fault tolerance implemented. Once the events are submitted, they get lost and can
     * no longer be submitted to the leader.
     * In a later moment, to support crashes in the leader, we can create control messages
     * for acknowledging batch reception. This way, we could hold batches in memory until
     * the acknowledgment arrives
     */
    private void batchEventsToLeader() {

        this.eventsToSendToLeader.drainTo(this.events);

        int remaining = this.events.size();

        while(remaining > 0){
            remaining = BatchUtils.assembleBatchPayload( remaining, this.events, this.writeBuffer);
            try {
                this.writeBuffer.flip();
                this.channel.write(this.writeBuffer).get();
            } catch (InterruptedException | ExecutionException e) {

                // return events to the deque
                for(TransactionEvent.Payload event : this.events) {
                    this.eventsToSendToLeader.offerFirst(event);
                }

                if(!this.channel.isOpen()){
                    this.leader.off();
                    remaining = 0; // force exit loop
                }

            } finally {
                this.writeBuffer.clear();
            }
        }

        this.events.clear();

    }

    private void sendBatchComplete(BatchComplete.Payload payload) {
        BatchComplete.write( this.writeBuffer, payload );
        this.write();
    }

    private void sendBatchCommitAck(BatchCommitAck.Payload payload) {
        BatchCommitAck.write( this.writeBuffer, payload );
        this.write();
    }

    private void sendTransactionAbort(TransactionAbort.Payload payload) {
        TransactionAbort.write( this.writeBuffer, payload );
        this.write();
    }

}