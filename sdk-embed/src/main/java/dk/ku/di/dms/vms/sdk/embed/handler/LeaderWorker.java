package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.ProducerWorker;
import dk.ku.di.dms.vms.web_common.channel.IChannel;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.System.Logger.Level.*;
import static java.lang.Thread.sleep;

/**
 * This class is responsible for all writes to the leader.
 * For now the methods are not inserting the same message again in the queue because
 * still not sure how leader is logging state after a crash
 * If so, may need to reinsert to continue the protocol from the same point
 */
final class LeaderWorker extends ProducerWorker {

    private static final System.Logger LOGGER = System.getLogger(LeaderWorker.class.getName());

    private final ServerNode leader;

    private final IChannel channel;

    private final Queue<Object> leaderWorkerQueue;

    private final VmsNode vmsNode;

    public LeaderWorker(VmsNode vmsNode,
                        ServerNode leader,
                        IChannel channel){
        super("nnn", new IdentifiableNode(), null, false);
        this.vmsNode = vmsNode;
        this.leader = leader;
        this.channel = channel;
        this.leaderWorkerQueue = new ConcurrentLinkedQueue<>();
    }

    private static final int MAX_TIMEOUT = 500;

    @Override
    @SuppressWarnings("BusyWait")
    public void run() {
        LOGGER.log(INFO, this.vmsNode.identifier+": Leader worker started!");
        int pollTimeout = 1;
        Object message = null;
        while (this.isRunning()){
            try {
                message = this.leaderWorkerQueue.poll();
                if (message == null) {
                    pollTimeout = Math.min(pollTimeout * 2, MAX_TIMEOUT);
                    sleep(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                LOGGER.log(DEBUG, this.vmsNode.identifier+": Leader worker will send message type: "+ message.getClass().getName());
                this.sendMessage(message);
            } catch (Exception e) {
                LOGGER.log(ERROR, this.vmsNode.identifier+": Error on taking message from worker queue: "+e.getCause().getMessage());
//                if(message != null){
//                    this.queueMessage(message);
//                }
            }
        }
    }

    void sendMessage(Object message) {
        switch (message) {
            case BatchComplete.Payload o -> this.sendBatchComplete(o);
            case BatchCommitAck.Payload o -> this.sendBatchCommitAck(o);
            case TransactionAbort.Payload o -> this.sendTransactionAbort(o);
            case TransactionEvent.PayloadRaw o -> this.sendEvent(o);
            default -> LOGGER.log(WARNING, this.vmsNode.identifier +
                    ": Leader worker do not recognize message type: " + message.getClass().getName());
        }
    }

    private void write(ByteBuffer writeBuffer) {
        try {
            writeBuffer.flip();
            this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
        } catch (Exception e){
            LOGGER.log(ERROR, this.vmsNode.identifier+": Error on writing message to Leader\n"+e.getCause().getMessage(), e);
            e.printStackTrace(System.out);
            // this.queueMessage(message);
            if(!this.channel.isOpen()) {
                this.leader.off();
                this.stop();
            }
        }
    }

    /**
     * No fault tolerance implemented. Once the events are submitted, they get lost and can
     * no longer be submitted to the leader.
     * In a later moment, to support crashes in the leader, we can create control messages
     * for acknowledging batch reception. This way, we could hold batches in memory until
     * the acknowledgment arrives
     */
    private void sendEvent(TransactionEvent.PayloadRaw payload) {
        this.acquireLock();
        int size = MemoryUtils.nextPowerOfTwo(payload.totalSize());
        ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer(size);
        TransactionEvent.write( writeBuffer, payload );
        this.write(writeBuffer);
    }

    private void sendBatchComplete(BatchComplete.Payload payload) {
        this.acquireLock();
        ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer(1024);
        BatchComplete.write( writeBuffer, payload );
        this.write(writeBuffer);
    }

    private void sendBatchCommitAck(BatchCommitAck.Payload payload) {
        this.acquireLock();
        ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer(1024);
        BatchCommitAck.write( writeBuffer, payload );
        this.write(writeBuffer);
    }

    private void sendTransactionAbort(TransactionAbort.Payload payload) {
        this.acquireLock();
        ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer(1024);
        TransactionAbort.write( writeBuffer, payload );
        this.write(writeBuffer);
    }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            if(byteBuffer.hasRemaining()){
                channel.write(byteBuffer, byteBuffer, this);
            } else {
                releaseLock();
                returnByteBuffer(byteBuffer);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            releaseLock();
            returnByteBuffer(byteBuffer);
        }
    }

}