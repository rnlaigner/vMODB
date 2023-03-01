package dk.ku.di.dms.vms.sdk.embed.channel;

import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Can potentially be a final static class, perhaps defined in modb-common?
 * To avoid passing over object instances...
 * One less parameter, but also less mutability and testability
 */
public final class VmsEmbeddedInternalChannels implements IVmsInternalChannels {

    private final BlockingQueue<InboundEvent> transactionInputQueue;

    private final BlockingQueue<VmsTransactionResult> transactionOutputQueue;

    private final BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue;

    private final BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue;

    private final BlockingQueue<Object> batchCommitCommandQueue;

    public VmsEmbeddedInternalChannels() {

        // linked blocking queue because method size is a constant time operation

        /* transaction **/
        this.transactionInputQueue = new LinkedBlockingQueue<>();
        this.transactionOutputQueue = new LinkedBlockingQueue<>();

        /* abort **/
        this.transactionAbortInputQueue = new LinkedBlockingQueue<>();
        this.transactionAbortOutputQueue = new LinkedBlockingQueue<>();

        /* batch */
        this.batchCommitCommandQueue = new LinkedBlockingQueue<>();

    }

    @Override
    public BlockingQueue<InboundEvent> transactionInputQueue() {
        return this.transactionInputQueue;
    }

    @Override
    public BlockingQueue<VmsTransactionResult> transactionOutputQueue() {
        return this.transactionOutputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue() {
        return this.transactionAbortInputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue() {
        return this.transactionAbortOutputQueue;
    }

    @Override
    public BlockingQueue<Object> batchCommitCommandQueue(){
        return this.batchCommitCommandQueue;
    }

}
