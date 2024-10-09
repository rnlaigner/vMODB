package dk.ku.di.dms.vms.sdk.embed.channel;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.sdk.core.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class VmsEmbedInternalChannels implements IVmsInternalChannels {

    private final BlockingQueue<InboundEvent> transactionInputQueue;

    private final BlockingQueue<IVmsTransactionResult> transactionOutputQueue;

    private final Queue<TransactionAbort.Payload> transactionAbortInputQueue;

    private final Queue<TransactionAbort.Payload> transactionAbortOutputQueue;

    public VmsEmbedInternalChannels() {
        /* transaction **/
        this.transactionInputQueue = new MpscBlockingConsumerArrayQueueWrapper(1024*100);
        this.transactionOutputQueue = new LinkedBlockingQueue<>();
        /* abort **/
        this.transactionAbortInputQueue = new ConcurrentLinkedQueue<>();
        this.transactionAbortOutputQueue = new ConcurrentLinkedQueue<>();
    }

    private static final class MpscBlockingConsumerArrayQueueWrapper extends MpscBlockingConsumerArrayQueue<InboundEvent> {
        public MpscBlockingConsumerArrayQueueWrapper(int capacity) {
            super(capacity);
        }
        @Override
        public int drainTo(Collection<? super InboundEvent> c) {
            int n = capacity();
            InboundEvent e;
            while(n>0 && (e = this.poll()) != null){
                c.add(e);
                n--;
            }
            return 0;
        }
    }

    @Override
    public BlockingQueue<InboundEvent> transactionInputQueue() {
        return this.transactionInputQueue;
    }

    @Override
    public BlockingQueue<IVmsTransactionResult> transactionOutputQueue() {
        return this.transactionOutputQueue;
    }

    @Override
    public Queue<TransactionAbort.Payload> transactionAbortInputQueue() {
        return this.transactionAbortInputQueue;
    }

    @Override
    public Queue<TransactionAbort.Payload> transactionAbortOutputQueue() {
        return this.transactionAbortOutputQueue;
    }

}
