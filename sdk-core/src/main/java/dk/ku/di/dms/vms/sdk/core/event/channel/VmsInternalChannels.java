package dk.ku.di.dms.vms.sdk.core.event.channel;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *   This class has the objective to decouple completely the
 *   payload handler (responsible for receiving external events)
 *   and the vms executor (responsible for collecting these events
 *   and reasoning about their schedule).
 *   Particularly, this class contains a repository of events
 *   that are ready for processing, either in case of
 *   events triggering data operations and results of operations.
 *
 *   This class is inspired by the Proxy pattern:
 *   https://en.wikipedia.org/wiki/Proxy_pattern
 *
 *   TODO to transfer data from a h2 to another, we can modify the script command to
 *          generate the sql query we want and then we transfer the file created
 *
 *   FIXME the concept of a channel (in the context of coroutines) may help in the future
 *         https://kotlinlang.org/docs/channels.html
 *
 * Provide communication channel for threads.
 * Must replace the vms internal channels class
 * It is just the same instance along the program execution, can be static
 */
public final class VmsInternalChannels implements IVmsInternalChannels {

    public static VmsInternalChannels getInstance(){
        return INSTANCE;
    }

    private static final VmsInternalChannels INSTANCE;

    private static final BlockingQueue<TransactionEvent.Payload> transactionInputQueue;

    private static final BlockingQueue<OutboundEventResult> transactionOutputQueue;

    private static final BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue;

    private static final BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue;

    static {
        INSTANCE = new VmsInternalChannels();

        /* transaction **/
        transactionInputQueue = new LinkedBlockingQueue<>();
        transactionOutputQueue = new LinkedBlockingQueue<>();

        /* abort **/
        transactionAbortInputQueue = new LinkedBlockingQueue<>();
        transactionAbortOutputQueue = new LinkedBlockingQueue<>();

    }

    @Override
    public BlockingQueue<TransactionEvent.Payload> transactionInputQueue() {
        return transactionInputQueue;
    }

    @Override
    public BlockingQueue<OutboundEventResult> transactionOutputQueue() {
        return transactionOutputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue() {
        return transactionAbortInputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue() {
        return transactionAbortOutputQueue;
    }

}
