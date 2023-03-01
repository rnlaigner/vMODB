package dk.ku.di.dms.vms.sdk.core.event.channel;

import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;

import java.util.concurrent.BlockingQueue;

/**
 * In a virtual microservice paradigm, internal components exchange a lot of internal events.
 * Some components require consuming from different streams (e.g. {@link VmsTransactionScheduler}).
 * Other only publish (task) and others consume from one and publish to another.
 * There are some limitations with Java interfaces. One of them is that a class cannot
 * implement the same interface twice, even though different types are used.
 * Besides, the {@link java.util.concurrent.Flow} interface requires a concrete implementation
 * and tie together the consumer and producer with the "requires" method call to receive data.
 * I believe this request nature is not fruitful here.
 * To decouple the components, this interface is used to hide the queue concrete implementations
 * and each component makes use of the queue of interest of its own work.
 * In the end, these are just explicit channels for different threads to communicate with
 * each other. Basically what Java offers for message-passing mechanism.
 *
 */
public interface IVmsInternalChannels {

    /*
     * TRANSACTIONAL EVENTS
     */

    /**
     * It represents events ready for scheduler consumption
     */
    BlockingQueue<InboundEvent> transactionInputQueue();

    /**
     *  It represents events ready for delivery
     *  The payload handler thread consumes from (and never inserts into!) this queue
     */
    BlockingQueue<VmsTransactionResult> transactionOutputQueue();

    /**
     * BATCH COMMIT, ABORT EVENTS
     */

    // this should be sent by terminal vms
    // BlockingQueue<BatchComplete.Payload> batchCompleteOutputQueue();

    // abort a specific transaction from the batch and restart state from there
    // should maintain an MV scheme to avoid rolling back to the last committed state
    BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue();

    //
    BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue();

    // used internally by terminal VMSs to log internal state
    BlockingQueue<Object> batchCommitCommandQueue();

    /*
     *  This is sent by the leader by all non-terminal VMSs involved in the last batch commit
     */
    // BlockingQueue<BatchCommitRequest.Payload> batchCommitQueue();

    // sent by the new leader
    // BlockingQueue<BatchAbortRequest.Payload> batchAbortQueue();

    // no response, al vms will definitely commit. if there is crash, they rerun the events
    // methods must be deterministic if developers want to maintain the same state as run before the crash
//    BlockingQueue<BatchCommitResponse.Payload> batchCommitResponseQueue();

    /*
     * DATA --> only for dbms-service
     */

    /*
     * A queue of requests for data
     */
    //Queue<DataRequestEvent> dataRequestQueue();

    /*
     * A map of data request responses (keyed by thread identifier)
     */
    //Map<Long, DataResponseEvent> dataResponseMap();

}
