package dk.ku.di.dms.vms.sdk.core.event.handler;


import dk.ku.di.dms.vms.modb.common.schema.node.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionEvent;

/**
 * Interface that makes clear what the VMS event handler must do
 * The class implementing this interface is oblivious of how the
 * messages are unpacked, sent, and received over the network
 * It only deals with logic involving the VMS transaction execution
 * MAYBE: Make the handler async like this project:
 * <a href="https://github.com/ebarlas/microhttp">...</a>
 * Inspired by
 * {@link java.net.http.WebSocket}
 */
public interface IVmsEventHandler {

    /**
     * Can come from both the leader and (producer) VMSs
     * @param transactionEventPayload
     */
    void onTransactionInputEvent(TransactionEvent.Payload transactionEventPayload);

    void onBatchCommitRequest(BatchCommitCommand.Payload batchCommitReq);

    void onBatchAbortRequest(BatchAbortRequest.Payload batchAbortReq);

    void onTransactionAbort(TransactionAbort.Payload transactionAbortReq);

    /**
     * Receives a presentation payload from the leader
     * @param node presentation
     */
    void onLeaderConnection(NetworkNode node);

    /**
     * Receives a presentation payload from a (producer) VMS
     * @param vms vms
     */
    void onVmsConnection(VmsNode vms);

}