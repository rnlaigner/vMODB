package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;

import java.nio.ByteBuffer;

public interface IVmsEventDeliverer {

    /**
     * To connect to consumer VMSs
     *
     * Keep track of what connections
     *
     * Maybe the up class must handle this? What is the cut between the
     *
     *
     * The connection can be made part of the send. the cut is more clear, since up class will
     * check if a connection is already established
     *
     */
    // CompletableFuture<?> connect(NetworkNode node);

    /**
     * These can be buffered.
     *
     * Send an event to a consumer VMS
     *
     * @param buffer
     * @param target
     */
    void sendEvent(ByteBuffer buffer, VmsNode target);

    // CompletionStage<?> onMessage(ByteBuffer message);

}
