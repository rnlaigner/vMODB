package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

public interface IVmsContainer {
    boolean queue(TransactionEvent.PayloadRaw payload);
    String identifier();
}