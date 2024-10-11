package dk.ku.di.dms.vms.coordinator.vms;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

public interface IVmsWorker {

    default void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) { }

    default void queueMessage(Object message) { }

}
