package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

public interface IVmsWorker {

    void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw);

    void queueMessage(Object message);

}
