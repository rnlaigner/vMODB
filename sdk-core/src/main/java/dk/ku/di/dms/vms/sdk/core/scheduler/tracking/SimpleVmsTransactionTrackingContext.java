package dk.ku.di.dms.vms.sdk.core.scheduler.tracking;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * For cases where only one task is involved
 * To avoid the overhead of creating many data structures for every new task
 * as found in a complex transaction
 */
public final class SimpleVmsTransactionTrackingContext implements IVmsTransactionTrackingContext {

    public final VmsTransactionTask task;

    public Future<VmsTransactionTaskResult> future;

    public VmsTransactionTaskResult result;

    public final Map<String, List<TransactionWrite>> updates;

    public SimpleVmsTransactionTrackingContext(VmsTransactionTask task, Map<String, List<TransactionWrite>> updates) {
        this.task = task;
        this.updates = updates;
    }

    @Override
    public SimpleVmsTransactionTrackingContext asSimple(){
        return this;
    }

}
