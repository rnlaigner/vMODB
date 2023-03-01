package dk.ku.di.dms.vms.sdk.core.scheduler.tracking;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;

import java.util.*;
import java.util.concurrent.Future;

/**
 * The context of the tasks from a transaction
 * (thus having the same TID) in a single VMS.
 * The tasks found in the lists are READY to be scheduled for execution.
 * In other words, all inputs are fulfilled.
 */
public final class ComplexVmsTransactionTrackingContext implements IVmsTransactionTrackingContext {

    private int nextTaskIdentifier;

    // the R tasks ready for execution
    public final List<VmsTransactionTask> readTasks;

    // the RW and W ready for execution
    public final Queue<VmsTransactionTask> writeTasks;

    public final List<Future<VmsTransactionTaskResult>> submittedTasks;

    public final List<VmsTransactionTaskResult> resultTasks;

    public final Map<String, List<TransactionWrite>> updates;

    public ComplexVmsTransactionTrackingContext(int numReadTasks, int numReadWriteTasks, int numWriteTasks) {
        this.nextTaskIdentifier = 1;
        this.readTasks = new ArrayList<>(numReadTasks);
        this.writeTasks = new ArrayDeque<>(numReadWriteTasks + numWriteTasks);
        int total = numReadTasks + numWriteTasks + numWriteTasks;
        this.submittedTasks = new ArrayList<>(total);
        this.resultTasks = new ArrayList<>(total);
        this.updates = new HashMap<>();
    }

    public int readAndIncrementNextTaskIdentifier(){
        return this.nextTaskIdentifier++;
    }

    @Override
    public boolean isSimple() {
        return false;
    }

    @Override
    public ComplexVmsTransactionTrackingContext asComplex(){
        return this;
    }

}
