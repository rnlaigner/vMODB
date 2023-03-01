package dk.ku.di.dms.vms.sdk.core.scheduler;

/**
 * While the {@link dk.ku.di.dms.vms.sdk.core.scheduler.tracking.IVmsTransactionTrackingContext}
 * tracks the underlying tasks of a given transaction, this class tracks the transaction offset (i.e. TID)
 * to allow the scheduler to progress to other TIDs and batches.
 */
class OffsetTracker {

    enum OffsetStatus {
        NEW,
        SCHEDULED, // it means it is the current offset
        FINISHED_SUCCESSFULLY,
        FINISHED_WITH_ERROR
    }

    private final long tid;

    // how many tasks from this tid remain to execute?
    private int remainingFinishedTasks;

    private OffsetStatus status;

    public OffsetTracker(long tid, int numberOfTasks) {
        this.tid = tid;
        //this.remainingReadyTasks = remainingReadyTasks;
        this.remainingFinishedTasks = numberOfTasks;
        this.status = OffsetStatus.NEW;
    }

    private void moveToDoneState(){
        assert this.status == OffsetStatus.SCHEDULED;
        this.status = OffsetStatus.FINISHED_SUCCESSFULLY;
    }

    private void moveToErrorState() {
        assert this.status == OffsetStatus.SCHEDULED;
        this.status = OffsetStatus.FINISHED_WITH_ERROR;
    }

    void signalScheduled() {
        assert this.status == OffsetStatus.NEW;
        this.status = OffsetStatus.SCHEDULED;
    }

    public void signalTaskFinished(){
        assert this.remainingFinishedTasks > 0;
        this.remainingFinishedTasks--;
        if(this.remainingFinishedTasks == 0) this.moveToDoneState();
    }

    public void signalError(){
        this.moveToErrorState();
    }

    public long tid() {
        return this.tid;
    }

    public OffsetStatus status(){
        return this.status;
    }
}
