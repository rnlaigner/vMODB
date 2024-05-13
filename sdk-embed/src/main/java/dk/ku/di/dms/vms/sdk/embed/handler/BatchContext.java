package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;

public final class BatchContext {

    public final long batch;

    public final long previousBatch;

    public final long lastTid;

    public final int numberOfTIDsBatch;

    // if an external thread (i.e., scheduler) modifies
    // this attribute, it needs to change to volatile
    private int status;

    // whether this vms is a terminal for this batch
    public final boolean terminal;

    public static BatchContext build(BatchCommitInfo.Payload batchCommitInfo){
        return new BatchContext(batchCommitInfo.batch(),
                batchCommitInfo.lastTidOfBatch(),
                batchCommitInfo.previousBatch(),
                batchCommitInfo.numberOfTIDsBatch(),
                true);
    }

    public static BatchContext build(long batch, long lastTidOfBatch, long previousBatch, int numberOfTIDsBatch){
        return new BatchContext(batch, lastTidOfBatch, previousBatch, numberOfTIDsBatch,false);
    }

    public static BatchContext build(BatchCommitCommand.Payload batchCommitRequest) {
        return new BatchContext(batchCommitRequest.batch(), batchCommitRequest.lastTidOfBatch(),
                batchCommitRequest.previousBatch(), batchCommitRequest.numberOfTIDsBatch(), false);
    }

    private BatchContext(long batch, long lastTidOfBatch, long previousBatch, int numberOfTIDsBatch, boolean terminal) {
        this.batch = batch;
        this.lastTid = lastTidOfBatch;
        this.previousBatch = previousBatch;
        // this.status = Status.OPEN.value; // always start with 0 anyway
        this.numberOfTIDsBatch = numberOfTIDsBatch;
        this.terminal = terminal;
    }

    /**
     * A batch being completed in a VMS does not necessarily mean
     * it can commit the batch. A new leader may need to abort the
     * last batch. In this case, the (local) state must be restored to
     * last logged state.
     */
    // newly received batch
    public static final int OPEN = 0;
    // this status is set after all TIDs of the batch have been processed
    public static final int BATCH_COMPLETED = 1;
    // this status is set when the logging process starts right after the leader sends the batch commit request
    public static final int LOGGING = 2;
    // this status is set when the state is logged
    public static final int BATCH_COMMITTED = 3;

    public boolean isOpen(){
        return this.status < BATCH_COMPLETED;
    }

    public boolean isCommitted(){
        return this.status == BATCH_COMMITTED;
    }

    public void setStatus(int status){
        this.status = status;
    }

}
