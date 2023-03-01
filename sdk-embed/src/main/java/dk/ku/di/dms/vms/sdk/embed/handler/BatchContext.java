package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitInfo;

public class BatchContext {

    public final long batch;

    public final long previousBatch;

    public final long lastTid;

    private int status;

    // whether this vms is a terminal for this batch
    public final boolean terminal;

    // for terminal nodes in a batch
    public static BatchContext build(BatchCommitInfo.Payload batchCommitInfo){
        return new BatchContext(batchCommitInfo.batch(), batchCommitInfo.lastTidOfBatch(), batchCommitInfo.previousBatch(), true);
    }

    public static BatchContext build(long batch, long lastTidOfBatch, long previousBatch){
        return new BatchContext(batch, lastTidOfBatch, previousBatch, false);
    }

    // for non-terminal nodes in a batch
    public static BatchContext build(BatchCommitCommand.Payload batchCommitRequest) {
        return new BatchContext(batchCommitRequest.batch(), batchCommitRequest.lastTidOfBatch(),
                batchCommitRequest.previousBatch(),false);
    }

    private BatchContext(long batch, long lastTidOfBatch, long previousBatch, boolean terminal) {
        this.batch = batch;
        this.lastTid = lastTidOfBatch;
        this.previousBatch = previousBatch;
        this.status = Status.OPEN.value;
        this.terminal = terminal;
    }

    /**
     * A batch being completed in a VMS does not necessarily mean
     * it can commit the batch. A new leader may need to abort the
     * last batch. In this case, the (local) state must be restored to
     * last logged state.
     */
    public enum Status {
        // newly received batch
        OPEN(0),
        // this status is set after all TIDs of the batch have been processed
        BATCH_EXECUTION_COMPLETED(1),
        // this status is set when the logging process starts right after the leader sends the batch commit request
        LOGGING(2),
        // this status is set when the state is logged
        BATCH_COMMITTED(3);

        public final int value;
        Status(int value) {
            this.value = value;
        }
    }

    public boolean isOpen(){
        return this.status < Status.BATCH_EXECUTION_COMPLETED.value;
    }

    public boolean isCommitted(){
        return this.status == Status.BATCH_COMMITTED.value;
    }

    public void setStatus(Status status){
        this.status = status.value;
    }

}
