package dk.ku.di.dms.vms.modb.common.schema.batch;

import dk.ku.di.dms.vms.modb.common.schema.Constants;

import java.nio.ByteBuffer;

/**
 * A batch-commit request payload.
 * Sent to non-terminal nodes in a batch.
 * Sent after all terminal nodes have replied a BATCH_COMPLETE event
 */
public final class BatchCommitCommand {

    public static final int size = 1 + (3 * Long.BYTES);

    // send the last tid (corresponding to the vms) and batch id
    public static void write(ByteBuffer buffer, long batch, long lastTidOfBatch, long previousBatch){
        buffer.put(Constants.BATCH_COMMIT_COMMAND);
        buffer.putLong( batch );
        buffer.putLong( lastTidOfBatch );
        buffer.putLong(previousBatch);
    }

    public static void write(ByteBuffer buffer, BatchCommitCommand.Payload payload){
        buffer.put(Constants.BATCH_COMMIT_COMMAND);
        buffer.putLong(payload.batch() );
        buffer.putLong(payload.lastTidOfBatch() );
        buffer.putLong(payload.previousBatch());
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        long lastTidOfBatch = buffer.getLong();
        long previousBatch = buffer.getLong();
        return new Payload(batch, lastTidOfBatch, previousBatch);
    }

    public record Payload(
            long batch, long lastTidOfBatch, long previousBatch
    ){}

}
