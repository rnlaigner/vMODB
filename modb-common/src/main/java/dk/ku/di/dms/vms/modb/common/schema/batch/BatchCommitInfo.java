package dk.ku.di.dms.vms.modb.common.schema.batch;

import dk.ku.di.dms.vms.modb.common.schema.Constants;

import java.nio.ByteBuffer;

/**
 * Payload that carries batch-commit information.
 * It may come appended with the batch of events
 * (as the last event) or may come alone.
 * Only sent to terminal nodes in a batch.
 */
public final class BatchCommitInfo {

    // message type + 2 longs + terminal bit
    public static final int size = 1 + (3 * Long.BYTES);

    public static void write(ByteBuffer buffer, long batch, long lastTidOfBatch, long previousBatch){
        buffer.put(Constants.BATCH_COMMIT_INFO);
        buffer.putLong( batch );
        buffer.putLong( lastTidOfBatch );
        buffer.putLong( previousBatch );
    }

    public static void write(ByteBuffer buffer, BatchCommitInfo.Payload payload){
        buffer.put(Constants.BATCH_COMMIT_INFO);
        buffer.putLong( payload.batch );
        buffer.putLong( payload.lastTidOfBatch );
        buffer.putLong( payload.previousBatch );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        long lastTidOfBatch = buffer.getLong();
        long previousBatch = buffer.getLong();
        return new Payload(batch, lastTidOfBatch, previousBatch);
    }

    public static Payload of(long batch, long lastTidOfBatch, long previousBatch){
        return new Payload(batch, lastTidOfBatch, previousBatch);
    }

    public record Payload(
            long batch, long lastTidOfBatch, long previousBatch
    ){}

}
