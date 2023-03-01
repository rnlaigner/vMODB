package dk.ku.di.dms.vms.modb.common.schema.batch.follower;

import dk.ku.di.dms.vms.modb.common.schema.Constants;

import java.nio.ByteBuffer;

/**
 * A batch-commit replication request payload
 * Used to replicate across followers
 */
public final class BatchReplicationAck {

    // commit   ----
    // type  | batch offset  |
    private static final int headerSize = Byte.BYTES + Long.BYTES;

    public static void write(ByteBuffer buffer, long batch){
        buffer.put(Constants.BATCH_REPLICATION_ACK);
        buffer.putLong( batch );
    }

    public static BatchReplicationAckPayload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        return new BatchReplicationAckPayload(batch);
    }

    public record BatchReplicationAckPayload(
        long batch
    ){}

}
