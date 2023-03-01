package dk.ku.di.dms.vms.modb.common.schema.batch.follower;

import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;
import dk.ku.di.dms.vms.modb.common.schema.Constants;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A batch-commit replication request payload
 * Used to replicate across followers
 */
public final class BatchReplication {

    public static void write(ByteBuffer buffer, long batch, String vmsTidMap){
        buffer.put(Constants.BATCH_REPLICATION);
        buffer.putLong( batch );
        buffer.putInt( vmsTidMap.length() );
        buffer.put( vmsTidMap.getBytes(StandardCharsets.UTF_8) );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String map = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new Payload(batch,map);
    }

    public record Payload(
        long batch, String vmsTidMap
    ){}

}
