package dk.ku.di.dms.vms.modb.common.schema.batch;

import dk.ku.di.dms.vms.modb.common.schema.Constants;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A batch-commit response payload
 * The concept of aborting a batch-commit does not exist.
 * For avoiding messages (optimizing the protocol), this message
 * can be dropped (ignored) or simply marshalled with another.
 */
public final class BatchCommitAck {

    public static void write(ByteBuffer buffer, long batch, VmsNode vmsIdentifier){
        buffer.put(Constants.BATCH_COMMIT_ACK);
        buffer.putLong( batch );
        byte[] nameBytes = vmsIdentifier.name.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( nameBytes.length );
        buffer.put( nameBytes );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new Payload(batch, vms);
    }

    public static void write(ByteBuffer buffer, Payload payload) {
        buffer.put(Constants.BATCH_COMMIT_ACK);
        buffer.putLong(payload.batch() );
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static BatchCommitAck.Payload of(long batch, String vms){
        return new BatchCommitAck.Payload(batch, vms);
    }

    // a leader cannot issue new events (and batches of course) without receiving batch ACKs from all vms involved
    // so no need for further information in the payload
    public record Payload(
            long batch, String vms
    ) {}

}
