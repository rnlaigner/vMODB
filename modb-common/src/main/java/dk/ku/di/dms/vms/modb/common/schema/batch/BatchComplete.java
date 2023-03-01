package dk.ku.di.dms.vms.modb.common.schema.batch;

import dk.ku.di.dms.vms.modb.common.schema.Constants;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A prepare request payload
 * Used to inform coordinator
 * Using name because network address can change within a batch
 */
public final class BatchComplete {

    public static void write(ByteBuffer buffer, long batch, VmsNode vmsIdentifier){
        buffer.put(Constants.BATCH_COMPLETE);
        buffer.putLong( batch );
        buffer.putInt( vmsIdentifier.name.length() );
        buffer.put( vmsIdentifier.name.getBytes(StandardCharsets.UTF_8) );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        int size = buffer.getInt();
        String vms = ByteUtils.extractStringFromByteBuffer(buffer, size);
        return new Payload(batch, vms);
    }

    public static void write(ByteBuffer buffer, Payload payload) {
        buffer.put(Constants.BATCH_COMPLETE);
        buffer.putLong(payload.batch() );
        buffer.putInt( payload.vms().length() );
        buffer.put( payload.vms().getBytes(StandardCharsets.UTF_8) );
    }

    public static Payload of(long batch, String vms){
        return new Payload(batch, vms);
    }

    public record Payload(
        long batch, String vms
    ){}

}
