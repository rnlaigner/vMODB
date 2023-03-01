package dk.ku.di.dms.vms.modb.common.schema.batch;

import dk.ku.di.dms.vms.modb.common.schema.Constants;

import java.nio.ByteBuffer;

/**
 * A batch-abort request payload
 * This only happens in case a new leader is elected while the previous leader has crashed in the middle of a batch
 * To avoid the overhead of replicating the transaction inputs on each sending, it is simpler to discard the entire batch
 * and restart with a new one
 */
public final class BatchAbortRequest {

    // send the last tid (corresponding to the vms) and batch id
    public static void write(ByteBuffer buffer, long batch){
        buffer.put(Constants.BATCH_ABORT_REQUEST);
        buffer.putLong( batch );
    }

    public static Payload read(ByteBuffer buffer){
        long batch = buffer.getLong();
        return new Payload(batch);
    }

    public record Payload (long batch){}

}