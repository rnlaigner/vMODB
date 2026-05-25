package dk.ku.di.dms.vms.modb.common.schema.network.transaction;

import dk.ku.di.dms.vms.modb.common.schema.network.Constants;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *  The actual payload of what is sent to the VMSs
 */
public final class TransactionEvent {

    // this payload
    // message type | tid | batch | size | event name | size | payload | size | precedence map
    private static final int FIXED_LENGTH = (2 * Long.BYTES) + (3 *  Integer.BYTES);

    public static void write(ByteBuffer buffer, PayloadRaw payload) {
        buffer.put(Constants.EVENT);
        // since the original {@PayloadRaw} goes into a batch,
        // it does not take into account the event type and the size of the payload correctly
        // for individual events, both must be included in the total size
        // jump one integer
        buffer.position(1 + Integer.BYTES);
        writeWithinBatch(buffer, payload);
        int position = buffer.position();
        buffer.putInt(1, position);
        buffer.position(position);
    }

    public static void writeWithinBatch(ByteBuffer buffer, PayloadRaw payload) {
        buffer.putLong( payload.tid );
        buffer.putLong( payload.batch );
        buffer.putInt( payload.event.length );
        buffer.put( payload.event );
        buffer.putInt( payload.payload.length );
        buffer.put( payload.payload );
        buffer.putInt( payload.precedenceMap.length );
        buffer.put( payload.precedenceMap );
    }

    public static Payload read(ByteBuffer buffer) {
        long tid = buffer.getLong();
        long batch = buffer.getLong();
        int eventSize = buffer.getInt();
        String event = ByteUtils.extractStringFromByteBuffer( buffer, eventSize );
        int payloadSize = buffer.getInt();
        String payload = ByteUtils.extractStringFromByteBuffer( buffer, payloadSize );
        int precedenceSize = buffer.getInt();
        String precedenceMap = ByteUtils.extractStringFromByteBuffer( buffer, precedenceSize );
        return new Payload(tid, batch, event, payload, precedenceMap);
    }

    /**
     * This is the base class for representing the data transferred across the framework and the sidecar
     * It serves both for input and output
     * Why total size? to know the size beforehand, before inserting into the byte buffer
     * otherwise would need further controls...
     */
    public record PayloadRaw(
            long tid, long batch, byte[] event, byte[] payload, byte[] precedenceMap, int totalSize
    ){
        @Override
        public String toString() {
            return "{"
                    + "\"tid\":" + tid
                    + ",\"batch\":" + batch
                    + "}";
        }
    }

    public record Payload(
            long tid, long batch, String event, String payload, String precedenceMap
    ){
        @Override
        public String toString() {
            return "{"
                    + "\"batch\":" + batch
                    + ",\"tid\":" + tid
                    + ",\"event\":\"" + event + "\""
                    + ",\"payload\":" + payload
                    + ",\"precedenceMap\":" + precedenceMap
                    + "}";
        }
    }

    /**
     * <a href="https://www.quora.com/How-many-bytes-can-a-string-hold">Considering UTF-8</a>
     */
    public static PayloadRaw of(long tid, long batch, String event, String payload, String precedenceMap) {
        byte[] eventBytes = event.getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        byte[] precedenceMapBytes = precedenceMap.getBytes(StandardCharsets.UTF_8);
        return new PayloadRaw(tid, batch, eventBytes, payloadBytes, precedenceMapBytes,
                FIXED_LENGTH + eventBytes.length + payloadBytes.length + precedenceMapBytes.length);
    }

}