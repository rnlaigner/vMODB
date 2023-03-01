package dk.ku.di.dms.vms.modb.common.schema.transaction;

import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;
import dk.ku.di.dms.vms.modb.common.schema.Constants;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *  The actual payload of what is sent to the VMSs
 */
public final class TransactionEvent {

    // this payload
    // message type | tid | batch | size | event name | size | payload
    private static final int fixedLength = 1 + (2 * Long.BYTES) + (3 *  Integer.BYTES);

    public static void write(ByteBuffer buffer, Payload payload){
        buffer.put( Constants.TRANSACTION_EVENT);
        buffer.putLong( payload.tid );
        buffer.putLong( payload.batch );
        byte[] eventBytes = payload.event.getBytes();
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );
        byte[] payloadBytes = payload.payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );
        if(payload.precedenceMap != null) {
            byte[] precedenceBytes = payload.precedenceMap.getBytes();
            buffer.putInt(precedenceBytes.length);
            buffer.put(precedenceBytes);
        }
        if(payload.updates() == null) return;
        if(!payload.updates().equals(EMPTY_STRING)){
            byte[] writesBytes = payload.updates().getBytes();
            buffer.putInt( writesBytes.length );
            buffer.put( writesBytes );
        } else {
            buffer.putInt( 0 );
        }
    }

    public static Payload write(ByteBuffer buffer, long tid, long batch, String event, String payload, String precedenceMap, String updates){
        write_(buffer, tid, batch, event, payload, precedenceMap );
        if(!updates.equals(EMPTY_STRING)){
            byte[] writesBytes = updates.getBytes();
            buffer.putInt( writesBytes.length );
            buffer.put( writesBytes );
        } else {
            buffer.putInt( 0 );
        }
        return Payload.of( tid, batch, event, payload, precedenceMap, updates );
    }

    /**
     * Events coming from the leader has no updates
     */
    public static Payload write(ByteBuffer buffer, long tid, long batch, String event, String payload, String precedenceMap){
        write_(buffer, tid, batch, event, payload, precedenceMap );
        return Payload.of( tid, batch, event, payload, precedenceMap );
    }

    private static void write_(ByteBuffer buffer, long tid, long batch, String event, String payload, String precedenceMap){
        buffer.put( Constants.TRANSACTION_EVENT);
        buffer.putLong( tid );
        buffer.putLong( batch );
        byte[] eventBytes = event.getBytes();
        buffer.putInt( eventBytes.length );
        buffer.put( eventBytes );
        byte[] payloadBytes = payload.getBytes();
        buffer.putInt( payloadBytes.length );
        buffer.put( payloadBytes );
        byte[] precedenceBytes = precedenceMap.getBytes();
        buffer.putInt( precedenceBytes.length );
        buffer.put( precedenceBytes );
    }

    public static Payload readFromLeader(ByteBuffer buffer){
        long tid = buffer.getLong();
        long batch = buffer.getLong();
        int eventSize = buffer.getInt();
        String event = ByteUtils.extractStringFromByteBuffer( buffer, eventSize );
        int payloadSize = buffer.getInt();
        String payload = ByteUtils.extractStringFromByteBuffer( buffer, payloadSize );
        int precedenceSize = buffer.getInt();
        String precedenceMap = ByteUtils.extractStringFromByteBuffer( buffer, precedenceSize );
        return Payload.of( tid, batch, event, payload, precedenceMap );
    }

    public static Payload readFromVMS(ByteBuffer buffer){
        long tid = buffer.getLong();
        long batch = buffer.getLong();
        int eventSize = buffer.getInt();
        String event = ByteUtils.extractStringFromByteBuffer( buffer, eventSize );
        int payloadSize = buffer.getInt();
        String payload = ByteUtils.extractStringFromByteBuffer( buffer, payloadSize );
        int precedenceSize = buffer.getInt();
        String precedenceMap = ByteUtils.extractStringFromByteBuffer( buffer, precedenceSize );

        int updateSize = buffer.getInt();
        if(updateSize > 0){
            String updatesMap = ByteUtils.extractStringFromByteBuffer( buffer, updateSize );
            return Payload.of( tid, batch, event, payload, precedenceMap, updatesMap );
        }

        return Payload.of( tid, batch, event, payload, precedenceMap );
    }

    public static final String EMPTY_STRING = "";
    public static final int EMPTY_STRING_SIZE = EMPTY_STRING.getBytes(StandardCharsets.UTF_8).length;

    /**
     * This is the base class for representing the data transferred across the framework and the sidecar
     * It serves both for input and output
     * Why total size? to know the size beforehand, before inserting into the byte buffer
     * otherwise would need further controls...
     */
    public record Payload (
        long tid,
        long batch,
        String event,
        String payload,
        String precedenceMap,
        String updates,
        int totalSize) {
        public static Payload of(long tid, long batch, String event, String payload, String precedenceMap){
            // considering UTF-8
            // https://www.quora.com/How-many-bytes-can-a-string-hold
            int eventBytes = event.length();
            int payloadBytes = payload.length();
            int precedenceMapBytes = precedenceMap.length();
            return new Payload(tid, batch, event, payload, precedenceMap, null, fixedLength + eventBytes + payloadBytes + precedenceMapBytes);
        }

        public static Payload of(long tid, long batch, String event, String payload, String precedenceMap, String updates){
            int eventBytes = event.length();
            int payloadBytes = payload.length();
            int precedenceMapBytes = precedenceMap.length();
            int updatesSize = updates.length();
            return new Payload(tid, batch, event, payload, precedenceMap, updates,fixedLength + eventBytes + payloadBytes + precedenceMapBytes + updatesSize);
        }

        public static Payload of(long tid, long batch, String event, String payload){
            int eventBytes = event.length();
            int payloadBytes = payload.length();
            return new Payload(tid, batch, event, payload, null, null, fixedLength + eventBytes + payloadBytes);
        }

    }

}