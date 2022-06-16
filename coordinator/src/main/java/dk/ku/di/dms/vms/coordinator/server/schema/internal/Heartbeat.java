package dk.ku.di.dms.vms.coordinator.server.schema.internal;

import dk.ku.di.dms.vms.coordinator.metadata.ServerIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.server.infra.Constants.HEARTBEAT;

/**
 * The payload of a heartbeat
 */
public final class Heartbeat {

    // type | port | size | <host address is variable>
    private static final int headerSize = Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, ServerIdentifier serverIdentifier){
        byte[] hostBytes = serverIdentifier.host.getBytes();
        buffer.put( HEARTBEAT );
        buffer.putInt(serverIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );
    }

    public static HeartbeatPayload read(ByteBuffer buffer){
        HeartbeatPayload payload = new HeartbeatPayload();
        payload.port = buffer.getInt();
        int size = buffer.getInt();
        payload.host = new String(buffer.array(), headerSize, size, StandardCharsets.UTF_8 );
        return payload;
    }

    public static class HeartbeatPayload {
        public String host;
        public int port;
    }

}
