package dk.ku.di.dms.vms.coordinator.election.schema;

import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static dk.ku.di.dms.vms.coordinator.election.Constants.LEADER_REQUEST;

public class LeaderRequest {

    // type | port | size | <host address is variable>
    private static final int fixedSize = Byte.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, ServerIdentifier serverIdentifier){

        byte[] hostBytes = serverIdentifier.host.getBytes();

        buffer.put(LEADER_REQUEST);
        buffer.putInt(serverIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );

    }

    public static LeaderRequestPayload read(ByteBuffer buffer){

        int port = buffer.getInt();

        int size = buffer.getInt();

        String host;
        if(buffer.isDirect()){
            byte[] byteArray = new byte[size];
            for(int i = 0; i < size; i++){
                byteArray[i] = buffer.get();
            }
            host = new String(byteArray, 0, size, StandardCharsets.UTF_8);
        } else {
            // 1 + 8 + 4 = 8 + 4 =
            host = new String(buffer.array(), fixedSize, size, StandardCharsets.UTF_8);
        }

        return new LeaderRequestPayload( host, port );

    }

    public static class LeaderRequestPayload {
        public String host;
        public int port;
        public int hashCode;

        public LeaderRequestPayload(String host, int port) {
            this.host = host;
            this.port = port;
            this.hashCode = Objects.hash(host, port);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

    }

}
