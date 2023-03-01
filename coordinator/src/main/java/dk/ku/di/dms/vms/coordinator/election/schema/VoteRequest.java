package dk.ku.di.dms.vms.coordinator.election.schema;

import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.coordinator.election.Constants.VOTE_REQUEST;

/**
 * The payload of a request for info
 */
public class VoteRequest {

                                        // type |     offset |       port |      timestamp |      size | <host address is variable>
    private static final int fixedSize = Byte.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    public static void write(ByteBuffer buffer, ServerIdentifier serverIdentifier){

        byte[] hostBytes = serverIdentifier.host.getBytes();

        buffer.put(VOTE_REQUEST);
        buffer.putLong( serverIdentifier.lastOffset );
        buffer.putInt(serverIdentifier.port );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );

    }

    public static ServerIdentifier read(ByteBuffer buffer){

        long offset = buffer.getLong();

        int port = buffer.getInt();

        int size = buffer.getInt();

        // 1 + 8 + 4 = 8 + 4 =
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

        return new ServerIdentifier(  host, port, offset );

    }


}
