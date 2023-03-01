package dk.ku.di.dms.vms.modb.common.schema.control;

import dk.ku.di.dms.vms.modb.common.schema.Constants;
import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Represents the information necessary of a
 * virtual microservice consumer, such as:
 * (a) The events the VMs subscribes to
 * (b) The foreign keys that must be maintained
 * (c) The tables that must be replicated
 */
public final class ConsumerContext {

    public static void write(ByteBuffer buffer,
                                   String mapStr){
        buffer.put( Constants.CONSUMER_CTX);
        byte[] mapBytes = mapStr.getBytes(StandardCharsets.UTF_8);
        buffer.putInt( mapBytes.length );
        buffer.put( mapBytes );
    }

    public static Map<String, List<NetworkAddress>> read(ByteBuffer buffer, IVmsSerdesProxy proxy){
        int size = buffer.getInt();
        if(size > 0) {
            String consumerSet = ByteUtils.extractStringFromByteBuffer(buffer, size);
            return proxy.deserializeConsumerSet(consumerSet);
        }
        return null;
    }

    /**
     * Simple payload at first and then later we specialize for PKs and subset of columns
     */
    public record Payload(
            Map<String, List<NetworkAddress>> eventToConsumerMap,
            Map<NetworkAddress, List<String>> consumerToTableMap
    ){}

}
