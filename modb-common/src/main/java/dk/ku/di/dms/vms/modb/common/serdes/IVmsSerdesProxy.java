package dk.ku.di.dms.vms.modb.common.serdes;

import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A proxy for all types of events exchanged between the sdk and the servers
 * Used for complex objects like schema definitions
 */
public interface IVmsSerdesProxy {

    String serializeEventSchema(Map<String, VmsEventSchema> vmsEventSchema);
    Map<String, VmsEventSchema> deserializeEventSchema(String vmsEventSchema);

    String serializeDataSchema(Map<String, VmsDataModel> vmsDataSchema);
    Map<String, VmsDataModel> deserializeDataSchema(String vmsDataSchema);

    <K,V> String serializeMap(Map<K,V> map);
    <K,V> Map<K,V> deserializeMap(String mapStr);

    <V> String serializeSet(Set<V> set);
    <V> Set<V> deserializeSet(String setStr);

    String serializeConsumerSet(Map<String, List<IdentifiableNode>> map);
    Map<String, List<IdentifiableNode>> deserializeConsumerSet(String mapStr);

    Map<String, Long> deserializeDependenceMap(String dependenceMapStr);

    <V> String serializeList(List<V> list);
    <V> List<V> deserializeList(String listStr);

    String serializeAsString(Object value, Class<?> clazz);
    byte[] serialize(Object value, Class<?> clazz);
    <T> T deserialize(String valueStr, Class<T> clazz);
    <T> T deserialize(byte[] valueBytes, Class<T> clazz);
}
