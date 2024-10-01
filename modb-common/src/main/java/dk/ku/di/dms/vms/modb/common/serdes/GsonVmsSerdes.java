package dk.ku.di.dms.vms.modb.common.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link IVmsSerdesProxy}
 * It abstracts the use of {@link Gson}
 */
final class GsonVmsSerdes implements IVmsSerdesProxy {

    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    /**
     VMS METADATA EVENTS
     **/

    @Override
    public String serializeEventSchema(Map<String, VmsEventSchema> vmsEventSchema) {
        return GSON.toJson( vmsEventSchema );
    }

    @Override
    public Map<String, VmsEventSchema> deserializeEventSchema(String vmsEventSchemaStr) {
        return GSON.fromJson(vmsEventSchemaStr, new TypeToken<Map<String, VmsEventSchema>>(){}.getType());
    }

    @Override
    public String serializeDataSchema(Map<String, VmsDataModel> vmsDataSchema) {
        return GSON.toJson( vmsDataSchema );
    }

    @Override
    public Map<String, VmsDataModel> deserializeDataSchema(String dataSchemaStr) {
        return GSON.fromJson(dataSchemaStr, new TypeToken<Map<String, VmsDataModel>>(){}.getType());
    }

    @Override
    public <K,V> String serializeMap( Map<K,V> map ){
        return GSON.toJson( map, new TypeToken<Map<K, V>>(){}.getType() );
    }

    @Override
    public <K,V> Map<K,V> deserializeMap(String mapStr){
        Type type = new TypeToken<Map<K, V>>(){}.getType();
        return GSON.fromJson(mapStr, type);
    }

    @Override
    public <V> String serializeSet(Set<V> map) {
        return GSON.toJson( map, new TypeToken<Set<V>>(){}.getType() );
    }

    @Override
    public <V> Set<V> deserializeSet(String setStr) {
        return GSON.fromJson(setStr, new TypeToken<Set<V>>(){}.getType());
    }

    @Override
    public String serializeConsumerSet(Map<String, List<IdentifiableNode>> map) {
        return GSON.toJson( map );
    }

    private static final Type typeConsMap = new TypeToken<Map<String, List<IdentifiableNode>>>(){}.getType();

    @Override
    public Map<String, List<IdentifiableNode>> deserializeConsumerSet(String mapStr) {
        return GSON.fromJson(mapStr, typeConsMap);
    }

    private static final Type typeDepMap = new TypeToken<Map<String, Long>>(){}.getType();

    @Override
    public Map<String, Long> deserializeDependenceMap(String dependenceMapStr) {
        try {
            return GSON.fromJson(dependenceMapStr, typeDepMap);
        } catch (JsonSyntaxException e){
            throw new RuntimeException("Failed to deserialize dependence map: " + dependenceMapStr, e);
        }
    }

    @Override
    public <V> String serializeList(List<V> list) {
        return GSON.toJson( list, new TypeToken<List<V>>(){}.getType() );
    }

    @Override
    public <V> List<V> deserializeList(String listStr) {
        return GSON.fromJson(listStr, new TypeToken<List<V>>(){}.getType());
    }

    @Override
    public String serializeAsString(Object value, Class<?> clazz) {
        return GSON.toJson( value, clazz );
    }

    @Override
    public byte[] serialize(Object value, Class<?> clazz) {
        return GSON.toJson(value, clazz).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <T> T deserialize(String valueStr, Class<T> clazz) {
        return GSON.fromJson( valueStr, clazz );
    }

    @Override
    public <T> T deserialize(byte[] valueBytes, Class<T> clazz) {
        return GSON.fromJson( new String(valueBytes, StandardCharsets.UTF_8), clazz );
    }

}
