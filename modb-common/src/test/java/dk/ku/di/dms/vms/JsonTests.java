package dk.ku.di.dms.vms;

import com.google.gson.*;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsTableSchema;
import org.junit.Test;

import java.io.*;
import java.util.Base64;

public class JsonTests {

    private static VmsTableSchema buildDefaultMetadata(){
        // return new VmsDataSchema("test","table", new int[]{1,2,3}, new String[]{"1"}, new DataType[]{DataType.INT}, null, null );
        return null;
    }

    @Test
    public void test0() throws IOException {
//        ByteArrayOutputStream stream = new ByteArrayOutputStream();
//        // OutputStreamWriter oswriter = new OutputStreamWriter();
//        GsonBuilder builder = new GsonBuilder();
//        Gson gson = builder.create();
//
//        StringWriter writer = new StringWriter();
//        CustomWriter custom = new CustomWriter();
//
//        StringBuffer sb = new StringBuffer();
//
//        JsonWriter jsonWriter = gson.newJsonWriter(custom);
//
//        gson.toJson(buildDefaultMetadata(), VmsDataSchema.class, jsonWriter);
//
//        custom.
//
//        return writer.getBuffer();

        // JsonWriter writer = new JsonWriter();

        // gson.createWriter(stream).write(obj);
    }

    @Test
    public void test1(){
        VmsTableSchema schema = buildDefaultMetadata();

        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(byte[].class, (JsonSerializer<byte[]>) (src, typeOfSrc, context) -> new JsonPrimitive(Base64.getEncoder().encodeToString(src)));
        builder.registerTypeAdapter(byte[].class, (JsonDeserializer<byte[]>) (json, typeOfT, context) -> Base64.getDecoder().decode(json.getAsString()));
        Gson gson = builder.create();

        gson.toJson( schema, byte[].class );

    }

    /*

            InputStream is = new ByteArrayInputStream(bytes);
        try {
            InputStreamReader r = new InputStreamReader( is, "UTF-8" );

            JsonReader reader = new JsonReader( r );

            reader.

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
     */

}
