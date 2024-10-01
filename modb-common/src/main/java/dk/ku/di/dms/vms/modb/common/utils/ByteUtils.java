package dk.ku.di.dms.vms.modb.common.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class ByteUtils {

    public static String extractStringFromByteBuffer(ByteBuffer buffer, int size){
        String extracted;
        if(buffer.hasArray()){
            extracted = new String(buffer.array(), buffer.position(), size, StandardCharsets.UTF_8);
            buffer.position(buffer.position() + size);
        } else {
            byte[] byteArray = new byte[size];
            buffer.get(byteArray);
            extracted = new String(byteArray, StandardCharsets.UTF_8);
        }
        return extracted;
    }

    public static byte[] extractByteArrayFromByteBuffer(ByteBuffer buffer, int size) {
        byte[] byteArray = new byte[size];
        buffer.get(byteArray);
        return byteArray;
    }
}
