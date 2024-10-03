package dk.ku.di.dms.vms.modb.common.memory;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public final class MemoryUtils {

    static {
        // initialize everything
        try {
            UNSAFE = getUnsafe();
            BUFFER_ADDRESS_FIELD_OFFSET =
                    getClassFieldOffset(Buffer.class, "address");
            BUFFER_CAPACITY_FIELD_OFFSET = getClassFieldOffset(Buffer.class, "capacity");
            DIRECT_BYTE_BUFFER_CLASS = getClassByName("java.nio.DirectByteBuffer");

            DEFAULT_PAGE_SIZE = getPageSize();
            DEFAULT_NUM_BUCKETS = 10;
            DEFAULT_NUM_RECORDS= 100000;
        } catch (Exception ignored) {}

    }

    public static jdk.internal.misc.Unsafe UNSAFE;

    private static long BUFFER_ADDRESS_FIELD_OFFSET;
    private static long BUFFER_CAPACITY_FIELD_OFFSET;
    private static Class<?> DIRECT_BYTE_BUFFER_CLASS;

    public static int DEFAULT_PAGE_SIZE;

    public static int DEFAULT_NUM_BUCKETS;

    public static int DEFAULT_NUM_RECORDS;

    /**
     * <a href="https://stackoverflow.com/questions/19047584/getting-virtual-memory-page-size-by-java-code">link</a>
     * @return page size
     */
    private static int getPageSize(){
        return UNSAFE.pageSize();
    }

    private static long getClassFieldOffset(@SuppressWarnings("SameParameterValue")
                                                    Class<?> cl, String fieldName) throws NoSuchFieldException {
        return UNSAFE.objectFieldOffset(cl.getDeclaredField(fieldName));
    }

    public static Class<?> getClassByName(
            @SuppressWarnings("SameParameterValue") String className) throws ClassNotFoundException {

        return Class.forName(className);
    }

    private static jdk.internal.misc.Unsafe getUnsafe() {
        Field unsafeField;
        try {
            unsafeField = Unsafe.class.getDeclaredField("theInternalUnsafe");
            unsafeField.setAccessible(true);
            return (jdk.internal.misc.Unsafe) unsafeField.get(null);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    // if the byte buffer is not direct this class will throw error...
    public static void changeByteBufferAddress(ByteBuffer buffer, long newAddress){
        UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, newAddress);
    }

    public static ByteBuffer wrapUnsafeMemoryWithByteBuffer(long address, int size) {
        try {
            ByteBuffer buffer = (ByteBuffer) UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
            UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
            buffer.clear();
            return buffer;
        } catch (Throwable t) {
            throw new Error("Failed to wrap unsafe off-heap memory with ByteBuffer", t);
        }
    }

    public static long getByteBufferAddress(ByteBuffer buffer) {
        return UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
    }

    public static int nextPowerOfTwo(int number) {
        number--;
        number |= number >> 1;
        number |= number >> 2;
        number |= number >> 4;
        number |= number >> 8;
        number |= number >> 16;
        return number + 1;
    }

}
