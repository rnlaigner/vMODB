package dk.ku.di.dms.vms.modb.persistence;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.IntKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashChainingBufferIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class PersistenceTest {

    private static jdk.internal.misc.Unsafe UNSAFE;

    @BeforeClass
    public static void setUp(){
        UNSAFE = MemoryUtils.UNSAFE;
    }

    // private static final 10737418240
    private static final long ONE_GB = Integer.MAX_VALUE / 2;
    private static final long TWO_GB = Integer.MAX_VALUE;
    private static final long TEN_GB = ONE_GB * 10;

    @Test
    public void testChaining() throws IOException {
        Schema schema = new Schema(new String[]{"id", "num"}, new DataType[]{ DataType.INT, DataType.INT },
                new int[]{ 0 }, null, false );
        String fileName = "mapped_file_test.data";
        Path path = Paths.get(fileName);
        int capacity = MemoryUtils.nextPowerOfTwo(2);
        FileChannel fc = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        MemorySegment memorySegment = fc.map(FileChannel.MapMode.READ_WRITE, 0,
                (long) capacity * schema.getRecordSize(), Arena.ofShared());
        RecordBufferContext bufCtx = RecordBufferContext.build(memorySegment, fileName);
        UniqueHashChainingBufferIndex index = new UniqueHashChainingBufferIndex("test", bufCtx, schema, schema.getPrimaryKeyColumns(),capacity);

        index.insert(IntKey.of(1), new Object[] { 1, 10 } );
        index.insert(IntKey.of(2), new Object[] { 2, 20 } );
        index.insert(IntKey.of(3), new Object[] { 3, 30 } );
        index.insert(IntKey.of(4), new Object[] { 4, 40 } );
        index.insert(IntKey.of(5), new Object[] { 5, 50 } );

        int count = 0;
        IRecordIterator<IKey> it = index.iterator();
        while(it.hasNext()){
            Object[] recordFromAddress = index.readFromIndex(it.address() + Schema.RECORD_HEADER);
            IKey currKey = it.next();
            System.out.println(currKey);
            Object[] recordFromApi = index.record(currKey);
            Assert.assertNotNull(recordFromApi);
            System.out.println(Arrays.toString(recordFromApi));
            Assert.assertEquals(recordFromAddress.length, recordFromApi.length);
            for(int i = 0; i < recordFromAddress.length; i++){
                Assert.assertEquals(recordFromAddress[i], recordFromApi[i]);
            }
            count++;
        }

        Assert.assertEquals(5, count);
        Assert.assertEquals(5, index.size());

        index.reset();
    }

    @Test
    public void testCollision() throws IOException {
        Schema schema = new Schema(new String[]{"id", "test"}, new DataType[]{ DataType.INT, DataType.STRING },
                new int[]{ 0 }, null, false );
        String fileName = "mapped_file_test.data";
        Path path = Paths.get(fileName);
        FileChannel fc = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ,
                StandardOpenOption.SPARSE,
                StandardOpenOption.WRITE
        );
        MemorySegment memorySegment = fc.map(FileChannel.MapMode.READ_WRITE, 0,
                7L * schema.getRecordSize(), Arena.ofShared());
        RecordBufferContext bufCtx = RecordBufferContext.build( memorySegment);
        UniqueHashBufferIndex index = new UniqueHashBufferIndex(bufCtx, schema, schema.getPrimaryKeyColumns(),7);

        index.insert(IntKey.of(10), new Object[] { 10, "test" } );
        index.insert(IntKey.of(14), new Object[] { 14, "test" } );
        index.insert(IntKey.of(24), new Object[] { 24, "test" } );

        Assert.assertEquals(3, index.size());

        index.reset();
    }

    @Test
    public void testMemoryMapping() throws IOException {
        Schema schema = new Schema(new String[]{"id", "test"}, new DataType[]{ DataType.INT, DataType.STRING },
                new int[]{ 0 }, null, false );
        var fileName = "mapped_file_test.data";
        Path path = Paths.get(fileName);
        var fc = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.READ,
                    StandardOpenOption.SPARSE,
                    StandardOpenOption.WRITE
                );
        MemorySegment memorySegment = fc.map(FileChannel.MapMode.READ_WRITE, 0,
                10L * schema.getRecordSize(), Arena.ofShared());

        var bufCtx = RecordBufferContext.build(memorySegment);
        var index = new UniqueHashBufferIndex(bufCtx, schema, schema.getPrimaryKeyColumns(), 10);
        for(int i = 1; i <= 10; i = i + 2){
            index.insert(IntKey.of(i), new Object[] { i, "test" } );
        }

        Object[] recordRes = index.record(IntKey.of(3));
        assert recordRes != null && ((String)recordRes[1]).contentEquals("test");

        // test force now
        bufCtx.force();
        fc.close();
        fc = FileChannel.open(path, StandardOpenOption.READ);

        memorySegment = fc.map(FileChannel.MapMode.READ_ONLY, 0,
                10L * schema.getRecordSize(), Arena.ofShared());
        bufCtx = RecordBufferContext.build(memorySegment);
        index = new UniqueHashBufferIndex(bufCtx, schema, schema.getPrimaryKeyColumns(), 10);

        var bb = ByteBuffer.allocate( 10 * schema.getRecordSize() );
        int res = fc.read(bb);
        Assert.assertEquals(730, res);

        var record = index.lookupByKey( IntKey.of(3) );
        Assert.assertNotNull(record);
        Assert.assertTrue(record[1].toString().contentEquals("test"));
    }

    @Test
    public void testFileMapping() throws IOException {

        String userHome = System.getProperty("user.home");

        String filePath = userHome + "/" + "testFile";

        File file = new File(filePath);
        boolean res = false;
        if(!file.exists()){
            res = file.createNewFile();
        }

        assert res;

        long divFactor = TWO_GB - 8;

        // every byte buffer "loses" 8 bytes per mapping

        int numberBuckets = (int) (TEN_GB / divFactor);

        long lostBytes = numberBuckets * 8;

        MemorySegment segment;
        try (Arena arena = Arena.ofShared()) {
            segment = arena.allocate(TEN_GB - lostBytes);
        }

        long nextOffset = 0;
        // long offsetEnd = divFactor - 1;

        ByteBuffer mappedBuffer;

        long[] initOffsetPerBuffer = new long[numberBuckets];

        Map<Integer, ByteBuffer> buffers = new HashMap<>( numberBuckets);
        for(int i = 0; i < numberBuckets; i++){

            initOffsetPerBuffer[i] = nextOffset;

            MemorySegment seg0 = segment.asSlice(nextOffset, divFactor);
            mappedBuffer = seg0.asByteBuffer();

            buffers.put(i, mappedBuffer);

            nextOffset += divFactor;
            nextOffset++;
        }

        mappedBuffer = buffers.get(0);

        ByteBuffer appBuffer = ByteBuffer.allocate(Integer.BYTES);
        appBuffer.putInt(1);
        appBuffer.clear();

        // writing an integer value to mapped buffer
        mappedBuffer.put( appBuffer );

        // flush
        segment.force();

        // reset position
        mappedBuffer.clear();

        assert(mappedBuffer.getInt() == appBuffer.getInt());

        // next check is regarding the last buffer
        long offsetToTest = initOffsetPerBuffer[2];

        mappedBuffer.position(0);
        mappedBuffer.putInt(10);

        // https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/memory/MemoryUtils.java
        UNSAFE.copyMemory( segment.address(), segment.address() + offsetToTest, Integer.BYTES );

        assert (buffers.get(2).getInt() == 10);

    }

}
