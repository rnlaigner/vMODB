package dk.ku.di.dms.vms.coordinator.store;

import dk.ku.di.dms.vms.coordinator.store.metadata.ServerMetadata;
import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class that encapsulates the complexities of dealing with memory-mapping files in Java
 *
 * Create a (memorymapping) FACADE. This class will implement the facade.
 * this class must be renamed to memorymappingImpl
 *
 */
public class MemoryMappingFactory {

    // hadoop
    static long getOperatingSystemPageSize() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe = (Unsafe)f.get(null);
            return unsafe.pageSize();
        } catch (Throwable e) {
            return 4096; // default size
        }
    }

    // flink
//    @SuppressWarnings("all")
//    private static final class PageSizeUtilInternal {
//
//        static int getSystemPageSize() {
//            Unsafe unsafe = unsafe();
//            return unsafe == null ? PAGE_SIZE_UNKNOWN : unsafe.pageSize();
//        }
//
//        @Nullable
//        private static Unsafe unsafe() {
//            if (PlatformDependent.hasUnsafe()) {
//                return (Unsafe) AccessController.doPrivileged(
//                        (PrivilegedAction<Object>) () -> UnsafeAccess.UNSAFE);
//            }
//            else {
//                return null;
//            }
//        }
//    }

    private static final Logger logger = LoggerFactory.getLogger("MemoryMappingFactory");

    /**
     * The structure of a coordinator metadata is as follows:
     *
     *  Index: pointers to the set of record, integer => position in the buffer
     *         1. leader host
     *         2. known servers (JSON)
     *         3. known VMSs (JSON)
     *         3. last batch info (batch commit offset and last tid of all VMSs [map] ) => long + JSON
     *         4. inputs of last batch (must be the last) => parsed, list of JSON objects [TransactionInput]
     *         5. transactions defined (DAGs) JSON
     *
     *  Last batch info
     *
     */
    private static final String LEADER_FILE = "leader";
    private static final String SERVERS_FILE = "servers";
    private static final String VMS_FILE = "vms";
    private static final String BATCH_FILE = "batch";
    private static final String INPUT_FILE = "input";
    private static final String TRANSACTION_FILE = "transactions";

    // the last position of the respective file. indexed by the above attributes
    private Map<String,Integer> lastPositionIndex = new HashMap<>();

    private static final String DEFAULT_DIR_PATH = System.getProperty("user.home");

    private static final int DEFAULT_FILE_LENGTH = 1024;

    // the files can only be loaded once
    private volatile boolean loaded = false;

    /**
     * Leader host: string size in bytes [host size] + string in bytes [host] + int [port]
     * @return
     */
    public ServerIdentifier loadLeader(){

        ByteBuffer buffer = loadMemoryMappedFile(DEFAULT_DIR_PATH, LEADER_FILE);

        if(buffer == null) return null;

        int port = buffer.getInt();
        int size = buffer.getInt();

        String hostStr = new String( buffer.array(), Integer.BYTES * 2, size );

        lastPositionIndex.put( LEADER_FILE, (Integer.BYTES * 2) + size );

        return new ServerIdentifier( hostStr, port );
    }

    public Map<Integer, ServerIdentifier> loadServers(){

        ByteBuffer buffer = loadMemoryMappedFile(DEFAULT_DIR_PATH, SERVERS_FILE);

        if(buffer == null) return null;

        int N = buffer.getInt();

        Map<Integer, ServerIdentifier> serverMap = new ConcurrentHashMap<>(N);

        for(int i = 0; i < N; i++){
            int port = buffer.getInt();
            int size = buffer.getInt();

            String hostStr = new String( buffer.array(), Integer.BYTES * 2, size );

            int currPos = buffer.position();
            buffer.position( currPos + size + 1 );

            ServerIdentifier server = new ServerIdentifier( hostStr, port );

            serverMap.put( server.hashCode(), server );
        }

        lastPositionIndex.put( SERVERS_FILE, buffer.position() );

        return serverMap;
    }

    public Map<Integer, VmsNode> loadVMSs(){

//        public long lastTid;
//        public long lastBatch;
//        public VmsDataSchema dataSchema;
//        public Map<String, VmsEventSchema> eventSchema;

        ByteBuffer buffer = loadMemoryMappedFile(DEFAULT_DIR_PATH, VMS_FILE);

        if(buffer == null) return null;

        int N = buffer.getInt();

        Map<Integer, VmsNode> vmsMap = new ConcurrentHashMap<>(N);

        for(int i = 0; i < N; i++){

            // continue...

        }

        lastPositionIndex.put( VMS_FILE, buffer.position() );

        return vmsMap;

    }

    private static void createMemoryMappedFile(String dirPath, String fileName, ByteBuffer contents){

        int size = contents.array().length * 2;



    }

    private static MappedByteBuffer loadMemoryMappedFile(String dirPath, String fileName) {
        try {
            String filepath = dirPath + "/" + fileName;
            RandomAccessFile raf = new RandomAccessFile( filepath, "rw" );
            FileChannel channel = raf.getChannel();
            return channel.map(FileChannel.MapMode.READ_WRITE, 0, DEFAULT_FILE_LENGTH);
        } catch(NullPointerException | IOException ignored){ }
        return null;
    }

    private void increaseFileSize(){



    }


    private static void initialize(RandomAccessFile raf) throws IOException {

        // number of records
        raf.writeLong( 0L );

    }

    /**
     *
     * @param raf the file mapped in memory
     * @return
     */
    private static ServerMetadata scan(RandomAccessFile raf) {

        // TODO implement read after finishing the write methods
        // reading the committed offset, starts with 0
        return null;

    }

}
