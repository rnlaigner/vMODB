package dk.ku.di.dms.vms.web_common;

import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class ProducerWorker extends StoppableRunnable {

    protected static final VarHandle WRITE_SYNCHRONIZER;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            WRITE_SYNCHRONIZER = l.findVarHandle(ProducerWorker.class, "writeSynchronizer", int.class);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }

    protected static final Deque<ByteBuffer> WRITE_BUFFER_POOL = new ConcurrentLinkedDeque<>();

    @SuppressWarnings("unused")
    protected volatile int writeSynchronizer;

    protected final Deque<ByteBuffer> pendingWritesBuffer = new ConcurrentLinkedDeque<>();

    protected final IVmsSerdesProxy serdesProxy;

    // the vms this worker is responsible for
    protected final IdentifiableNode consumerVms;

    protected final ILoggingHandler loggingHandler;

    protected final Queue<ByteBuffer> loggingWriteBuffers;

    protected final List<TransactionEvent.PayloadRaw> drained = new ArrayList<>(1024*10);

    public ProducerWorker(String identifier, IdentifiableNode consumerVms, IVmsSerdesProxy serdesProxy, boolean logging){
        this.consumerVms = consumerVms;
        this.serdesProxy = serdesProxy;
        ILoggingHandler loggingHandler;
        var logIdentifier = identifier+"_"+consumerVms.identifier;
        if(logging){
            this.loggingWriteBuffers = new ConcurrentLinkedQueue<>();
            loggingHandler = LoggingHandlerBuilder.build(logIdentifier);
        } else {
            String loggingStr = System.getProperty("logging");
            if(Boolean.parseBoolean(loggingStr)){
                this.loggingWriteBuffers = new ConcurrentLinkedQueue<>();
                loggingHandler = LoggingHandlerBuilder.build(logIdentifier);
            } else {
                this.loggingWriteBuffers = new LinkedList<>();
                loggingHandler = new ILoggingHandler() { };
            }
        }
        this.loggingHandler = loggingHandler;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void acquireLock(){
        while(! WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1) );
    }

    public boolean tryAcquireLock(){
        return WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1);
    }

    public void releaseLock(){
        WRITE_SYNCHRONIZER.setVolatile(this, 0);
    }

    protected static ByteBuffer retrieveByteBuffer(int size){
        ByteBuffer bb = WRITE_BUFFER_POOL.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(size);
    }

    protected static void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        WRITE_BUFFER_POOL.add(bb);
    }

    protected static final int CUSTOM_HEADER = BatchUtils.HEADER + Integer.BYTES;

}
