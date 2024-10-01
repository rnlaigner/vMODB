package dk.ku.di.dms.vms.coordinator.vms;

import dk.ku.di.dms.vms.coordinator.options.VmsWorkerOptions;
import dk.ku.di.dms.vms.modb.common.logging.ILoggingHandler;
import dk.ku.di.dms.vms.modb.common.logging.LoggingHandlerBuilder;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.IChannel;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static dk.ku.di.dms.vms.coordinator.vms.VmsWorker.State.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static java.lang.System.Logger.Level.*;
import static java.lang.Thread.sleep;

@SuppressWarnings("SequencedCollectionMethodCanBeUsed")
public final class VmsWorker extends StoppableRunnable implements IVmsWorker {

    private static final System.Logger LOGGER = System.getLogger(VmsWorker.class.getName());

    private static final VarHandle WRITE_SYNCHRONIZER;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            WRITE_SYNCHRONIZER = l.findVarHandle(VmsWorker.class, "writeSynchronizer", int.class);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }

    @SuppressWarnings("unused")
    private volatile int writeSynchronizer;

    enum State {
        NEW,
        CONNECTION_ESTABLISHED,
        CONNECTION_FAILED,
        LEADER_PRESENTATION_SENT,
        LEADER_PRESENTATION_SEND_FAILED,
        VMS_PRESENTATION_RECEIVED,
        VMS_PRESENTATION_RECEIVE_FAILED,
        VMS_PRESENTATION_PROCESSED,
        CONSUMER_SET_READY_FOR_SENDING,
        CONSUMER_SET_SENDING_FAILED,
        CONSUMER_EXECUTING
    }

    private final ServerNode me;
    
    private final VmsWorkerOptions options;

    // the vms this worker is responsible for
    private final IdentifiableNode consumerVms;

    private State state;

    private final ILoggingHandler loggingHandler;

    private final IVmsSerdesProxy serdesProxy;

    private final ByteBuffer readBuffer;

    private final Deque<ByteBuffer> writeBufferPool;

    /**
     * Queue to inform coordinator about an important event
     */
    private final Queue<Object> coordinatorQueue;

    private final Supplier<IChannel> channelFactory;
    private IChannel channel;

    // DTs particular to this vms worker
    private final IVmsDeque transactionEventQueue;

    private final Deque<Object> messageQueue;

    private final Queue<ByteBuffer> loggingWriteBuffers = new ConcurrentLinkedQueue<>();

    private final Deque<ByteBuffer> pendingWriteBuffers = new ConcurrentLinkedDeque<>();

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final BatchWriteCompletionHandler batchWriteCompletionHandler = new BatchWriteCompletionHandler();

    private final Consumer<Object> queueMessage_;

    private interface IVmsDeque {
        void drain(List<TransactionEvent.PayloadRaw> list, int maxSize);
        void insert(TransactionEvent.PayloadRaw payloadRaw);
    }

    private static final class MultiDeque implements IVmsDeque {
        private final int numQueues;
        private int nextPos;
        private final List<Deque<TransactionEvent.PayloadRaw>> queues;
        private MultiDeque(int numQueues) {
            this.numQueues = numQueues;
            this.nextPos = 0;
            this.queues = new ArrayList<>(numQueues);
            for (int i = 0; i < numQueues; i++) {
                this.queues.add(new ConcurrentLinkedDeque<>());
            }
        }
        @Override
        public void insert(TransactionEvent.PayloadRaw payloadRaw) {
            // this method can be called concurrently
            int pos = this.nextPos-1; if(pos < 0) pos = 0;
            this.queues.get(pos).add(payloadRaw);
        }
        @Override
        public void drain(List<TransactionEvent.PayloadRaw> list, int maxSize){
            int totalSize = 0;
            TransactionEvent.PayloadRaw txEvent;
            while ((txEvent = this.queues.get(this.nextPos).poll()) != null){
                list.add(txEvent);
                totalSize += txEvent.totalSize();
                if(totalSize >= maxSize) break;
            }
            this.nextPos++;
            if(this.nextPos == this.numQueues){ this.nextPos=0; }
        }
    }

    private static final class SingleDeque implements IVmsDeque {
        private final ConcurrentLinkedDeque<TransactionEvent.PayloadRaw> queue = new ConcurrentLinkedDeque<>();
        @Override
        public void insert(TransactionEvent.PayloadRaw payloadRaw) {
            this.queue.offerLast(payloadRaw);
        }
        @Override
        public void drain(List<TransactionEvent.PayloadRaw> list, int maxSize){
            int totalSize = 0;
            TransactionEvent.PayloadRaw txEvent = this.queue.poll();
            if(txEvent == null) return;
            totalSize += txEvent.totalSize();
            while(true) {
                list.add(txEvent);
                totalSize += txEvent.totalSize();
                if(totalSize >= maxSize){
                    break;
                }
                txEvent = this.queue.poll();
                if(txEvent == null) break;
            }
        }
    }
    
    public static VmsWorker build(// coordinator reference
                                    ServerNode me,
                                    // the vms this thread is responsible for
                                    IdentifiableNode consumerVms,
                                    // shared data structure to communicate messages to coordinator
                                    Queue<Object> coordinatorQueue,
                                    Supplier<IChannel> channelFactory,
                                    VmsWorkerOptions options,
                                    IVmsSerdesProxy serdesProxy) throws IOException {
        return new VmsWorker(me, consumerVms, coordinatorQueue,
                channelFactory, options, serdesProxy);
    }

    private VmsWorker(// coordinator reference
                      ServerNode me,
                      // the vms this thread is responsible for
                      IdentifiableNode consumerVms,
                      // events to share with coordinator
                      Queue<Object> coordinatorQueue,
                      Supplier<IChannel> channelFactory,
                      VmsWorkerOptions options,
                      IVmsSerdesProxy serdesProxy) {
        this.me = me;
        this.state = State.NEW;
        this.consumerVms = consumerVms;

        this.channelFactory = channelFactory;
        this.options = options;

        // initialize the write buffer
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        // populate buffer pool
        this.writeBufferPool.addFirst( retrieveByteBuffer() );
        this.writeBufferPool.addFirst( retrieveByteBuffer() );

        this.readBuffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());

        // logging
        if(options.logging()) {
            this.loggingHandler = LoggingHandlerBuilder.build("coordinator_"+consumerVms.identifier); }
        else {
            this.loggingHandler = new ILoggingHandler() { };
        }
        this.serdesProxy = serdesProxy;

        // in
        this.messageQueue = new ConcurrentLinkedDeque<>();
        if(options.numQueuesVmsWorker() > 1) {
            this.transactionEventQueue = new MultiDeque(options.numQueuesVmsWorker());
        } else {
            this.transactionEventQueue = new SingleDeque();
        }

        if(this.options.active()){
            this.queueMessage_ = this.messageQueue::offerLast;
        } else {
            this.queueMessage_ = this::sendMessage;
        }

        // out - shared by many vms workers
        this.coordinatorQueue = coordinatorQueue;
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

    private void connect() throws IOException, InterruptedException, ExecutionException {
        this.channel = channelFactory.get();
        NetworkUtils.configure(this.channel, options.networkBufferSize());
        // if not active, maybe set tcp_nodelay to true?
        this.channel.connect(this.consumerVms.asInetSocketAddress()).get();
    }

    @SuppressWarnings("BusyWait")
    public void initHandshakeProtocol(){
        int waitTime = 1000;
        LOGGER.log(INFO, "Leader: Attempting connection to "+this.consumerVms.identifier);
        while(true) {
            try {
                this.connect();
                LOGGER.log(INFO, "Leader: Connection established to "+this.consumerVms.identifier);
                break;
            } catch (IOException | InterruptedException | ExecutionException e) {
                LOGGER.log(ERROR, "Leader: Connection attempt to " + this.consumerVms.identifier + " failed. Retrying in "+waitTime/1000+" second(s)...");
                try {
                    sleep(waitTime);
                    waitTime = waitTime + 1000;
                } catch (InterruptedException ignored) { }
            }
        }

        try {
            LOGGER.log(INFO, "Leader: Sending presentation to "+this.consumerVms.identifier);
            this.state = CONNECTION_ESTABLISHED;
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            this.sendLeaderPresentationToVms(writeBuffer);
            this.state = State.LEADER_PRESENTATION_SENT;

            // set read handler here
            this.channel.read( this.readBuffer, 0, new VmsReadCompletionHandler() );
        } catch (Exception e) {
            LOGGER.log(WARNING,"Failed to connect to a known VMS: " + this.consumerVms.identifier);
            this.releaseLock();
            if (this.state == State.NEW) {
                // forget about it, let the vms connect then...
                this.state = State.CONNECTION_FAILED;
            } else if(this.state == CONNECTION_ESTABLISHED) {
                this.state = LEADER_PRESENTATION_SEND_FAILED;
                // check if connection is still online. if so, try again
                // otherwise, retry connection in a few minutes
                if(this.channel.isOpen()){
                    // try again? what is the problem?
                    LOGGER.log(WARNING,"It was not possible to send a presentation message, although the channel is open. The connection will be closed now.");
                    this.channel.close();
                } else {
                    LOGGER.log(WARNING,"It was not possible to send a presentation message and the channel is not open. Check the consumer VMS: " + consumerVms);
                }
            } else {
                LOGGER.log(WARNING,"Cannot find the root problem. Please have a look: "+e.getCause().getMessage());
            }
            // important for consistency of state (if debugging, good to see the code controls the thread state)
            this.stop();
        }
    }

    // write presentation
    private void sendLeaderPresentationToVms(ByteBuffer writeBuffer) {
        Presentation.writeServer(writeBuffer, this.me, true);
        writeBuffer.flip();
        this.acquireLock();
        this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
    }

    @Override
    public void run() {
        LOGGER.log(INFO, "VmsWorker starting for consumer VMS: "+this.consumerVms);
        if(this.options.initHandshake()) {
            this.initHandshakeProtocol();
        } else {
            if (this.initSimpleConnection()) return;
        }
        if(this.options.active()) {
            this.eventLoop();
        }
        LOGGER.log(INFO, "VmsWorker finished for consumer VMS: "+this.consumerVms);
    }

    private boolean initSimpleConnection() {
        // only connect. the presentation has already been sent
        LOGGER.log(DEBUG, Thread.currentThread().getName()+": Attempting additional connection to to VMS: " + this.consumerVms.identifier);
        try {
            this.connect();
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            this.sendLeaderPresentationToVms(writeBuffer);
        } catch(Exception ignored){
            LOGGER.log(ERROR, Thread.currentThread().getName()+": Cannot connect to VMS: " + this.consumerVms.identifier);
            return true;
        }
        LOGGER.log(INFO, Thread.currentThread().getName()+": Additional connection to "+this.consumerVms.identifier+" succeeded");
        return false;
    }

    /**
     * Event loop performs some tasks:
     * (a) Receive and process batch-tracking messages from coordinator
     * (b) Process transaction input events
     * (c) Logging
     */
    private void eventLoop() {
        int pollTimeout = 1;
        while (this.isRunning()){
            try {
                this.transactionEventQueue.drain(this.drained, this.options.networkBufferSize());
                if(this.drained.isEmpty()){
                    pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep());
                    this.processPendingNetworkTasks();
                    this.processPendingLogging();
                    this.giveUpCpu(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
//                if(!this.transactionEventQueue.isEmpty()){
                    this.sendBatchOfEvents();
//                } else {
//                    this.sendEvent(payloadRaw);
//                }
                this.processPendingNetworkTasks();
                this.processPendingLogging();
            } catch (Exception e) {
                LOGGER.log(ERROR, "Leader: VMS worker for "+this.consumerVms.identifier+" has caught an exception: \n"+e);
            }
        }
    }

    private void processPendingLogging(){
        ByteBuffer writeBuffer;
        if((writeBuffer = this.loggingWriteBuffers.poll()) != null){
            try {
                writeBuffer.position(0);
                this.loggingHandler.log(writeBuffer);
                // return buffer
                this.returnByteBuffer(writeBuffer);
            } catch (IOException e) {
                LOGGER.log(ERROR, "error on writing byte buffer to logging file: "+e.getMessage());
                this.loggingWriteBuffers.add(writeBuffer);
            }
        }
    }

    private void processPendingNetworkTasks() {
        Object pendingMessage;
        while((pendingMessage = this.messageQueue.pollFirst()) != null){
            this.sendMessage(pendingMessage);
        }
        ByteBuffer bb = this.pendingWriteBuffers.poll();
        if(bb != null){
            LOGGER.log(INFO, "Leader: Retrying sending failed buffer send");
            try {
                this.acquireLock();
                this.channel.write(bb, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, bb, this.batchWriteCompletionHandler);
            } catch (Exception e){
                LOGGER.log(ERROR, "Leader: ERROR on retrying to send failed buffer: \n"+e);
                if(e instanceof IllegalStateException){
                    // probably comes from the class {@AsynchronousSocketChannelImpl}:
                    // "Writing not allowed due to timeout or cancellation"
                    // stop thread since there is no way to write to this channel anymore
                    this.stop();
                }
                this.releaseLock();
                bb.clear();
                this.pendingWriteBuffers.offer(bb);
            }
        }
    }

    @Override
    public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
        this.transactionEventQueue.insert(payloadRaw);
    }

    @SuppressWarnings("unused")
    private void sendEvent(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        TransactionEvent.write( writeBuffer, payload );
        writeBuffer.flip();
        this.acquireLock();
        this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
    }

    @Override
    public void queueMessage(Object message) {
        this.queueMessage_.accept(message);
    }

    private void sendMessage(Object message) {
        switch (message) {
            case BatchCommitCommand.Payload o -> {
                this.sendBatchCommitCommand(o);
                this.loggingHandler.force();
            }
            case BatchCommitInfo.Payload o -> {
                this.sendBatchCommitInfo(o);
                this.loggingHandler.force();
            }
            case TransactionAbort.Payload o -> this.sendTransactionAbort(o);
            case String o -> this.sendConsumerSet(o);
            default ->
                    LOGGER.log(WARNING, "Leader: VMS worker for " + this.consumerVms.identifier + " has unknown message type: " + message.getClass().getName());
        }
    }

    private void sendTransactionAbort(TransactionAbort.Payload tidToAbort) {
        ByteBuffer writeBuffer = retrieveByteBuffer();
        TransactionAbort.write(writeBuffer, tidToAbort);
        writeBuffer.flip();
        this.acquireLock();
        this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
        LOGGER.log(WARNING,"Leader: Transaction abort sent to: " + this.consumerVms.identifier);
    }

    private void sendBatchCommitCommand(BatchCommitCommand.Payload batchCommitCommand) {
        try {
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            BatchCommitCommand.write(writeBuffer, batchCommitCommand);
            writeBuffer.flip();
            this.acquireLock();
            this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            LOGGER.log(DEBUG, "Leader: Batch ("+batchCommitCommand.batch()+") commit command sent to: " + this.consumerVms.identifier);
        } catch (Exception e){
            LOGGER.log(ERROR,"Leader: Batch ("+batchCommitCommand.batch()+") commit command write has failed:\n"+e.getMessage());
            if(!this.channel.isOpen()){
                LOGGER.log(WARNING,"Leader: Channel with "+this.consumerVms.identifier+"is closed");
                this.stop(); // no reason to continue the loop
            }
            this.releaseLock();
            this.messageQueue.offerFirst(batchCommitCommand);
        }
    }

    private void sendConsumerSet(String vmsConsumerSet) {
        // the first or new information
        if(this.state == VMS_PRESENTATION_PROCESSED) {
            this.state = CONSUMER_SET_READY_FOR_SENDING;
            LOGGER.log(INFO, "Leader: Consumer set will be sent to: "+this.consumerVms.identifier);
        } else if(this.state == CONSUMER_EXECUTING){
            LOGGER.log(INFO, "Leader: Consumer set is going to be updated for: "+this.consumerVms.identifier);
        } else if(this.state == CONSUMER_SET_SENDING_FAILED){
            LOGGER.log(INFO, "Leader: Consumer set, another attempt to write to: "+this.consumerVms.identifier);
        } // else, nothing...

        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        try {
            ConsumerSet.write(writeBuffer, vmsConsumerSet);
            writeBuffer.flip();
            this.acquireLock();
            this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            if (this.state == CONSUMER_SET_READY_FOR_SENDING) {// or != CONSUMER_EXECUTING
                this.state = CONSUMER_EXECUTING;
            }
        } catch (IOException | BufferOverflowException e){
            this.state = CONSUMER_SET_SENDING_FAILED;
            LOGGER.log(WARNING,"Write has failed and the VMS worker will undergo an unknown state: " + consumerVms, e);
            this.releaseLock();
            this.stop(); // no reason to continue the loop
        }
    }

    /**
     * Reuses the thread from the socket thread pool, instead of assigning a specific thread
     * Removes thread context switching costs.
     * This thread should not block.
     * The idea is to decode the message and deliver back to socket loop as soon as possible
     * This thread must be set free as soon as possible, should not do long-running computation
     */
    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, Integer> {
        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                LOGGER.log(WARNING, "Leader: " + consumerVms.identifier+" has disconnected!");
                channel.close();
                return;
            }
            if(startPos == 0){
                readBuffer.flip();
            }
            // decode message by getting the first byte
            readBuffer.position(startPos);
            byte type = readBuffer.get();
            try {
                switch (type) {
                    case PRESENTATION -> {
                        // this is a very bad conditional statement
                        // we could do better removing this concept of "unknown" and simply check the state
                        if (state == LEADER_PRESENTATION_SENT) {
                            state = VMS_PRESENTATION_RECEIVED;// for the first time
                            this.processVmsIdentifier();
                            state = VMS_PRESENTATION_PROCESSED;
                        } else {
                            // in the future it can be an update of the vms schema or crash recovery
                            LOGGER.log(ERROR, "Leader: Presentation already received from VMS: " + consumerVms.identifier);
                        }
                    }
                    // from all terminal VMSs involved in the last batch
                    case BATCH_COMPLETE -> {
                        // don't actually need the host and port in the payload since we have the attachment to this read operation...
                        BatchComplete.Payload response = BatchComplete.read(readBuffer);
                        LOGGER.log(DEBUG, "Leader: Batch (" + response.batch() + ") complete received from: " + consumerVms.identifier);
                        // must have a context, i.e., what batch, the last?
                        coordinatorQueue.add(response);
                        // if one abort, no need to keep receiving
                        // actually it is unclear in which circumstances a vms would respond no... probably in case it has not received an ack from an aborted commit response?
                        // because only the aborted transaction will be rolled back
                    }
                    case BATCH_COMMIT_ACK -> {
                        LOGGER.log(DEBUG, "Leader: Batch commit ACK received from: " + consumerVms.identifier);
                        BatchCommitAck.Payload response = BatchCommitAck.read(readBuffer);
                        // logger.config("Just logging it, since we don't necessarily need to wait for that. "+response);
                        coordinatorQueue.add(response);
                    }
                    case TX_ABORT -> {
                        // get information of what
                        TransactionAbort.Payload response = TransactionAbort.read(readBuffer);
                        coordinatorQueue.add(response);
                    }
                    case EVENT -> LOGGER.log(INFO, "Leader: New event received from: " + consumerVms.identifier);
                    case BATCH_OF_EVENTS -> LOGGER.log(INFO, "Leader: New batch of events received from VMS");
                    default -> LOGGER.log(WARNING, "Leader: Unknown message received.");

                }
            } catch (BufferUnderflowException e){
                LOGGER.log(WARNING, "Leader: Buffer underflow captured. Will read more with the hope the full data is delivered.");
                e.printStackTrace(System.out);
                readBuffer.position(startPos);
                readBuffer.compact();
                channel.read( readBuffer, startPos, this );
                return;
            } catch (Exception e){
                LOGGER.log(ERROR, "Leader: Unknown error captured:"+e.getMessage(), e);
                e.printStackTrace(System.out);
                return;
            }
            if(readBuffer.hasRemaining()){
                this.completed(result, readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private void setUpNewRead() {
            readBuffer.clear();
            channel.read(readBuffer, 0, this);
        }

        @Override
        public void failed(Throwable exc, Integer startPosition) {
            if(state == LEADER_PRESENTATION_SENT){
                state = VMS_PRESENTATION_RECEIVE_FAILED;
                LOGGER.log(WARNING,"It was not possible to receive a presentation message from consumer VMS: "+exc.getMessage());
            } else {
                if (channel.isOpen()) {
                    LOGGER.log(WARNING,"Read has failed but channel is open. Trying to read again from: " + consumerVms);
                } else {
                    LOGGER.log(WARNING,"Read has failed and channel is closed: " + consumerVms);
                }
            }
            this.setUpNewRead();
        }

        private void processVmsIdentifier() {
            // always a vms
            readBuffer.position(2);
            // vms is sending local interface as host, so it is necessary to overwrite that
            VmsNode vmsNodeReceived = Presentation.readVms(readBuffer, serdesProxy, consumerVms.host);
            state = State.VMS_PRESENTATION_PROCESSED;
            // let coordinator aware this vms worker already has the vms identifier
            coordinatorQueue.add(vmsNodeReceived);
        }
    }

    private void sendBatchCommitInfo(BatchCommitInfo.Payload batchCommitInfo){
        // then send only the batch commit info
        LOGGER.log(DEBUG, "Leader: Batch ("+batchCommitInfo.batch()+") commit info will be sent to " + this.consumerVms.identifier);
        try {
            ByteBuffer writeBuffer = this.retrieveByteBuffer();
            BatchCommitInfo.write(writeBuffer, batchCommitInfo);
            writeBuffer.flip();
            this.acquireLock();
            this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            LOGGER.log(DEBUG, "Leader: Batch ("+batchCommitInfo.batch()+") commit info sent to " + this.consumerVms.identifier+"\n"+batchCommitInfo);
        } catch (Exception e) {
            LOGGER.log(ERROR, "Leader: Error on sending a batch commit info to VMS: " + e.getMessage());
            this.releaseLock();
            this.messageQueue.offerFirst(batchCommitInfo);
        }
    }

    private ByteBuffer retrieveByteBuffer() {
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        //logger.log(INFO, "New ByteBuffer will be created");
        // leads to several bugs =(
        // return ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        return MemoryManager.getTemporaryDirectBuffer(this.options.networkBufferSize());
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        this.writeBufferPool.add(bb);
    }

    private final List<TransactionEvent.PayloadRaw> drained = new ArrayList<>(1024);

    /**
     * While a write operation is in progress, it must wait for completion and then submit the next write.
     */
    private void sendBatchOfEvents(){
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer;
        while(remaining > 0) {
            try {
                writeBuffer = this.retrieveByteBuffer();
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer);

                LOGGER.log(DEBUG, "Leader: Submitting ["+(count - remaining)+"] events to "+consumerVms.identifier);
                count = remaining;
                writeBuffer.flip();

                // blocking way
//                do {
//                    this.channel.write(writeBuffer).get();
//                } while(writeBuffer.hasRemaining());
//                this.returnByteBuffer(writeBuffer);

                // maximize useful work
                while(!this.tryAcquireLock()){
                    this.processPendingLogging();
                }
                this.channel.write(writeBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, writeBuffer, this.batchWriteCompletionHandler);
            } catch (Exception e) {
                LOGGER.log(ERROR, "Leader: Error on submitting ["+count+"] events to "+this.consumerVms.identifier+":"+e);
                // return events to the deque
                while(!this.drained.isEmpty()) {
                    this.transactionEventQueue.insert(this.drained.remove(0));
                }
                // force exit loop
                remaining = 0;
                this.releaseLock();
            }
        }
        this.drained.clear();
    }

    /**
     * For commit-related messages
     */
    private final class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            if(byteBuffer.hasRemaining()) {
                LOGGER.log(WARNING, "Leader: Found not all bytes of message (type: "+byteBuffer.get(0)+") were sent to "+consumerVms.identifier+" Trying to send the remaining now...");
                channel.write(byteBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, byteBuffer, this);
                return;
            }
            releaseLock();
            returnByteBuffer(byteBuffer);
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            releaseLock();
            LOGGER.log(ERROR, "Leader: ERROR on writing batch of events to "+consumerVms.identifier+": "+exc);
            returnByteBuffer(byteBuffer);
        }
    }

    private final class BatchWriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            LOGGER.log(DEBUG, "Leader: Message with size " + result + " has been sent to: " + consumerVms.identifier);
            if(byteBuffer.hasRemaining()) {
                // keep the lock and send the remaining
                channel.write(byteBuffer, options.networkSendTimeout(), TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                releaseLock();
                if(options.logging()){
                    loggingWriteBuffers.add(byteBuffer);
                } else {
                    returnByteBuffer(byteBuffer);
                }
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            releaseLock();
            LOGGER.log(ERROR, "Leader: ERROR on writing batch of events to "+consumerVms.identifier+": "+exc);
            byteBuffer.position(0);
            pendingWriteBuffers.offer(byteBuffer);
            LOGGER.log(INFO,  "Leader: Byte buffer added to pending queue. #pending: "+ pendingWriteBuffers.size());
        }
    }

}
