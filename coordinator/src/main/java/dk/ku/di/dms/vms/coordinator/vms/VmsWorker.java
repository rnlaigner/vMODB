package dk.ku.di.dms.vms.coordinator.vms;

import dk.ku.di.dms.vms.modb.common.compressing.CompressingUtils;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
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
import dk.ku.di.dms.vms.web_common.ProducerWorker;
import dk.ku.di.dms.vms.web_common.channel.IChannel;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscBlockingConsumerArrayQueue;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static dk.ku.di.dms.vms.coordinator.vms.VmsWorker.State.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static java.lang.System.Logger.Level.*;

public final class VmsWorker extends ProducerWorker implements IVmsWorker {

    private static final System.Logger LOGGER = System.getLogger(VmsWorker.class.getName());

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

    public record VmsWorkerOptions(boolean active,
                                   boolean compressing,
                                   boolean logging,
                                   int maxSleep,
                                   int networkBufferSize,
                                   int networkSendTimeout,
                                   int numQueuesVmsWorker,
                                   boolean initHandshake) {}

    private final ServerNode me;
    
    private final VmsWorkerOptions options;

    private State state;

    private final ByteBuffer readBuffer;

    /**
     * Queue to inform coordinator about an important event
     */
    private final Queue<Object> coordinatorQueue;

    private final Supplier<IChannel> channelFactory;
    private IChannel channel;

    // DTs particular to this vms worker
    private final IVmsQueue transactionEventQueue;

    private final Deque<Object> messageQueue;

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final BatchWriteCompletionHandler batchWriteCompletionHandler = new BatchWriteCompletionHandler();

    private interface IVmsQueue {
        void drain(List<TransactionEvent.PayloadRaw> list);
        void queue(TransactionEvent.PayloadRaw payloadRaw);
        default void addAll(List<TransactionEvent.PayloadRaw> list){
            for(var event : list){
                this.queue(event);
            }
        }
    }

    private static final class MultiQueue implements IVmsQueue {
        private final int numQueues;
        private final Deque<TransactionEvent.PayloadRaw>[] queues;
        @SuppressWarnings("unchecked")
        private MultiQueue(int numQueues) {
            this.numQueues = numQueues;
            this.queues = new Deque[numQueues];
            for (int i = 0; i < numQueues; i++) {
                this.queues[i] = new ConcurrentLinkedDeque<>();
            }
        }
        @Override
        public void queue(TransactionEvent.PayloadRaw payloadRaw) {
            // this method can be called concurrently
            int pos = ThreadLocalRandom.current().nextInt(0, this.numQueues);
            this.queues[pos].addLast(payloadRaw);
        }
        @Override
        public void drain(List<TransactionEvent.PayloadRaw> list){
            TransactionEvent.PayloadRaw txEvent;
            int nextPos = 0;
            do {
                while ((txEvent = this.queues[nextPos].pollFirst()) != null){
                    list.add(txEvent);
                }
                nextPos++;
            } while (nextPos < this.numQueues);
        }
    }

    private static final class SingleQueue implements IVmsQueue {
        private final MpscArrayQueue<TransactionEvent.PayloadRaw> queue = new MpscArrayQueue<>(1024*100);
        @Override
        public void queue(TransactionEvent.PayloadRaw payloadRaw) {
            this.queue.offer(payloadRaw);
        }
        @Override
        public void drain(List<TransactionEvent.PayloadRaw> list){
            this.queue.drain(list::add);
        }
    }

    private static final class BlockingSingleQueue implements IVmsQueue {
        private final MpscBlockingConsumerArrayQueue<TransactionEvent.PayloadRaw> queue = new MpscBlockingConsumerArrayQueue<>(1024*100);
        @Override
        public void queue(TransactionEvent.PayloadRaw payloadRaw) {
            this.queue.offer(payloadRaw);
        }
        @Override
        public void drain(List<TransactionEvent.PayloadRaw> list){
            if(this.queue.isEmpty()) {
                try {
                    var obj = this.queue.take();
                    list.add(obj);
                } catch (InterruptedException ignored) { }
            }
            this.queue.drain(list::add);
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
        return new VmsWorker(me, consumerVms, coordinatorQueue, channelFactory, options, serdesProxy);
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
        super("coordinator", consumerVms, serdesProxy, options.logging());
        this.me = me;
        this.state = State.NEW;
        this.channelFactory = channelFactory;
        this.options = options;

        this.readBuffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());

        // in
        this.messageQueue = new ConcurrentLinkedDeque<>();
        if(options.numQueuesVmsWorker() > 1) {
            this.transactionEventQueue = new MultiQueue(options.numQueuesVmsWorker());
        } else {
            this.transactionEventQueue = //new BlockingSingleQueue();
                                        new SingleQueue();
        }

        // out - shared by many vms workers
        this.coordinatorQueue = coordinatorQueue;
    }

    private void connect() throws InterruptedException, ExecutionException {
        this.channel = this.channelFactory.get();
        NetworkUtils.configure(this.channel.getNetworkChannel(), this.options.networkBufferSize);
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
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.log(ERROR, "Leader: Connection attempt to " + this.consumerVms.identifier + " failed. Retrying in "+waitTime/1000+" second(s)...");
                try {
                    Thread.sleep(waitTime);
                    waitTime = waitTime + 1000;
                } catch (InterruptedException ignored) { }
            }
        }

        try {
            LOGGER.log(INFO, "Leader: Sending presentation to "+this.consumerVms.identifier);
            this.state = CONNECTION_ESTABLISHED;
            ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
            this.sendLeaderPresentationToVms(writeBuffer);
            this.state = State.LEADER_PRESENTATION_SENT;

            // set read handler here
            this.channel.read(this.readBuffer, 0, new VmsReadCompletionHandler());
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
        this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
    }

    @Override
    public void run() {
        LOGGER.log(INFO, "VmsWorker starting for consumer VMS: "+this.consumerVms);
        if(this.options.initHandshake()) {
            this.initHandshakeProtocol();
        } else {
            if (this.initSimpleConnection()) return;
        }
        if(this.options.active) {
            if(this.options.compressing){
                this.eventLoopCompressing();
            } else if(this.options.logging) {
                this.eventLoopLogging();
            } else {
                this.eventLoop();
            }
        }
        LOGGER.log(INFO, "VmsWorker finished for consumer VMS: "+this.consumerVms);
    }

    private boolean initSimpleConnection() {
        // only connect. the presentation has already been sent
        LOGGER.log(DEBUG, Thread.currentThread().getName()+": Attempting additional connection to to VMS: " + this.consumerVms.identifier);
        try {
            this.connect();
            ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
            this.sendLeaderPresentationToVms(writeBuffer);
        } catch(Exception ignored){
            LOGGER.log(ERROR, Thread.currentThread().getName()+": Cannot connect to VMS: " + this.consumerVms.identifier);
            return true;
        }
        LOGGER.log(INFO, Thread.currentThread().getName()+": Additional connection to "+this.consumerVms.identifier+" succeeded");
        return false;
    }

    private void eventLoopCompressing() {
        int pollTimeout = 1;
        while (this.isRunning()){
            try {
                this.transactionEventQueue.drain(this.drained);
                if(this.drained.isEmpty()){
                    pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep);
                    this.processPendingNetworkTasks();
                    this.processPendingLogging();
                    this.giveUpCpu(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                if(this.drained.size() == 1){
                    this.sendEvent(this.drained.removeFirst());
                } else if(this.drained.size() < 10){
                    this.sendBatchOfEventsNonBlockingAndLogging();
                } else {
                    this.sendCompressedBatchOfEvents();
                }
                this.processPendingNetworkTasks();
                this.processPendingLogging();
            } catch (Exception e) {
                LOGGER.log(ERROR, "Leader: VMS worker for "+this.consumerVms.identifier+" has caught an exception: \n"+e);
            }
        }
    }

    private void eventLoop() {
        int pollTimeout = 1;
        while (this.isRunning()){
            try {
                this.transactionEventQueue.drain(this.drained);
                if(this.drained.isEmpty()){
                    pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep);
                    this.processPendingNetworkTasks();
                    this.giveUpCpu(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                this.processPendingNetworkTasks();
                this.sendBatchOfEventsNonBlocking();
            } catch (Exception e) {
                LOGGER.log(ERROR, "Leader: VMS worker for "+this.consumerVms.identifier+" has caught an exception: \n"+e);
            }
        }
    }

    /**
     * Event loop performs some tasks:
     * (a) Receive and process batch-tracking messages from coordinator
     * (b) Process transaction input events
     * (c) Logging
     */
    private void eventLoopLogging() {
        int pollTimeout = 1;
        while (this.isRunning()){
            try {
                this.transactionEventQueue.drain(this.drained);
                if(this.drained.isEmpty()){
                    pollTimeout = Math.min(pollTimeout * 2, this.options.maxSleep);
                    this.processPendingNetworkTasks();
                    this.processPendingLogging();
                    this.giveUpCpu(pollTimeout);
                    continue;
                }
                pollTimeout = pollTimeout > 0 ? pollTimeout / 2 : 0;
                this.sendBatchOfEventsNonBlockingAndLogging();
                this.processPendingNetworkTasks();
                this.processPendingLogging();
            } catch (Exception e) {
                LOGGER.log(ERROR, "Leader: VMS worker for "+this.consumerVms.identifier+" has caught an exception: \n"+e);
            }
        }
    }

    private void processPendingLogging(){
        ByteBuffer writeBuffer;
        if((writeBuffer = this.loggingWriteBuffers.poll()) == null) {
            return;
        }
        try {
            writeBuffer.position(0);
            this.loggingHandler.log(writeBuffer);
            returnByteBuffer(writeBuffer);
        } catch (IOException e) {
            LOGGER.log(ERROR, "Error on writing byte buffer to logging file: "+e.getMessage());
            this.loggingWriteBuffers.add(writeBuffer);
        }
    }

    private void processPendingNetworkTasks() {
        Object pendingMessage;
        while((pendingMessage = this.messageQueue.pollFirst()) != null){
            this.sendMessage(pendingMessage);
        }
        if(!this.pendingWritesBuffer.isEmpty() && this.tryAcquireLock()){
            if(this.pendingWritesBuffer.size() == 1) {
                this.sendSinglePendingBuffer();
            } else {
                this.sendMultiplePendingBuffers();
            }
        }
    }

    private void sendMultiplePendingBuffers() {
        ByteBuffer bb;
        ByteBuffer[] srcs = new ByteBuffer[this.pendingWritesBuffer.size()];
        int idx = 0;
        while ((bb = this.pendingWritesBuffer.poll()) != null) {
            srcs[idx] = bb;
            idx++;
            if(idx == srcs.length) break;
        }
        this.channel.write(srcs, 0, srcs, this.multiBufferWriteCompletionHandler);
    }

    private void sendSinglePendingBuffer() {
        ByteBuffer bb = this.pendingWritesBuffer.pollFirst();
        LOGGER.log(DEBUG, "Leader: Sending pending buffer");
        try {
            this.channel.write(bb, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, bb, this.batchWriteCompletionHandler);
        } catch (Exception e){
            LOGGER.log(ERROR, "Leader: ERROR on sending pending buffer: \n"+e);
            if(e instanceof IllegalStateException){
                this.stop();
            }
            this.releaseLock();
            if(bb != null) {
                bb.position(0);
                this.pendingWritesBuffer.offerFirst(bb);
            }
        }
    }

    @Override
    public void queueTransactionEvent(TransactionEvent.PayloadRaw payloadRaw) {
        this.transactionEventQueue.queue(payloadRaw);
    }

    private void sendEvent(TransactionEvent.PayloadRaw payload) {
        ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize());
        TransactionEvent.write(writeBuffer, payload);
        writeBuffer.flip();
        this.acquireLock();
        this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
    }

    @Override
    public void queueMessage(Object message) {
        this.sendMessage(message);
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
            default -> LOGGER.log(WARNING, "Leader: VMS worker for " + this.consumerVms.identifier + " has unknown message type: " + message.getClass().getName());
        }
    }

    private void sendTransactionAbort(TransactionAbort.Payload tidToAbort) {
        ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
        TransactionAbort.write(writeBuffer, tidToAbort);
        writeBuffer.flip();
        this.acquireLock();
        this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
        LOGGER.log(WARNING,"Leader: Transaction abort sent to: " + this.consumerVms.identifier);
    }

    private void sendBatchCommitCommand(BatchCommitCommand.Payload batchCommitCommand) {
        try {
            ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
            BatchCommitCommand.write(writeBuffer, batchCommitCommand);
            writeBuffer.flip();
            if(this.tryAcquireLock()){
                this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            } else {
                if(options.active){
                    messageQueue.offerFirst(batchCommitCommand);
                } else {
                    this.acquireLock();
                    this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
                }
            }
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

    private void sendBatchCommitInfo(BatchCommitInfo.Payload batchCommitInfo){
        // then send only the batch commit info
        LOGGER.log(DEBUG, "Leader: Batch ("+batchCommitInfo.batch()+") commit info will be sent to " + this.consumerVms.identifier);
        try {
            ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
            BatchCommitInfo.write(writeBuffer, batchCommitInfo);
            writeBuffer.flip();
            if(this.tryAcquireLock()){
                this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
            } else {
                if(options.active){
                    messageQueue.offerFirst(batchCommitInfo);
                } else {
                    this.acquireLock();
                    this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
                }
            }
        } catch (Exception e) {
            LOGGER.log(ERROR, "Leader: Error on sending a batch commit info to VMS: " + e.getMessage());
            this.releaseLock();
            this.messageQueue.offerFirst(batchCommitInfo);
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

        ByteBuffer writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
        try {
            ConsumerSet.write(writeBuffer, vmsConsumerSet);
            writeBuffer.flip();
            this.acquireLock();
            this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.writeCompletionHandler);
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

    public final class MultiBufferCompletionHandler implements CompletionHandler<Long, ByteBuffer[]> {
        @Override
        public void completed(Long result, ByteBuffer[] srcs) {
            int offset = -1;
            for (int i = 0; i < srcs.length; i++) {
                if(srcs[i].hasRemaining()){
                    offset = i;
                    break;
                }
            }
            if (offset != -1) {
                channel.write(srcs, offset, srcs,this);
            } else {
                releaseLock();
                for (ByteBuffer src : srcs){
                    returnByteBuffer(src);
                }
            }
        }
        @Override
        public void failed(Throwable exc, ByteBuffer[] srcs) {
            LOGGER.log(ERROR, "Error caught on sending multiple buffers: "+exc);
            releaseLock();
            for (ByteBuffer src : srcs) {
                if(src.hasRemaining()){
                    pendingWritesBuffer.add(src);
                } else {
                    returnByteBuffer(src);
                }
            }
        }
    }

    private final MultiBufferCompletionHandler multiBufferWriteCompletionHandler = new MultiBufferCompletionHandler();

    private void sendBatchOfEventsNonBlocking(){
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        while(remaining > 0) {
            try {
                writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer);
                writeBuffer.flip();
                LOGGER.log(DEBUG, "Leader: Submitting ["+(count - remaining)+"] events to "+consumerVms.identifier);
                count = remaining;
                if(this.tryAcquireLock()){
                    if(this.pendingWritesBuffer.isEmpty()) {
                        this.channel.write(writeBuffer, writeBuffer, this.writeCompletionHandler);
                    } else {
                        ByteBuffer bb;
                        ByteBuffer[] srcs = new ByteBuffer[this.pendingWritesBuffer.size() + 1];
                        srcs[0] = writeBuffer;
                        int idx = 1;
                        while ((bb = this.pendingWritesBuffer.poll()) != null) {
                            srcs[idx] = bb;
                            idx++;
                            if(idx == srcs.length) break;
                        }
                        this.channel.write(srcs, 0, srcs, this.multiBufferWriteCompletionHandler);
                    }
                } else {
                    this.pendingWritesBuffer.addLast(writeBuffer);
                }
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                // force exit loop
                remaining = 0;
            }
        }
        this.drained.clear();
    }

    /**
     * While a write operation is in progress, it must wait for completion and then submit the next write.
     */
    private void sendBatchOfEventsNonBlockingAndLogging(){
        int remaining = this.drained.size();
        int count = remaining;
        ByteBuffer writeBuffer = null;
        boolean lockAcquired = false;
        while(remaining > 0) {
            try {
                writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer);

                LOGGER.log(DEBUG, "Leader: Submitting ["+(count - remaining)+"] events to "+consumerVms.identifier);
                count = remaining;
                writeBuffer.flip();

                // maximize useful work
                while(!this.tryAcquireLock()){
                    this.processPendingLogging();
                }
                lockAcquired = true;
                this.channel.write(writeBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, writeBuffer, this.batchWriteCompletionHandler);
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                // force exit loop
                remaining = 0;
                if(lockAcquired) {
                    this.releaseLock();
                }
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
                channel.write(byteBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, byteBuffer, this);
                return;
            }
            releaseLock();
            returnByteBuffer(byteBuffer);
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            releaseLock();
            returnByteBuffer(byteBuffer);
            LOGGER.log(ERROR, "Leader: ERROR on writing batch of events to "+consumerVms.identifier+": "+exc);
        }
    }

    private final class BatchWriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            LOGGER.log(DEBUG, "Leader: Message with size " + result + " has been sent to: " + consumerVms.identifier);
            if(byteBuffer.hasRemaining()) {
                // keep the lock and send the remaining
                channel.write(byteBuffer, options.networkSendTimeout, TimeUnit.MILLISECONDS, byteBuffer, this);
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
            pendingWritesBuffer.addFirst(byteBuffer);
            LOGGER.log(INFO, "Leader: Byte buffer added to pending queue. #pending: "+ pendingWritesBuffer.size());
        }
    }

    private void failSafe(Exception e, ByteBuffer writeBuffer) {
        LOGGER.log(ERROR, "Leader: Error submitting events to "+this.consumerVms.identifier+"\n"+ e);
        // return non-processed events to original location or what?
        if (!this.channel.isOpen()) {
            LOGGER.log(WARNING, "The "+this.consumerVms.identifier+" VMS is offline");
        }
        // return events to the deque
        this.transactionEventQueue.addAll(this.drained);
        if(writeBuffer != null) {
            returnByteBuffer(writeBuffer);
        }
    }

    private void sendCompressedBatchOfEvents() {
        int remaining = this.drained.size();
        ByteBuffer writeBuffer = null;
        boolean lockAcquired = false;
        while(remaining > 0){
            try {
                writeBuffer = retrieveByteBuffer(this.options.networkBufferSize);
                remaining = BatchUtils.assembleBatchOfEvents(remaining, this.drained, writeBuffer, CUSTOM_HEADER);
                writeBuffer.flip();
                int maxLength = writeBuffer.limit() - CUSTOM_HEADER;
                writeBuffer.position(CUSTOM_HEADER);

                ByteBuffer compressedBuffer = retrieveByteBuffer(this.options.networkBufferSize);
                compressedBuffer.position(CUSTOM_HEADER);
                CompressingUtils.compress(writeBuffer, compressedBuffer);
                compressedBuffer.flip();

                int limit = compressedBuffer.limit();
                compressedBuffer.put(0, COMPRESSED_BATCH_OF_EVENTS);
                compressedBuffer.putInt(1, limit);
                compressedBuffer.putInt(5, writeBuffer.getInt(5));
                compressedBuffer.putInt(9, maxLength);

                writeBuffer.clear();
                returnByteBuffer(writeBuffer);

                // maximize useful work
                lockAcquired = this.tryAcquireLock();
                if(lockAcquired){
                    // check if there is a pending one
                    ByteBuffer pendingBuffer = this.pendingWritesBuffer.pollFirst();
                    if(pendingBuffer != null) {
                        this.channel.write(pendingBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, pendingBuffer, this.writeCompletionHandler);
                        this.pendingWritesBuffer.addLast(compressedBuffer);
                    } else {
                        this.channel.write(compressedBuffer, this.options.networkSendTimeout(), TimeUnit.MILLISECONDS, compressedBuffer, this.writeCompletionHandler);
                    }
                } else {
                    this.pendingWritesBuffer.addLast(compressedBuffer);
                }
            } catch (Exception e) {
                this.failSafe(e, writeBuffer);
                remaining = 0;
                if(lockAcquired) {
                    this.releaseLock();
                }
            }
        }
        this.drained.clear();
    }

}
