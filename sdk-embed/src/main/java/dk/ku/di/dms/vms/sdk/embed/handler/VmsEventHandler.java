package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.compressing.CompressingUtils;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.*;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import dk.ku.di.dms.vms.web_common.ModbHttpServer;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.JdkAsyncChannel;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static java.lang.System.Logger.Level.*;

/**
 * This default event handler connects direct to the coordinator
 * So in this approach it bypasses the sidecar. In this way,
 * the DBMS must also be run within this code.
 * The virtual microservice doesn't know who is the coordinator. It should be passive.
 * The leader and followers must share a list of VMSs.
 * Could also try to adapt to JNI:
 * <a href="https://nachtimwald.com/2017/06/17/calling-java-from-c/">...</a>
 */
public final class VmsEventHandler extends ModbHttpServer {

    private static final System.Logger LOGGER = System.getLogger(VmsEventHandler.class.getName());
    
    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    /** INTERNAL CHANNELS **/
    private final VmsEmbedInternalChannels vmsInternalChannels;

    /** VMS METADATA **/
    private final VmsNode me; // this merges network and semantic data about the vms

    private final VmsRuntimeMetadata vmsMetadata;

    /** EXTERNAL VMSs **/
    private final Map<String, List<IVmsContainer>> eventToConsumersMap;

    // built while connecting to the consumers
    private final Map<IdentifiableNode, IVmsContainer> consumerVmsContainerMap;

    // built dynamically as new producers request connection
    private final Map<Integer, ConnectionMetadata> producerConnectionMetadataMap;

    /** For checkpointing the state */
    private final ITransactionManager transactionManager;

    /** SERIALIZATION & DESERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;

    private final VmsHandlerOptions options;

    private final IHttpHandler httpHandler;
    
    /** COORDINATOR **/
    private ServerNode leader;

    private ConnectionMetadata leaderConnectionMetadata;

    // the thread responsible to send data to the leader
    private LeaderWorker leaderWorker;

    // refer to what operation must be performed
    // private final BlockingQueue<Object> leaderWorkerQueue;

    // cannot be final, may differ across time and new leaders
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final Set<String> queuesLeaderSubscribesTo;

    /** INTERNAL STATE **/

    // metadata about all non-committed batches.
    // when a batch commit finishes, it should be removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    /**
     * It marks how many TIDs that the scheduler has executed.
     * The scheduler is batch-agnostic. That means in order
     * to progress with the batch in the event handler, we need to check if the
     * batch has completed using the number of TIDs executed.
     * It must be separated from batchContextMap due to different timing of batch context sending
     */
    public final Map<Long, BatchMetadata> trackingBatchMap;

    public static final class BatchMetadata {
        public int numberTIDsExecuted;
        public long maxTidExecuted;
    }

    /**
     * It is necessary a way to store the tid received to a
     * corresponding dependence map.
     */
    private final Map<Long, Map<String, Long>> tidToPrecedenceMap;

    public static VmsEventHandler build(// to identify which vms this is
                                        VmsNode me,
                                        // to checkpoint private state
                                        ITransactionManager transactionalHandler,
                                        // for communicating with other components
                                        VmsEmbedInternalChannels vmsInternalChannels,
                                        // metadata about this vms
                                        VmsRuntimeMetadata vmsMetadata,
                                        VmsApplicationOptions options,
                                        IHttpHandler httpHandler,
                                        // serialization/deserialization of objects
                                        IVmsSerdesProxy serdesProxy){
        try {
            return new VmsEventHandler(me, vmsMetadata,
                    transactionalHandler, vmsInternalChannels,
                    new VmsEventHandler.VmsHandlerOptions( options.maxSleep(), options.networkBufferSize(),
                            options.osBufferSize(), options.networkThreadPoolType(), options.networkThreadPoolSize(), options.networkSendTimeout(),
                            options.numVmsWorkers(), true, options.isLogging(), options.isCheckpointing()),
                    httpHandler, serdesProxy);
        } catch (IOException e){
            throw new RuntimeException("Error on setting up event handler: "+e.getCause()+ " "+ e.getMessage());
        }
    }

    public record VmsHandlerOptions(int maxSleep,
                                    int networkBufferSize,
                                    int osBufferSize,
                                    String networkThreadPoolType,
                                    int networkThreadPoolSize,
                                    int networkSendTimeout,
                                    int numVmsWorkers,
                                    boolean compressing,
                                    boolean logging,
                                    boolean checkpointing) {}

    private VmsEventHandler(VmsNode me,
                            VmsRuntimeMetadata vmsMetadata,
                            ITransactionManager transactionManager,
                            VmsEmbedInternalChannels vmsInternalChannels,
                            VmsHandlerOptions options,
                            IHttpHandler httpHandler,
                            IVmsSerdesProxy serdesProxy) throws IOException {
        super();

        // network and executor
        switch (options.networkThreadPoolType){
            case "default" -> this.group = AsynchronousChannelGroup.withFixedThreadPool(
                    options.networkThreadPoolSize > 0 ? options.networkThreadPoolSize : Runtime.getRuntime().availableProcessors(),
                    Thread.ofPlatform().name("vms-network-thread").factory()
            );
            case "vthread" -> this.group = AsynchronousChannelGroup.withFixedThreadPool(
                    options.networkThreadPoolSize > 0 ? options.networkThreadPoolSize : Runtime.getRuntime().availableProcessors(),
                    Thread.ofVirtual().name("vms-network-vthread").factory()
            );
            case null, default -> this.group = null;
        }
        this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        this.serverSocket.bind(me.asInetSocketAddress());

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        // no concurrent threads modifying them
        this.eventToConsumersMap = new HashMap<>();
        this.consumerVmsContainerMap = new HashMap<>();
        // concurrent threads modifying it
        this.producerConnectionMetadataMap = new ConcurrentHashMap<>();

        this.serdesProxy = serdesProxy;

        this.batchContextMap = new ConcurrentHashMap<>();
        this.trackingBatchMap = new ConcurrentHashMap<>();
        this.tidToPrecedenceMap = new ConcurrentHashMap<>();

        this.transactionManager = transactionManager;

        // set leader off at the start
        this.leader = new ServerNode("0.0.0.0",0);
        this.leader.off();

        this.queuesLeaderSubscribesTo = new HashSet<>();

        this.options = options;
        this.httpHandler = httpHandler;
    }

    @Override
    public void run() {
        LOGGER.log(INFO,this.me.identifier+": Event handler has started");
        // setup accept since we need to accept connections from the coordinator and other VMSs
        this.serverSocket.accept(null, new AcceptCompletionHandler());
        LOGGER.log(INFO,this.me.identifier+": Accept handler has been setup");
        LOGGER.log(INFO,this.me.identifier+": Event handler has finished execution.");
    }

    public void processOutputEvent(IVmsTransactionResult txResult) {
        LOGGER.log(DEBUG,this.me.identifier+": New transaction result in event handler. TID = "+ txResult.tid());
        // it is a void method that executed, nothing to send
        if (txResult.getOutboundEventResult().outputQueue() != null) {
            Map<String, Long> precedenceMap = this.tidToPrecedenceMap.get(txResult.tid());
            if (precedenceMap != null) {
                // remove ourselves (which also saves some bytes)
                precedenceMap.remove(this.me.identifier);
                String precedenceMapUpdated = this.serdesProxy.serializeMap(precedenceMap);
                this.processOutputEvent(txResult.getOutboundEventResult(), precedenceMapUpdated);
            } else {
                LOGGER.log(ERROR, this.me.identifier + ": No precedence map found for TID: " + txResult.tid());
            }
        }
        // scheduler can be way ahead of the last batch committed
        this.updateBatchStats(txResult.getOutboundEventResult());
    }

    /**
     * Many outputs from the same transaction may arrive here concurrently,
     * but can only send the batch commit once
     */
    private void updateBatchStats(OutboundEventResult outputEvent) {
        BatchMetadata batchMetadata = this.updateBatchMetadataAtomically(outputEvent);
        // not arrived yet
        if(!this.batchContextMap.containsKey(outputEvent.batch())) return;
        BatchContext thisBatch = this.batchContextMap.get(outputEvent.batch());
        if(thisBatch.numberOfTIDsBatch != batchMetadata.numberTIDsExecuted) {
            return;
        }
        LOGGER.log(INFO, this.me.identifier + ": All TIDs for the batch " + thisBatch.batch + " have been executed");
        thisBatch.setStatus(BatchContext.BATCH_COMPLETED);
        if(this.options.checkpointing()){
            LOGGER.log(INFO, this.me.identifier + ": Requesting checkpoint for batch " + thisBatch.batch);
            submitBackgroundTask(()->checkpoint(thisBatch.batch, batchMetadata.maxTidExecuted));
        }
        // if terminal, must send batch complete
        if (thisBatch.terminal) {
            LOGGER.log(INFO, this.me.identifier + ": Requesting leader worker to send batch " + thisBatch.batch + " complete");
            // must be queued in case leader is off and comes back online
            this.leaderWorker.queueMessage(BatchComplete.of(thisBatch.batch, this.me.identifier));
        }
    }

    private BatchMetadata updateBatchMetadataAtomically(OutboundEventResult outputEvent) {
        return this.trackingBatchMap.compute(outputEvent.batch(),
                (x,y) -> {
                    BatchMetadata toMod = y;
                    if(toMod == null){
                        toMod = new BatchMetadata();
                    }
                    toMod.numberTIDsExecuted += 1;
                    if(toMod.maxTidExecuted < outputEvent.tid()){
                        toMod.maxTidExecuted = outputEvent.tid();
                    }
                    return toMod;
        });
    }

    private void connectToReceivedConsumerSet(Map<String, List<IdentifiableNode>> receivedConsumerVms) {
        Map<IdentifiableNode, List<String>> consumerToEventsMap = new HashMap<>();
        // build an indirect map
        for(Map.Entry<String,List<IdentifiableNode>> entry : receivedConsumerVms.entrySet()) {
            for(IdentifiableNode consumer : entry.getValue()){
                consumerToEventsMap.computeIfAbsent(consumer, (ignored) -> new ArrayList<>()).add(entry.getKey());
            }
        }
        for( Map.Entry<IdentifiableNode,List<String>> consumerEntry : consumerToEventsMap.entrySet() ) {
            // connect to more consumers...
            for(int i = 0; i < this.options.numVmsWorkers; i++){
                this.initConsumerVmsWorker(consumerEntry.getKey(), consumerEntry.getValue(), i);
            }
        }
    }

    private static final boolean INFORM_BATCH_ACK = false;

    private void checkpoint(long batch, long maxTid) {
        //this.batchContextMap.get(batch).setStatus(BatchContext.CHECKPOINTING);
        // of course, I do not need to stop the scheduler on commit
        // I need to make access to the data versions data race free
        // so new transactions get data versions from the version map or the store
        //long initTs = System.currentTimeMillis();
        this.transactionManager.checkpoint(maxTid);
        //LOGGER.log(WARNING, me.identifier+": Checkpointing latency is "+(System.currentTimeMillis()-initTs));
        this.batchContextMap.get(batch).setStatus(BatchContext.BATCH_COMMITTED);
        // it may not be necessary. the leader has already moved on at this point
        if(INFORM_BATCH_ACK) {
            this.leaderWorker.queueMessage(BatchCommitAck.of(batch, this.me.identifier));
        }
    }

    /**
     * It creates the payload to be sent downstream
     * @param outputEvent the event to be sent to the respective consumer vms
     */
    private void processOutputEvent(OutboundEventResult outputEvent, String precedenceMap){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());
        byte[] outputEventBytes = this.serdesProxy.serialize(outputEvent.output(), clazz);
        /*
         * does the leader consume this queue?
        if( this.queuesLeaderSubscribesTo.contains( outputEvent.outputQueue() ) ){
            logger.log(DEBUG,me.identifier+": An output event (queue: "+outputEvent.outputQueue()+") will be queued to leader");
            this.leaderWorkerQueue.add(new LeaderWorker.Message(SEND_EVENT, payload));
        }
        */
        List<IVmsContainer> consumerVMSs = this.eventToConsumersMap.get(outputEvent.outputQueue());
        if(consumerVMSs == null || consumerVMSs.isEmpty()){
            LOGGER.log(DEBUG,this.me.identifier+": An output event (queue: "+outputEvent.outputQueue()+") has no target virtual microservices.");
            return;
        }
        TransactionEvent.PayloadRaw payload = TransactionEvent.of(outputEvent.tid(), outputEvent.batch(), outputEvent.outputQueue(), outputEventBytes, precedenceMap);
        for(IVmsContainer consumerVmsContainer : consumerVMSs) {
            LOGGER.log(DEBUG,this.me.identifier+": An output event (queue: " + outputEvent.outputQueue() + ") will be queued to VMS: " + consumerVmsContainer.identifier());
            consumerVmsContainer.queue(payload);
        }
    }

    public void initConsumerVmsWorker(IdentifiableNode node, List<String> outputEvents, int identifier){
        if(this.producerConnectionMetadataMap.containsKey(node.hashCode())){
            LOGGER.log(WARNING,"The node "+ node.host+" "+ node.port+" already contains a connection as a producer");
        }
        if(this.me.hashCode() == node.hashCode()){
            LOGGER.log(ERROR,this.me.identifier+" is receiving itself as consumer: "+ node.identifier);
            return;
        }
        ConsumerVmsWorker consumerVmsWorker = ConsumerVmsWorker.build(this.me, node,
                        () -> JdkAsyncChannel.create(this.group),
                        this.options,
                        this.serdesProxy);
        Thread.ofPlatform().name("vms-consumer-"+node.identifier+"-"+identifier)
                .inheritInheritableThreadLocals(false)
                .start(consumerVmsWorker);
        if(!this.consumerVmsContainerMap.containsKey(node)){
            if(this.options.numVmsWorkers == 1) {
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            } else {
                MultiVmsContainer multiVmsContainer = new MultiVmsContainer(consumerVmsWorker, node, this.options.numVmsWorkers);
                this.consumerVmsContainerMap.put(node, multiVmsContainer);
            }
            // add to tracked VMSs
            for (String outputEvent : outputEvents) {
                LOGGER.log(INFO,me.identifier+ " adding "+outputEvent+" to consumers map with "+node.identifier);
                this.eventToConsumersMap.computeIfAbsent(outputEvent, (ignored) -> new ArrayList<>());
                this.eventToConsumersMap.get(outputEvent).add(consumerVmsWorker);
            }
        } else {
            IVmsContainer vmsContainer = this.consumerVmsContainerMap.get(node);
            if(vmsContainer instanceof MultiVmsContainer multiVmsContainer){
                multiVmsContainer.addConsumerVms(consumerVmsWorker);
            } else {
                // stop previous, replace by the new one
                ((ConsumerVmsWorker)vmsContainer).stop();
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            }
        }
        // set up read from consumer vms? we read nothing from consumer vms. maybe in the future can negotiate amount of data to avoid performance problems
        // channel.read(buffer, 0, new VmsReadCompletionHandler(this.node, connMetadata, buffer));
    }

    /**
     * The completion handler must execute fast
     */
    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        // the VMS sending events to me
        private final IdentifiableNode node;
        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public VmsReadCompletionHandler(IdentifiableNode node,
                                        ConnectionMetadata connectionMetadata,
                                        ByteBuffer byteBuffer){
            this.node = node;
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = byteBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                // end-of-stream signal, no more data can be read
                LOGGER.log(WARNING,me.identifier+": VMS "+node.identifier+" has disconnected!");
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException ignored) { }
                return;
            }
            if(startPos == 0){
                this.readBuffer.flip();
            }
            byte messageType = this.readBuffer.get();
            switch (messageType) {
                case COMPRESSED_BATCH_OF_EVENTS -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processCompressedBatchOfEvents();
                }
                case BATCH_OF_EVENTS -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processBatchOfEvents(this.readBuffer);
                }
                case EVENT -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processSingleEvent();
                }
                default -> LOGGER.log(ERROR,me.identifier+": Unknown message type "+messageType+" received from: "+node.identifier);
            }
            if(this.readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            // check if we can read an integer
            if(this.readBuffer.remaining() > Integer.BYTES) {
                // size of the batch
                bufferSize = this.readBuffer.getInt();
                // discard message type and size of batch from the total size since it has already been read
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void processSingleEvent() {
            try {
                TransactionEvent.PayloadRaw payload = TransactionEvent.read(this.readBuffer);
                LOGGER.log(DEBUG,me.identifier+": 1 event received from "+node.identifier+"\n"+payload);
                String eventStr = new String(payload.event(), StandardCharsets.UTF_8);
                if (vmsMetadata.queueToEventMap().containsKey(eventStr)) {
                    InboundEvent inboundEvent = buildInboundEventFromVms(payload, eventStr);
                    vmsInternalChannels.transactionInputQueue().add(inboundEvent);
                }
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> inboundEvents = LIST_BUFFER.poll();
            if(inboundEvents == null) inboundEvents = new ArrayList<>(1024);
            try {
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from " + node.identifier);
                TransactionEvent.PayloadRaw payload;
                int i = 0;
                while (i < count) {
                    payload = TransactionEvent.read(readBuffer);
                    LOGGER.log(DEBUG, me.identifier+": Processed TID "+payload.tid());
                    String eventStr = new String(payload.event(), StandardCharsets.UTF_8);
                    if (vmsMetadata.queueToEventMap().containsKey(eventStr)) {
                        InboundEvent inboundEvent = this.buildInboundEventFromVms(payload, eventStr);
                        inboundEvents.add(inboundEvent);
                    }
                    i++;
                }
                if(count != inboundEvents.size()){
                    LOGGER.log(WARNING,me.identifier + ": Batch of [" +count+ "] events != from "+inboundEvents.size()+" that will be pushed to worker " + node.identifier);
                }
                vmsInternalChannels.transactionInputQueue().addAll(inboundEvents);
            } catch(Exception e){
                if (e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading batch: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            } finally {
                inboundEvents.clear();
                LIST_BUFFER.add(inboundEvents);
            }
        }

        private void processCompressedBatchOfEvents() {
            ByteBuffer decompressedBuffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize());
            CompressingUtils.decompress(this.readBuffer, decompressedBuffer);
            decompressedBuffer.flip();
            this.processBatchOfEvents(decompressedBuffer);
            decompressedBuffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(decompressedBuffer);
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Error on reading VMS message from "+node.identifier+"\n"+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }

        private InboundEvent buildInboundEventFromVms(TransactionEvent.PayloadRaw payload, String eventStr){
            Class<?> clazz = vmsMetadata.queueToEventMap().get(eventStr);
            Object input = serdesProxy.deserialize(payload.payload(), clazz);
            return buildInboundEvent(payload, eventStr, clazz, input);
        }
    }

    /**
     * On a connection attempt, it is unknown what is the type of node
     * attempting the connection. We find out after the first read.
     */
    private final class UnknownNodeReadCompletionHandler implements CompletionHandler<Integer, Void> {

        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;

        public UnknownNodeReadCompletionHandler(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        public void completed(Integer result, Void void_) {
            String remoteAddress = "";
            try {
                remoteAddress = channel.getRemoteAddress().toString();
            } catch (IOException ignored) { }
            if(result == 0){
                LOGGER.log(WARNING,me.identifier+": A node ("+remoteAddress+") is trying to connect with an empty message!");
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            } else if(result == -1){
                LOGGER.log(WARNING,me.identifier+": A node ("+remoteAddress+") died before sending the presentation message");
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            }
            // message identifier
            byte messageIdentifier = this.buffer.get(0);
            if(messageIdentifier != PRESENTATION){
                this.buffer.flip();
                String request = StandardCharsets.UTF_8.decode(this.buffer).toString();
                if(isHttpClient(request)){
                    HttpReadCompletionHandler readCompletionHandler = new HttpReadCompletionHandler(
                            new ConnectionMetadata("http_client".hashCode(),
                                    ConnectionMetadata.NodeType.HTTP_CLIENT,
                                    this.channel),
                            this.buffer,
                            MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize),
                            httpHandler);
                    try { NetworkUtils.configure(this.channel, options.osBufferSize()); } catch (IOException ignored) { }
                    readCompletionHandler.process(request);
                } else {
                    LOGGER.log(WARNING, me.identifier + ": A node is trying to connect without a presentation message.\n"+request);
                    this.buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                    try { this.channel.close(); } catch (IOException ignored) { }
                }
                return;
            }
            byte nodeTypeIdentifier = this.buffer.get(1);
            this.buffer.position(2);
            switch (nodeTypeIdentifier) {
                case (Presentation.SERVER_TYPE) -> this.processServerPresentation();
                case (Presentation.VMS_TYPE) -> this.processVmsPresentation();
                default -> this.processUnknownNodeType(nodeTypeIdentifier);
            }
        }

        private void processServerPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a server");
            if(!leader.isActive()) {
                ConnectionFromLeaderProtocol connectionFromLeader = new ConnectionFromLeaderProtocol(this.channel, this.buffer);
                connectionFromLeader.processLeaderPresentation();
            } else {
                // discard include metadata bit
                this.buffer.get();
                ServerNode serverNode = Presentation.readServer(this.buffer);
                // known leader attempting additional connection?
                if(serverNode.asInetSocketAddress().equals(leader.asInetSocketAddress())) {
                    LOGGER.log(INFO, me.identifier + ": Leader requested an additional connection");
                    this.buffer.clear();
                    channel.read(buffer, 0, new LeaderReadCompletionHandler(new ConnectionMetadata(leader.hashCode(), ConnectionMetadata.NodeType.SERVER, channel), buffer));
                } else {
                    try {
                        LOGGER.log(WARNING,"Dropping a connection attempt from a node claiming to be leader");
                         this.channel.close();
                    } catch (Exception ignored) {}
                }
            }
        }

        private void processVmsPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a VMS");

            // then it is a VMS intending to connect due to a data/event
            // that should be delivered to this vms
            VmsNode producerVms = Presentation.readVms(this.buffer, serdesProxy);
            LOGGER.log(INFO, me.identifier+": Producer VMS received:\n"+producerVms);
            this.buffer.clear();

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    producerVms.hashCode(),
                    ConnectionMetadata.NodeType.VMS,
                    this.channel
            );

            // what if a vms is both producer to and consumer from this vms?
            if(consumerVmsContainerMap.containsKey(producerVms)){
                LOGGER.log(WARNING,me.identifier+": The node "+producerVms.host+" "+producerVms.port+" already contains a connection as a consumer");
            }

            // just to keep track whether this
            if(producerConnectionMetadataMap.containsKey(producerVms.hashCode())) {
                LOGGER.log(INFO, me.identifier+": Setting up additional consumption from producer "+producerVms);
            } else {
                producerConnectionMetadataMap.put(producerVms.hashCode(), connMetadata);
                // setup event receiving from this vms
                LOGGER.log(INFO,me.identifier+": Setting up consumption from producer "+producerVms);
            }

            this.channel.read(this.buffer, 0, new VmsReadCompletionHandler(producerVms, connMetadata, this.buffer));
        }

        private void processUnknownNodeType(byte nodeTypeIdentifier) {
            LOGGER.log(WARNING,me.identifier+": Presentation message from unknown source:" + nodeTypeIdentifier);
            this.buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
            try {
                this.channel.close();
            } catch (IOException ignored) { }
        }

        @Override
        public void failed(Throwable exc, Void void_) {
            LOGGER.log(WARNING,"Error on processing presentation message!");
        }
    }

    /**
     * Class is iteratively called by the socket pool threads.
     */
    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {
        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            LOGGER.log(DEBUG,me.identifier+": An unknown host has started a connection attempt.");
            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize);
            try {
                NetworkUtils.configure(channel, options.osBufferSize);
                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                channel.read(buffer, null, new UnknownNodeReadCompletionHandler(channel, buffer));
            } catch(Exception e){
                LOGGER.log(ERROR,me.identifier+": Accept handler caught an exception:\n"+e);
                buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                LOGGER.log(DEBUG,me.identifier+": Accept handler set up again for listening to new connections");
                // continue listening
                serverSocket.accept(null, this);
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            String message = exc.getMessage();
            boolean logError = true;
            if(message == null){
                if (exc.getCause() instanceof ClosedChannelException){
                    message = "Connection is closed";
                } else if ( exc instanceof AsynchronousCloseException || exc.getCause() instanceof AsynchronousCloseException) {
                    message = "Event handler has been stopped?";
                } else {
                    message = "No cause identified";
                }
                LOGGER.log(WARNING, me.identifier + ": Error on accepting connection: " + message);
            } else if(message.equalsIgnoreCase("Too many open files")){
                logError = false;
                System.out.println("Too many open files error was caught. Cannot log the error appropriately.");
            }

            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            } else if(logError) {
                LOGGER.log(WARNING,me.identifier+": Socket is not open anymore. Cannot set up accept again");
            }

        }
    }

    private final class ConnectionFromLeaderProtocol {
        private State state;
        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;
        public final CompletionHandler<Integer, Void> writeCompletionHandler;

        public ConnectionFromLeaderProtocol(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.state = State.PRESENTATION_RECEIVED;
            this.channel = channel;
            this.writeCompletionHandler = new WriteCompletionHandler();
            this.buffer = buffer;
        }

        private enum State {
            PRESENTATION_RECEIVED,
            PRESENTATION_PROCESSED,
            PRESENTATION_SENT
        }

        private final class WriteCompletionHandler implements CompletionHandler<Integer,Void> {

            @Override
            public void completed(Integer result, Void attachment) {
                state = State.PRESENTATION_SENT;
                LOGGER.log(INFO,me.identifier+": Message sent to Leader successfully = "+state);
                // set up leader worker
                leaderWorker = new LeaderWorker(me, leader,
                        leaderConnectionMetadata.channel,
                        MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize));
                LOGGER.log(INFO,me.identifier+": Leader worker set up");
                buffer.clear();
                channel.read(buffer, 0, new LeaderReadCompletionHandler(leaderConnectionMetadata, buffer) );
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                LOGGER.log(INFO,me.identifier+": Failed to send presentation to Leader");
                buffer.clear();
                if(!channel.isOpen()) {
                    leaderWorker.stop();
                    leader.off();
                }
                // else what to do try again? no, let the new leader connect
            }
        }

        public void processLeaderPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing the Leader presentation");
            boolean includeMetadata = this.buffer.get() == Presentation.YES;
            // leader has disconnected, or new leader
            leader = Presentation.readServer(this.buffer);
            // read queues leader is interested
            boolean hasQueuesToSubscribe = this.buffer.get() == Presentation.YES;
            if(hasQueuesToSubscribe){
                queuesLeaderSubscribesTo.addAll(Presentation.readQueuesToSubscribeTo(this.buffer, serdesProxy));
            }
            // only connects to all VMSs on first leader connection
            if(leaderConnectionMetadata != null) {
                // considering the leader has replicated the metadata before failing
                // so no need to send metadata again. but it may be necessary...
                // what if the tid and batch id is necessary. the replica may not be
                // sync with last leader...
                LOGGER.log(WARNING, me.identifier+": Updating leader connection metadata due to new connection");
            }
            leaderConnectionMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    channel
            );
            leader.on();
            this.buffer.clear();
            if(includeMetadata) {
                String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                String vmsInputEventSchemaStr = serdesProxy.serializeEventSchema(me.inputEventSchema);
                String vmsOutputEventSchemaStr = serdesProxy.serializeEventSchema(me.outputEventSchema);
                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch, vmsDataSchemaStr, vmsInputEventSchemaStr, vmsOutputEventSchemaStr);
                // the protocol requires the leader to wait for the metadata in order to start sending messages
            } else {
                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch);
            }
            this.buffer.flip();
            this.state = State.PRESENTATION_PROCESSED;
            LOGGER.log(INFO,me.identifier+": Message successfully received from the Leader  = "+state);
            this.channel.write( this.buffer, null, this.writeCompletionHandler );
        }
    }

    private static final ConcurrentLinkedDeque<List<InboundEvent>> LIST_BUFFER = new ConcurrentLinkedDeque<>();

    private final class LeaderReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public LeaderReadCompletionHandler(ConnectionMetadata connectionMetadata, ByteBuffer readBuffer){
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                LOGGER.log(INFO,me.identifier+": Leader has disconnected");
                leader.off();
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException e) {
                    e.printStackTrace(System.out);
                }
                return;
            }
            if(startPos == 0){
                // sets the position to 0 and sets the limit to the current position
                this.readBuffer.flip();
                LOGGER.log(DEBUG,me.identifier+": Leader has sent "+this.readBuffer.limit()+" bytes");
            }
            // guaranteed we always have at least one byte to read
            byte messageType = this.readBuffer.get();
            try {
                switch (messageType) {
                    case (BATCH_OF_EVENTS) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processBatchOfEvents(this.readBuffer);
                    }
                    case (EVENT) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processSingleEvent(readBuffer);
                    }
                    case (BATCH_COMMIT_INFO) -> {
                        if(this.readBuffer.remaining() < (BatchCommitInfo.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // events of this batch from VMSs may arrive before the batch commit info
                        // it means this VMS is a terminal node for the batch
                        BatchCommitInfo.Payload bPayload = BatchCommitInfo.read(this.readBuffer);
                        LOGGER.log(INFO, me.identifier + ": Batch (" + bPayload.batch() + ") commit info received from the leader");
                        this.processNewBatchInfo(bPayload);
                    }
                    case (BATCH_COMMIT_COMMAND) -> {
                        if(this.readBuffer.remaining() < (BatchCommitCommand.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // a batch commit queue from next batch can arrive before this vms moves next? yes
                        BatchCommitCommand.Payload payload = BatchCommitCommand.read(this.readBuffer);
                        LOGGER.log(DEBUG, me.identifier + ": Batch (" + payload.batch() + ") commit command received from the leader");
                        this.processNewBatchCommand(payload);
                    }
                    case (TX_ABORT) -> {
                        if(this.readBuffer.remaining() < (TransactionAbort.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        TransactionAbort.Payload txAbortPayload = TransactionAbort.read(this.readBuffer);
                        LOGGER.log(WARNING, "Transaction (" + txAbortPayload.batch() + ") abort received from the leader?");
                        vmsInternalChannels.transactionAbortInputQueue().add(txAbortPayload);
                    }
                    case (BATCH_ABORT_REQUEST) -> {
                        if(this.readBuffer.remaining() < (BatchAbortRequest.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        // some new leader request to roll back to last batch commit
                        BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read(this.readBuffer);
                        LOGGER.log(WARNING, "Batch (" + batchAbortReq.batch() + ") abort received from the leader");
                        // vmsInternalChannels.batchAbortQueue().add(batchAbortReq);
                    }
                    case (CONSUMER_SET) -> {
                        try {
                            LOGGER.log(INFO, me.identifier + ": Consumer set received from the leader");
                            Map<String, List<IdentifiableNode>> receivedConsumerVms = ConsumerSet.read(this.readBuffer, serdesProxy);
                            if (!receivedConsumerVms.isEmpty()) {
                                connectToReceivedConsumerSet(receivedConsumerVms);
                            } else {
                                LOGGER.log(WARNING, me.identifier + ": Consumer set is empty");
                            }
                        } catch (IOException e) {
                            LOGGER.log(ERROR, me.identifier + ": IOException while reading consumer set: " + e);
                            e.printStackTrace(System.out);
                        }
                    }
                    case (PRESENTATION) ->
                            LOGGER.log(WARNING, me.identifier + ": Presentation being sent again by the leader!?");
                    default ->
                            LOGGER.log(ERROR, me.identifier + ": Message type sent by the leader cannot be identified: " + messageType);
                }
            } catch (Exception e){
                LOGGER.log(ERROR, "Leader: Error caught\n"+e.getMessage(), e);
                e.printStackTrace(System.out);
            }

            if(this.readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            // check if we can read an integer
            if(this.readBuffer.remaining() > Integer.BYTES) {
                // size of the batch
                bufferSize = this.readBuffer.getInt();
                // discard message type and size of batch from the total size since it has already been read
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        /**
         * This method should be called only when strictly necessary to complete a read
         * Otherwise there would be an overhead due to the many I/Os
         */
        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            // get the rest of the batch
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            // set up another read for cases of bursts of data
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> payloads = LIST_BUFFER.poll();
            if(payloads == null) payloads = new ArrayList<>(1024);
            /*
             * Given a new batch of events sent by the leader, the last message is the batch info
             */
            TransactionEvent.PayloadRaw payload;
            try {
                // to increase performance, one would buffer this buffer for processing and then read from another buffer
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from the leader");
                // extract events batched
                for (int i = 0; i < count; i++) {
                    payload = TransactionEvent.read(readBuffer);
                    String eventStr = new String(payload.event(), StandardCharsets.UTF_8);
                    if (vmsMetadata.queueToEventMap().containsKey(eventStr)) {
                        payloads.add(buildInboundEventFromLeader(payload, eventStr));
                        continue;
                    }
                    LOGGER.log(WARNING,me.identifier + ": queue not identified for event received from the leader \n"+payload);
                }
                vmsInternalChannels.transactionInputQueue().addAll(payloads);
            } catch (Exception e){
                LOGGER.log(ERROR, me.identifier +": Error while processing a batch\n"+e);
                e.printStackTrace(System.out);
                if(e instanceof BufferUnderflowException) {
                    throw new RuntimeException(e);
                }
            } finally {
                payloads.clear();
                LIST_BUFFER.add(payloads);
            }
        }

        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.PayloadRaw payload = TransactionEvent.read(readBuffer);
                LOGGER.log(DEBUG,me.identifier + ": 1 event received from the leader \n"+payload);
                // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                String eventStr = new String(payload.event(), StandardCharsets.UTF_8);
                if (vmsMetadata.queueToEventMap().containsKey(eventStr)) {
                    InboundEvent event = this.buildInboundEventFromLeader(payload, eventStr);
                    vmsInternalChannels.transactionInputQueue().add(event);
                    return;
                }
                LOGGER.log(WARNING,me.identifier + ": queue not identified for event received from the leader \n"+payload);
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private InboundEvent buildInboundEventFromLeader(TransactionEvent.PayloadRaw payload, String eventStr){
            Class<?> clazz = vmsMetadata.queueToEventMap().get(eventStr);
            String payloadStr = new String(payload.payload(), StandardCharsets.UTF_8);
            Object input = serdesProxy.deserialize(payloadStr, clazz);
            return buildInboundEvent(payload, eventStr, clazz, input);
        }

        private void processNewBatchInfo(BatchCommitInfo.Payload batchCommitInfo){
            BatchContext batchContext = BatchContext.build(batchCommitInfo);
            batchContextMap.put(batchCommitInfo.batch(), batchContext);
            // if it has been completed but not moved to status, then should send
            if(trackingBatchMap.containsKey(batchCommitInfo.batch())
                    && trackingBatchMap.get(batchCommitInfo.batch()).numberTIDsExecuted == batchCommitInfo.numberOfTIDsBatch()){
                LOGGER.log(INFO,me.identifier+": Requesting leader worker to send batch ("+batchCommitInfo.batch()+") complete (LATE)");
                leaderWorker.queueMessage(BatchComplete.of(batchCommitInfo.batch(), me.identifier));
            }
        }

        /**
         * Context of execution of this method:
         * This is not a terminal node in this batch
         */
        private void processNewBatchCommand(BatchCommitCommand.Payload batchCommitCommand){
            BatchContext batchContext = BatchContext.build(batchCommitCommand);
            batchContextMap.put(batchCommitCommand.batch(), batchContext);
            LOGGER.log(INFO,me.identifier+": Batch command received from leader for batch ("+ batchCommitCommand.batch()+")");
            if(!trackingBatchMap.containsKey(batchCommitCommand.batch())){
                LOGGER.log(WARNING,me.identifier+": Cannot find tracking of batch "+ batchCommitCommand.batch());
                return;
            }
            BatchMetadata batchMetadata = trackingBatchMap.get(batchCommitCommand.batch());
            if(batchContext.numberOfTIDsBatch != batchMetadata.numberTIDsExecuted) {
                LOGGER.log(INFO,me.identifier+": Batch "+ batchCommitCommand.batch()+" has not yet finished!");
                return;
            }
            LOGGER.log(INFO, me.identifier + ": All TIDs for the batch " + batchCommitCommand.batch() + " have been executed");
            batchContext.setStatus(BatchContext.BATCH_COMPLETED);
            if(options.checkpointing()){
                LOGGER.log(INFO, me.identifier + ": Requesting checkpoint for batch " + batchCommitCommand.batch());
                submitBackgroundTask(()->checkpoint(batchCommitCommand.batch(), batchMetadata.maxTidExecuted));
            }
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Message could not be processed: "+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }
    }

    /**
     * Precedence map could also be serialized by message pack, but leave like this for now
     */
    private InboundEvent buildInboundEvent(TransactionEvent.PayloadRaw payload, String eventStr, Class<?> clazz, Object input) {
        String precedenceMapStr = new String(payload.precedenceMap(), StandardCharsets.UTF_8);
        Map<String, Long> precedenceMap = this.serdesProxy.deserializeMap(precedenceMapStr);
        if(precedenceMap == null){
            throw new IllegalStateException("Precedence map is null.");
        }
        if(!precedenceMap.containsKey(this.me.identifier)){
            throw new IllegalStateException("Precedent tid of "+payload.tid()+" is unknown.");
        }
        this.tidToPrecedenceMap.put(payload.tid(), precedenceMap);
        return new InboundEvent(payload.tid(), precedenceMap.get(this.me.identifier), payload.batch(), eventStr, clazz, input);
    }

}
