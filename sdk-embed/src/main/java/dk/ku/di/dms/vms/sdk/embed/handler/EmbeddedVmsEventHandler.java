package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.control.ConsumerContext;
import dk.ku.di.dms.vms.modb.common.schema.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.modb.transaction.api.CheckpointingAPI;
import dk.ku.di.dms.vms.modb.common.transaction.api.ReplicationAPI;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.ISchedulerHandler;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.ingest.BulkDataLoader;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

import static dk.ku.di.dms.vms.modb.common.schema.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.control.Presentation.*;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * This default event handler connects direct to the coordinator
 * So in this approach it bypasses the sidecar. In this way,
 * the DBMS must also be run within this code.
 * -
 * The virtual microservice don't know who is the coordinator. It should be passive.
 * The leader and followers must share a list of VMSs.
 * Could also try to adapt to JNI:
 * <a href="https://nachtimwald.com/2017/06/17/calling-java-from-c/">...</a>
 */
public final class EmbeddedVmsEventHandler extends SignalingStoppableRunnable {

    static final int DEFAULT_DELAY_FOR_BATCH_SEND = 1000;

    private final ExecutorService executorService;

    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    /** INTERNAL CHANNELS **/
    private final IVmsInternalChannels vmsInternalChannels;

    /** VMS METADATA **/
    private final VmsNode me; // this merges network and semantic data about the vms
    private final VmsRuntimeMetadata vmsMetadata;

    /** EXTERNAL VMSs **/
    private final Map<String, Set<ConsumerVms>> eventToConsumersMap;

    // consumers
    // private final Set<ConsumerVms> consumersMap;

    /** For checkpointing the state */
    private final CheckpointingAPI checkpointingAPI;

    /** For updating replication configuration */
    private final ReplicationAPI replicationAPI;

    /** SERIALIZATION & DESERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;

    /** COORDINATOR **/

    // the thread responsible to send data to the leader
    private LeaderWorker leaderWorker;

    // cannot be final, may differ across time and new leaders
    private final Set<String> queuesLeaderSubscribesTo;

    // refer to what operations must be performed by the leader worker
    private final BlockingQueue<LeaderWorker.Message> leaderWorkerQueue;

    // set of events to send to leader
    public final BlockingDeque<TransactionEvent.Payload> eventsToSendToLeader;

    /** INTERNAL STATE **/

    /**
     * When is the current batch updated to the next?
     * - When the last tid of this batch (for this VMS) finishes execution,
     *   if this VMS is a terminal in this batch, send the batch complete event to leader
     *   if this vms is not a terminal, must wait for a batch commit request from leader
     *   -- but this wait can entail low throughput (rethink that later)
     */
    private BatchContext currentBatch;

    /**
     * It just marks the last tid that the scheduler has executed.
     * The scheduler is batch-agnostic. That means in order
     * to progress with the batch here, we need to check if the
     * batch has completed using the last tid executed.
     */
    private long lastTidFinished;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    private final Map<Long, Long> batchToNextBatchMap;

    // basically allows inserting behavior in the underlying vms task scheduler, such as checkpointing
    public final ISchedulerHandler checkpointHandler;

    /*
     * It is necessary a way to store the tid received to a corresponding dependence map.
     */
    private final Map<Long, Map<String, Long>> tidToPrecedenceMap;

    public static EmbeddedVmsEventHandler buildWithDefaults(// to identify which vms this is
                                                            VmsNode me,
                                                            // map event to VMSs
                                                            Map<String, Set<ConsumerVms>> eventToConsumersMap,
                                                            // to checkpoint private state
                                                            CheckpointingAPI checkpointingAPI,
                                                            // management of replication config
                                                            ReplicationAPI replicationAPI,
                                                            // for communicating with other components
                                                            IVmsInternalChannels vmsInternalChannels,
                                                            // metadata about this vms
                                                            VmsRuntimeMetadata vmsMetadata,
                                                            // serialization/deserialization of objects
                                                            IVmsSerdesProxy serdesProxy,
                                                            // for recurrent and continuous tasks
                                                            ExecutorService executorService) throws Exception {
        try {
            return new EmbeddedVmsEventHandler(me, vmsMetadata,
                    eventToConsumersMap == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>( eventToConsumersMap ),
                    checkpointingAPI, replicationAPI, vmsInternalChannels, serdesProxy, executorService);
        } catch (IOException e){
            throw new Exception("Error on setting up event handler: "+e.getCause()+ " "+ e.getMessage());
        }
    }

    private EmbeddedVmsEventHandler(VmsNode me,
                                    VmsRuntimeMetadata vmsMetadata,
                                    Map<String, Set<ConsumerVms>> eventToConsumersMap,
                                    CheckpointingAPI checkpointingAPI,
                                    ReplicationAPI replicationAPI,
                                    IVmsInternalChannels vmsInternalChannels,
                                    IVmsSerdesProxy serdesProxy,
                                    ExecutorService executorService) throws IOException {
        super();

        // network and executor
        this.group = AsynchronousChannelGroup.withThreadPool(executorService);
        this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        this.serverSocket.bind(me.asInetSocketAddress());

        this.executorService = executorService;

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        this.eventToConsumersMap = eventToConsumersMap;

        this.checkpointingAPI = checkpointingAPI;
        this.replicationAPI = replicationAPI;

        this.serdesProxy = serdesProxy;

        this.currentBatch = BatchContext.build(me.batch, me.previousBatch, me.lastTidOfBatch);
        this.lastTidFinished = me.lastTidOfBatch;
        this.currentBatch.setStatus(BatchContext.Status.BATCH_COMMITTED);
        this.batchContextMap = new ConcurrentHashMap<>(3);
        this.batchToNextBatchMap = new ConcurrentHashMap<>();
        this.tidToPrecedenceMap = new ConcurrentHashMap<>();

        // scheduler decorators
        this.checkpointHandler = new CheckpointHandler();

        this.queuesLeaderSubscribesTo = new ConcurrentSkipListSet<>();
        this.leaderWorkerQueue = new LinkedBlockingDeque<>();
        this.eventsToSendToLeader = new LinkedBlockingDeque<>();
    }

    /**
     * A thread that basically writes events to other VMSs and the Leader
     * Retrieves data from all output queues
     * -
     * All output queues must be read in order to send their data
     * -
     * A batch strategy for sending would involve sleeping until the next timeout for batch,
     * send and set up the next. Do that iteratively
     */

    private void eventLoop(){

        this.logger.info("Event handler has started running.");

        this.connectToStarterConsumers();

        // setup accept since we need to accept connections from the coordinator and other VMSs
        this.serverSocket.accept( null, new AcceptCompletionHandler());
        this.logger.info("Accept handler has been setup.");

        while(this.isRunning()){

            try {

//                if(!vmsInternalChannels.transactionAbortOutputQueue().isEmpty()){
//                    // TODO handle. if this can be handled by leader worker
                      //  this thread can wait on the transactionOutputQueue
//                }

                // it is better to get all the results of a given transaction instead of one by one. it must be atomic anyway

                // TODO poll for some time

                if(!this.vmsInternalChannels.transactionOutputQueue().isEmpty()){

                    VmsTransactionResult txResult = this.vmsInternalChannels.transactionOutputQueue().take();

                    this.logger.info("New transaction result in event handler. TID = "+txResult.tid);

                    this.lastTidFinished = txResult.tid;

                    Map<String, Long> precedenceMap = this.tidToPrecedenceMap.get(this.lastTidFinished);
                    // remove ourselves
                    precedenceMap.remove(this.me.name);

                    String precedenceMapUpdated = this.serdesProxy.serializeMap(precedenceMap);

                    // just send events to appropriate targets
                    for(OutboundEventResult outputEvent : txResult.resultTasks){
                        this.processOutputEvent(outputEvent, precedenceMapUpdated);
                    }

                }

               this.moveBatchIfNecessary();

            } catch (Exception e) {
                this.logger.error("Problem on handling event on event handler:"+e.getMessage());
            }

        }

        this.failSafeClose();
        this.logger.info("Event handler has finished execution.");

    }

    private void connectToStarterConsumers() {
        if(this.eventToConsumersMap.isEmpty()) {
            return;
        }

        // then it is received from constructor, and we must initially contact them
        Map<ConsumerVms, List<String>> consumerToEventsMap = new HashMap<>();
        // build an indirect map
        for(Map.Entry<String, Set<ConsumerVms>> entry : this.eventToConsumersMap.entrySet()) {
            for(ConsumerVms consumer : entry.getValue()){
                consumerToEventsMap.computeIfAbsent(consumer, x -> new ArrayList<>()).add(entry.getKey());
            }
        }
        for( var consumerEntry : consumerToEventsMap.entrySet() ) {
            this.connectToConsumerVms( consumerEntry.getKey(), consumerEntry.getValue() );
        }

    }

    private void connectToReceivedConsumerSet( Map<String, List<NetworkAddress>> receivedConsumerVms ) {
        Map<NetworkAddress, List<String>> consumerToEventsMap = new HashMap<>();
        // build an indirect map
        for(Map.Entry<String, List<NetworkAddress>> entry : receivedConsumerVms.entrySet()) {
            for(NetworkAddress consumer : entry.getValue()){
                consumerToEventsMap.computeIfAbsent(consumer, x -> new ArrayList<>()).add(entry.getKey());
            }
        }
        for( var consumerEntry : consumerToEventsMap.entrySet() ) {
            this.connectToConsumerVms( consumerEntry.getKey(), consumerEntry.getValue()  );
        }
    }

    private static final Object DUMB_OBJECT = new Object();

    /**
     * it may be the case that, due to an abort of the last tid, the last tid changes
     * the current code is not incorporating that
     */
    private void moveBatchIfNecessary(){

        // update if current batch is done AND the next batch has already arrived
        if(this.currentBatch.isCommitted() && this.batchToNextBatchMap.get( this.currentBatch.batch ) != null){
            long nextBatchOffset = this.batchToNextBatchMap.get( this.currentBatch.batch );
            this.currentBatch = batchContextMap.get(nextBatchOffset);
        }

        // have we processed all the TIDs of this batch?
        if(this.currentBatch.isOpen() && this.currentBatch.lastTid == this.lastTidFinished){
            // we need to alert the scheduler...
            this.logger.info("The last TID for the current batch has arrived. Time to inform the coordinator about the completion if I am a terminal node.");

            // many outputs from the same transaction may arrive here, but can only send the batch commit once
            this.currentBatch.setStatus(BatchContext.Status.BATCH_EXECUTION_COMPLETED);

            // if terminal, must send batch complete
            if(this.currentBatch.terminal) {
                // must be queued in case leader is off and comes back online
                this.leaderWorkerQueue.add(new LeaderWorker.Message(LeaderWorker.Command.SEND_BATCH_COMPLETE,
                        BatchComplete.of(this.currentBatch.batch, this.me.name)));
            }
            // triggers decorator inside scheduler
            this.vmsInternalChannels.batchCommitCommandQueue().add( DUMB_OBJECT );
        }

    }

    /**
     * From <a href="https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html">...</a>
     * "The Java runtime automatically closes the input and output streams, the client socket,
     * and the server socket because they have been created in the try-with-resources statement."
     * Which means that different tests must bind to different addresses
     */
    private void failSafeClose(){
        // safe close
        try { if(this.serverSocket.isOpen()) this.serverSocket.close(); } catch (IOException ignored) {
            logger.warn("Could not close socket");
        }
    }

    @Override
    public void run() {
        this.eventLoop();
    }

    private final class CheckpointHandler implements ISchedulerHandler {
        @Override
        public Future<?> run() {
            vmsInternalChannels.batchCommitCommandQueue().remove();
            return executorService.submit(EmbeddedVmsEventHandler.this::log);
        }
        @Override
        public boolean conditionHolds() {
            return !vmsInternalChannels.batchCommitCommandQueue().isEmpty();
        }
    }

    private void log() {
        this.currentBatch.setStatus(BatchContext.Status.LOGGING);
        // of course, I do not need to stop the scheduler on commit
        // I need to make access to the data versions data race free
        // so new transactions get data versions from the version map or the store
        this.checkpointingAPI.checkpoint();

        this.currentBatch.setStatus(BatchContext.Status.BATCH_COMMITTED);
        this.leaderWorkerQueue.add( new LeaderWorker.Message( LeaderWorker.Command.SEND_BATCH_COMMIT_ACK,
                BatchCommitAck.of(this.currentBatch.batch, this.me.name) ) );
    }

    /**
     * It creates the payload to be sent downstream.
     * This task can be potentially parallelized without compromising safety.
     * @param outputEvent the event to be sent to the respective consumer vms
     */
    private void processOutputEvent(OutboundEventResult outputEvent, String precedenceMap){

        // TODO however, if write transaction, may need to send updates
        if(outputEvent.outputQueue() == null) return; // it is a void method that executed, nothing to send

        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());
        String objStr = this.serdesProxy.serialize(outputEvent.output(), clazz);

        // does the leader consumes this queue?
        if( this.queuesLeaderSubscribesTo.contains( outputEvent.outputQueue() ) ){
            this.logger.info("An output event (queue: "+outputEvent.outputQueue()+") will be queued to leader");
            TransactionEvent.Payload payloadLeader = TransactionEvent.Payload.of(
                    outputEvent.tid(), outputEvent.batch(), outputEvent.outputQueue(), objStr);
            this.eventsToSendToLeader.add(payloadLeader);
        }

        Set<ConsumerVms> consumerVMSs = this.eventToConsumersMap.get(outputEvent.outputQueue());
        if(consumerVMSs == null || consumerVMSs.isEmpty()){
            this.logger.warn("An output event (queue: "
                    +outputEvent.outputQueue()+") has no target virtual microservices.");
            return;
        }

        for(ConsumerVms consumerVms : consumerVMSs) {
            this.logger.info("An output event (queue: " + outputEvent.outputQueue() + ") will be queued to vms: " + consumerVms);

            // should replicate updates to this consumer vms?
            //  if so, append to the end of the payload the changes then
            if(!consumerVms.subscribedTables.isEmpty()){

                Map<String, String> tableToUpdatesMap = new HashMap<>();
                for(var entry : outputEvent.updates().entrySet()){
                    if (!consumerVms.subscribedTables.contains( entry.getKey() )) continue;
                    String updatesSerialized = this.serdesProxy.serializeList( entry.getValue() );
                    tableToUpdatesMap.put(entry.getKey(), updatesSerialized);
                }

                String serializedMapOfUpdates = this.serdesProxy.serializeMap( tableToUpdatesMap );

                TransactionEvent.Payload payload = TransactionEvent.Payload.of(
                        outputEvent.tid(), outputEvent.batch(), outputEvent.outputQueue(),
                        objStr, precedenceMap, serializedMapOfUpdates );

                // concurrency issue if add to a list. consumer vms worker is being spawned concurrently
                consumerVms.transactionEventsPerBatch.computeIfAbsent(outputEvent.batch(), (x) -> new LinkedBlockingDeque<>()).add(payload);

            } else {
                // right now just including the original precedence map. ideally we must reduce the size by removing this vms
                TransactionEvent.Payload payload = TransactionEvent.Payload.of(
                        outputEvent.tid(), outputEvent.batch(), outputEvent.outputQueue(), objStr, precedenceMap );

                // concurrency issue if add to a list. consumer vms worker is being spawned concurrently
                consumerVms.transactionEventsPerBatch.computeIfAbsent(outputEvent.batch(), (x) -> new LinkedBlockingDeque<>()).add(payload);
            }

        }
    }

    /**
     * Responsible for making sure the handshake protocol
     * is successfully performed with a consumer VMS
     */
    private final class ConnectToConsumerVmsProtocol {

        private State state;
        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;
        public final CompletionHandler<Void, ConnectToConsumerVmsProtocol> connectCompletionHandler;
        private final NetworkAddress address;

        private final List<String> outputEvents;

        public ConnectToConsumerVmsProtocol(AsynchronousSocketChannel channel, List<String> outputEvents, NetworkAddress address) {
            this.state = State.NEW;
            this.channel = channel;
            this.connectCompletionHandler = new ConnectToVmsCompletionHandler();
            this.buffer = MemoryManager.getTemporaryDirectBuffer();
            this.outputEvents = outputEvents;
            this.address = address;
        }

        private enum State { NEW, CONNECTED, PRESENTATION_SENT }

        private class ConnectToVmsCompletionHandler implements CompletionHandler<Void, ConnectToConsumerVmsProtocol> {

            @Override
            public void completed(Void result, ConnectToConsumerVmsProtocol attachment) {

                attachment.state = State.CONNECTED;

                ConnectionMetadata connMetadata = new ConnectionMetadata(attachment.buffer, channel);

                String dataSchema = serdesProxy.serializeDataSchema(me.tableSchema);
                String inputEventSchema = serdesProxy.serializeEventSchema(me.inputEventSchema);
                String outputEventSchema = serdesProxy.serializeEventSchema(me.outputEventSchema);

                attachment.buffer.clear();
                Presentation.writeVms( attachment.buffer, me, me.name, me.batch, me.lastTidOfBatch, me.previousBatch, dataSchema, inputEventSchema, outputEventSchema );
                attachment.buffer.flip();

                // have to make sure we send the presentation before writing to this VMS, otherwise an exception can occur (two writers)
                attachment.channel.write(attachment.buffer, attachment, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ConnectToConsumerVmsProtocol attachment) {
                        attachment.state = State.PRESENTATION_SENT;
                        attachment.buffer.clear();

                        logger.info("Setting up VMS worker ");

                        // if the instance comes from constructor, the object is already created
                        if(address instanceof ConsumerVms consumerVms) {
                            // just set up the timer
                            consumerVms.timer = new Timer("vms-sender-timer", true);
                            consumerVms.timer.scheduleAtFixedRate(new ConsumerVmsWorker(consumerVms, attachment.channel, connMetadata.buffer()), DEFAULT_DELAY_FOR_BATCH_SEND, DEFAULT_DELAY_FOR_BATCH_SEND);
                        } else {
                            // set up event sender timer task
                            ConsumerVms consumerVms = new ConsumerVms(address, new Timer("vms-sender-timer", true));
                            consumerVms.timer.scheduleAtFixedRate(new ConsumerVmsWorker(consumerVms, attachment.channel, connMetadata.buffer()), DEFAULT_DELAY_FOR_BATCH_SEND, DEFAULT_DELAY_FOR_BATCH_SEND);

                            // add to tracked VMSs...
                            for (String outputEvent : outputEvents) {
                                eventToConsumersMap.computeIfAbsent(outputEvent, (x) -> new HashSet<>()).add(consumerVms);
                            }
                        }

                        // why setting the reader from consumer? are we consuming something from the consumer?
                        // maybe in the future to adjust submission rate?
                        attachment.channel.read(attachment.buffer, connMetadata, new VmsReadCompletionHandler());
                    }

                    @Override
                    public void failed(Throwable exc, ConnectToConsumerVmsProtocol attachment) {
                        // check if connection is still online. if so, try again
                        // otherwise, retry connection in a few minutes
                        logger.error("Error connecting to consumer VMS: "+exc.getMessage());
                        attachment.buffer.clear();
                    }
                });

            }

            @Override
            public void failed(Throwable exc, ConnectToConsumerVmsProtocol attachment) {
                // queue for later attempt
                // perhaps can use scheduled task
                logger.error("Cannot connect to consumer VMS: "+exc.getMessage());
                // node.off(); no need it is already off
            }
        }

    }

    private void connectToConsumerVms(NetworkAddress vms, List<String> outputEvents) {
        try(AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group)) {
            channel.setOption(TCP_NODELAY, true);
            channel.setOption(SO_KEEPALIVE, true);
            ConnectToConsumerVmsProtocol protocol = new ConnectToConsumerVmsProtocol(channel, outputEvents, vms);
            channel.connect(vms.asInetSocketAddress(), protocol, protocol.connectCompletionHandler);
        } catch (IOException ignored) {
            this.logger.error("Cannot connect to consumer VMS: "+vms);
        }
    }

    /**
     * Should we only submit the events from the current batch?
     */
    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {
            byte messageType = connectionMetadata.buffer().get(0);
            switch (messageType) {
                case (BATCH_OF_TRANSACTION_EVENTS) -> {
                    connectionMetadata.buffer().position(1);
                    int count = connectionMetadata.buffer().getInt();
                    TransactionEvent.Payload payload;
                    for(int i = 0; i < count; i++){
                        payload = TransactionEvent.readFromVMS(connectionMetadata.buffer());
                        assert vmsMetadata.queueToEventMap().get(payload.event()) != null;
                        vmsInternalChannels.transactionInputQueue().add(buildInboundEvent(payload));
                    }
                }
                case (TRANSACTION_EVENT) -> {
                    // can only be event, skip reading the message type
                    connectionMetadata.buffer().position(1);
                    // data dependence or input event
                    TransactionEvent.Payload payload = TransactionEvent.readFromVMS(connectionMetadata.buffer());
                    // send to scheduler
                    assert vmsMetadata.queueToEventMap().get(payload.event()) != null;
                    InboundEvent inboundEvent = buildInboundEvent(payload);
                    vmsInternalChannels.transactionInputQueue().add(inboundEvent);
                }
                default ->
                    logger.warn("Unknown message type received from another VMS: "+messageType);
            }
            connectionMetadata.buffer().clear();
            connectionMetadata.channel().read(connectionMetadata.buffer(), connectionMetadata, this);
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel().isOpen()){
                logger.warn("Error on reading from VMS even though channel is open: "+exc.getMessage());
                connectionMetadata.channel().read(connectionMetadata.buffer(), connectionMetadata, this);
            } // else no nothing. upon a new connection this metadata can be recycled
        }
    }

    /**
     * On a connection attempt, it is unknown what is the type of node
     * attempting the connection. We find out after the first read.
     */
    private final class UnknownNodeReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            logger.info("Starting process for processing presentation message");

            // message identifier
            byte messageIdentifier = connectionMetadata.buffer().get(0);
            if(messageIdentifier != PRESENTATION){
                logger.warn("A node is trying to connect without a presentation message");
                connectionMetadata.buffer().clear();
                MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.buffer());
                try { connectionMetadata.channel().close(); } catch (IOException ignored) {}
                return;
            }

            byte nodeTypeIdentifier = connectionMetadata.buffer().get(1);
            connectionMetadata.buffer().position(2);

            switch (nodeTypeIdentifier) {
                case (SERVER_TYPE) -> {
                    if(leaderWorker == null || !leaderWorker.isRunning()) {
                        // set up leader worker
                        leaderWorker = new LeaderWorker(
                                me, connectionMetadata.channel(), MemoryManager.getTemporaryDirectBuffer(),
                                eventsToSendToLeader, queuesLeaderSubscribesTo,
                                leaderWorkerQueue, serdesProxy);

                        // no need to synchronize after handshake, the completion handler will fail automatically after channel is closed

                        new Thread(leaderWorker).start();
                        connectionMetadata.channel().read( connectionMetadata.buffer(), connectionMetadata, new LeaderReadCompletionHandler() );
                        logger.info("Leader worker set up");
                    } else {
                        logger.warn("Dropping a connection attempt from a node claiming to be leader");
                        try { connectionMetadata.channel().close(); } catch (IOException ignored) { }
                    }
                }
                case (VMS_TYPE) -> {
                    // then it is a vms intending to connect due to a data/event
                    // that should be delivered to this vms
                    VmsNode producerVms = Presentation.readVms(connectionMetadata.buffer(), serdesProxy);
                    // what should we do with this producer?
                    connectionMetadata.buffer().clear();
                    // setup event handler for this producer vms
                    connectionMetadata.channel().read(connectionMetadata.buffer(), connectionMetadata, new VmsReadCompletionHandler());
                }
                case CLIENT -> {
                    // used for bulk data loading for now (maybe used for tests later)
                    String tableName = Presentation.readClient(connectionMetadata.buffer());
                    connectionMetadata.buffer().clear();
                    BulkDataLoader bulkDataLoader = (BulkDataLoader) vmsMetadata.loadedVmsInstances().get("data_loader");
                    if(bulkDataLoader != null) {
                        bulkDataLoader.init(tableName, connectionMetadata);
                    } else {
                        logger.warn("Data loader is not loaded in the runtime.");
                    }
                }
                default -> {
                    logger.warn("Presentation message from unknown source:" + nodeTypeIdentifier);
                    connectionMetadata.buffer().clear();
                    MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.buffer());
                    try {
                        connectionMetadata.channel().close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            logger.warn("Error on processing presentation message!");
        }

    }

    /**
     * Class is iteratively called by the socket pool threads.
     */
    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            logger.info("An unknown host has started a connection attempt.");
            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
            try {
                logger.info("Remote address: "+channel.getRemoteAddress().toString());
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);
                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                channel.read( buffer, new ConnectionMetadata(buffer, channel), new UnknownNodeReadCompletionHandler() );
                logger.info("Read handler for unknown node has been setup: "+channel.getRemoteAddress());
            } catch(Exception e){
                logger.info("Accept handler for unknown node caught exception: "+e.getMessage());
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                logger.info("Accept handler set up again for listening.");
                // continue listening
                serverSocket.accept(null, this);
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            String message = exc.getMessage();
            if(message == null){
                if (exc.getCause() instanceof ClosedChannelException){
                    message = "Connection is closed";
                } else if ( exc instanceof AsynchronousCloseException || exc.getCause() instanceof AsynchronousCloseException) {
                    message = "Event handler has been stopped?";
                } else {
                    message = "No cause identified";
                }
            }

            logger.warn("Error on accepting connection: "+ message);
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            } else {
                logger.warn("Socket is not open anymore. Cannot set up accept again");
            }
        }

    }

    private InboundEvent buildInboundEvent(TransactionEvent.Payload payload){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(payload.event());
        Object input = this.serdesProxy.deserialize(payload.payload(), clazz);
        Map<String, Long> precedenceMap = this.serdesProxy.deserializeDependenceMap(payload.precedenceMap());
        assert (precedenceMap != null);
        if(!precedenceMap.containsKey(this.me.name)){
            throw new IllegalStateException("Precedent tid of "+payload.tid()+" is unknown.");
        }
        this.tidToPrecedenceMap.put(payload.tid(), precedenceMap);

        // insert the updates in the replication stream first
        Map<String, String> writesMap = serdesProxy.deserializeMap( payload.updates() );

        // by queuing replicated items before the input event,
        // that implicitly ensures the transaction runs with
        // the updates applied to the internal state
        Map<String,List<TransactionWrite>> updateMap;
        if(!writesMap.isEmpty()) {
            updateMap = new HashMap<>();
            for (var tableWrites : writesMap.entrySet()) {
                List<TransactionWrite> updates = serdesProxy.deserializeList(tableWrites.getValue());
                updateMap.put(tableWrites.getKey(), updates);
            }
        } else {
            updateMap = Collections.emptyMap();
        }

        return new InboundEvent( payload.tid(), precedenceMap.get(this.me.name), payload.batch(), payload.event(), input, updateMap );
    }

    private final class LeaderReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            connectionMetadata.buffer().position(0);
            byte messageType = connectionMetadata.buffer().get();

            logger.info("Leader has sent a message type: "+messageType);

            // receive input events
            switch (messageType) {
                /*
                 * Given a new batch of events sent by the leader, the last message is the batch info
                 */
                case (BATCH_OF_TRANSACTION_EVENTS) -> {
                    // to increase performance, one would buffer this buffer for processing and then read from another buffer
                    int count = connectionMetadata.buffer().getInt();
                    List<InboundEvent> payloads = new ArrayList<>(count);
                    TransactionEvent.Payload payload;
                    // extract events batched
                    for (int i = 0; i < count - 1; i++) {
                        // move offset to discard message type
                        connectionMetadata.buffer().get();
                        payload = TransactionEvent.readFromLeader(connectionMetadata.buffer());
                        if (vmsMetadata.queueToEventMap().get(payload.event()) != null) {
                            payloads.add(buildInboundEvent(payload));
                        }
                    }

                    // batch commit info always come last
                    byte eventType = connectionMetadata.buffer().get();
                    if (eventType == BATCH_COMMIT_INFO) {
                        // it means this VMS is a terminal node in sent batch
                        BatchCommitInfo.Payload bPayload = BatchCommitInfo.read(connectionMetadata.buffer());
                        processNewBatchInfo(bPayload);
                    } else { // then it is still event
                        payload = TransactionEvent.readFromLeader(connectionMetadata.buffer());
                        if (vmsMetadata.queueToEventMap().get(payload.event()) != null)
                            payloads.add(buildInboundEvent(payload));
                    }
                    // add after to make sure the batch context map is filled by the time the output event is generated
                    vmsInternalChannels.transactionInputQueue().addAll(payloads);
                }
                case (BATCH_COMMIT_INFO) -> {
                    // events of this batch from VMSs may arrive before the batch commit info
                    // it means this VMS is a terminal node for the batch
                    logger.info("Batch commit info received from the leader");
                    BatchCommitInfo.Payload bPayload = BatchCommitInfo.read(connectionMetadata.buffer());
                    processNewBatchInfo(bPayload);
                }
                case (BATCH_COMMIT_COMMAND) -> {
                    logger.info("Batch commit command received from the leader");
                    // a batch commit queue from next batch can arrive before this vms moves next? yes
                    BatchCommitCommand.Payload payload = BatchCommitCommand.read(connectionMetadata.buffer());
                    processNewBatchInfo(payload);
                }
                case (TRANSACTION_EVENT) -> {
                    TransactionEvent.Payload payload = TransactionEvent.readFromLeader(connectionMetadata.buffer());
                    // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                    if (vmsMetadata.queueToEventMap().get(payload.event()) != null) {
                        vmsInternalChannels.transactionInputQueue().add(buildInboundEvent(payload));
                    }
                }
                case (TX_ABORT) -> {
                    TransactionAbort.Payload transactionAbortReq = TransactionAbort.read(connectionMetadata.buffer());
                    vmsInternalChannels.transactionAbortInputQueue().add(transactionAbortReq);
                }
//            else if (messageType == BATCH_ABORT_REQUEST){
//                // some new leader request to roll back to last batch commit
//                BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read( connectionMetadata.readBuffer );
//                vmsInternalChannels.batchAbortQueue().add(batchAbortReq);
//            }
                case (CONSUMER_CTX) -> {

                    // FIXME complete!!!!

                    Map<String, List<NetworkAddress>> receivedConsumerVms = ConsumerContext.read(connectionMetadata.buffer(), serdesProxy);
                    if (receivedConsumerVms != null) {
                        connectToReceivedConsumerSet(receivedConsumerVms);
                    }
                }
                case (PRESENTATION) -> logger.warn("Presentation being sent again by the leader!?");
                default -> logger.warn("Message type sent by leader cannot be identified: "+messageType);
            }

            connectionMetadata.buffer().clear();
            connectionMetadata.channel().read(connectionMetadata.buffer(), connectionMetadata, this);
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel().isOpen()){
                connectionMetadata.channel().read(connectionMetadata.buffer(), connectionMetadata, this);
            } else {
                // stop worker unilaterally
                leaderWorker.stop();
            }
        }

    }

    private void processNewBatchInfo(BatchCommitInfo.Payload batchCommitInfo){
        BatchContext batchContext = BatchContext.build(batchCommitInfo);
        this.batchContextMap.put(batchCommitInfo.batch(), batchContext);
        this.batchToNextBatchMap.put( batchCommitInfo.previousBatch(), batchCommitInfo.batch() );
    }

    /**
     * Context of execution of this method:
     * This is not a terminal node in this batch, which means
     * it does not know anything about the batch commit command just received.
     * If the previous batch is completed and this received batch is the next,
     * we just let the main loop update it
     */
    private void processNewBatchInfo(BatchCommitCommand.Payload batchCommitCommand){
        BatchContext batchContext = BatchContext.build(batchCommitCommand);
        this.batchContextMap.put(batchCommitCommand.batch(), batchContext);
        this.batchToNextBatchMap.put( batchCommitCommand.previousBatch(), batchCommitCommand.batch() );
    }

}
