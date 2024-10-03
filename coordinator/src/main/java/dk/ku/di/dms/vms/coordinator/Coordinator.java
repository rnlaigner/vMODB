package dk.ku.di.dms.vms.coordinator;

import dk.ku.di.dms.vms.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.options.VmsWorkerOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionWorker;
import dk.ku.di.dms.vms.coordinator.vms.IVmsWorker;
import dk.ku.di.dms.vms.coordinator.vms.VmsWorker;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import dk.ku.di.dms.vms.web_common.ModbHttpServer;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.JdkAsyncChannel;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.PRESENTATION;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.SERVER_TYPE;
import static dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata.NodeType.SERVER;
import static java.lang.System.Logger.Level.*;

/**
 * Also known as the "Leader"
 * Class that encapsulates all logic related to issuing of
 * transactions, batch commits, transaction aborts, ...
 */
public final class Coordinator extends ModbHttpServer {

    private static final System.Logger LOGGER = System.getLogger(Coordinator.class.getName());
    
    private final CoordinatorOptions options;

    // this server socket
    private final AsynchronousServerSocketChannel serverSocket;

    // group for channels
    private final AsynchronousChannelGroup group;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerNode> servers;

    // for server nodes
    private final Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap;

    /* VMS data structures **/

    /**
     * received from program start
     * also called known VMSs
     */
    private final Map<String, IdentifiableNode> starterVMSs;

    /**
     * Those received from program start + those that joined later
     * shared with vms workers
     */
    private final Map<String, VmsNode> vmsMetadataMap;

    // the identification of this server
    private final ServerNode me;

    /*
     * the offset of the pending batch commit (always < batchOffset)
     * volatile because it is accessed by http threads from driver requests
     */
    private volatile long batchOffsetPendingCommit;

    // metadata about all non-committed batches. when a batch commit finishes, it is removed from this map
    private final Map<Long, BatchContext> batchContextMap;

    // transaction requests coming from the http event loop
    private final Deque<TransactionInput>[] transactionInputDeques;

    // transaction definitions coming from the http event loop
    private final Map<String, TransactionDAG> transactionMap;

    // serialization and deserialization of complex objects
    private final IVmsSerdesProxy serdesProxy;

    private final BlockingQueue<Object> coordinatorQueue;

    private final Map<String, IVmsWorker> vmsWorkerContainerMap;

    private final List<Tuple<TransactionWorker, Thread>> transactionWorkers;

    private final Consumer<TransactionInput> transactionInputConsumer;

    private final IHttpHandler httpHandler;

    public static Coordinator build(// obtained from leader election or passed by parameter on setup
                                    Map<Integer, ServerNode> servers,
                                    // passed by parameter
                                    Map<String, IdentifiableNode> startersVMSs,
                                    Map<String, TransactionDAG> transactionMap,
                                    ServerNode me,
                                    // coordinator configuration
                                    CoordinatorOptions options,
                                    // starting batch offset (may come from storage after a crash)
                                    long startingBatchOffset,
                                    // starting tid (may come from storage after a crash)
                                    long startingTid,
                                    Function<Coordinator, IHttpHandler> httpHandlerSupplier,
                                    IVmsSerdesProxy serdesProxy) throws IOException {
        return new Coordinator(servers == null ? new ConcurrentHashMap<>() : servers,
                new HashMap<>(), startersVMSs, transactionMap,
                me, options, startingBatchOffset, startingTid,
                httpHandlerSupplier, serdesProxy);
    }

    @SuppressWarnings("unchecked")
    private Coordinator(Map<Integer, ServerNode> servers,
                        Map<Integer, LockConnectionMetadata> serverConnectionMetadataMap,
                        Map<String, IdentifiableNode> startersVMSs,
                        Map<String, TransactionDAG> transactionMap,
                        ServerNode me,
                        CoordinatorOptions options,
                        long startingBatchOffset,
                        long startingTid,
                        Function<Coordinator, IHttpHandler> httpHandlerSupplier,
                        IVmsSerdesProxy serdesProxy) throws IOException {
        super();

        // coordinator options
        this.options = options;

        if(options.getNetworkThreadPoolSize() > 0) {
            this.group = AsynchronousChannelGroup.withThreadPool(Executors.newWorkStealingPool(options.getNetworkThreadPoolSize()));
            this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        } else {
            /* may lead to better performance than default group
            this.group = AsynchronousChannelGroup.withThreadPool(ForkJoinPool.commonPool());
            this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
             */
            this.group = null;
            this.serverSocket = AsynchronousServerSocketChannel.open();
        }

        // network and executor
        this.serverSocket.bind(me.asInetSocketAddress());

        this.starterVMSs = startersVMSs;
        this.vmsMetadataMap = new ConcurrentHashMap<>();

        // might come filled from election process
        this.servers = servers;
        this.serverConnectionMetadataMap = serverConnectionMetadataMap;
        this.me = me;

        // infra
        this.serdesProxy = serdesProxy;

        // shared data structure with http handler
        this.transactionInputDeques = new Deque[options.getNumTransactionWorkers()];
        for(int i = 0; i < options.getNumTransactionWorkers(); i++){
            this.transactionInputDeques[i] = new ConcurrentLinkedDeque<>();
        }

        if(options.getNumTransactionWorkers() == 1){
            var inputQueue = this.transactionInputDeques[0];
            transactionInputConsumer = inputQueue::offerLast;
        } else {
            transactionInputConsumer = transactionInput -> {
                int idx = ThreadLocalRandom.current().nextInt(0, options.getNumTransactionWorkers());
                transactionInputDeques[idx].offerLast(transactionInput);
            };
        }

        // in production, it requires receiving new transaction definitions
        this.transactionMap = transactionMap;

        // to hold actions spawned by events received by different VMSs
        this.coordinatorQueue = new LinkedBlockingQueue<>();

        // batch commit metadata
        long dummyBatchOffset = startingBatchOffset - 1;
        this.batchOffsetPendingCommit = startingBatchOffset;

        // initialize batch offset map
        this.batchContextMap = new ConcurrentHashMap<>();
        BatchContext currentBatch = new BatchContext(dummyBatchOffset);
        currentBatch.seal(startingTid - 1, startingTid - 1, Map.of(), Map.of());
        this.batchContextMap.put(dummyBatchOffset, currentBatch);

        this.vmsWorkerContainerMap = new HashMap<>();
        this.transactionWorkers = new ArrayList<>();
        this.httpHandler = httpHandlerSupplier.apply(this);
    }

    private final Map<String, VmsNode[]> vmsIdentifiersPerDAG = new HashMap<>();

    /**
     * This method contains the event loop that contains the main functions of a leader/coordinator
     * What happens if two nodes declare themselves as leaders? We need some way to let it know
     * OK (b) Batch management
     * designing leader mode first
     * design follower mode in another class to avoid convoluted code.
     * Going for a different socket to allow for heterogeneous ways for a client to connect with the servers e.g., http.
     * It is also good to better separate resources, so VMSs and followers do not share resources with external clients
     */
    @Override
    public void run() {
        // setup asynchronous listener for new connections
        this.serverSocket.accept(null, new AcceptCompletionHandler());
        // connect to all virtual microservices
        this.setupStarterVMSs();
        this.preprocessDAGs();
        this.setUpTransactionWorkers();

        // event loop
        try {
            Object message;
            do {
//            try {
//                message = this.coordinatorQueue.take();
//                this.processVmsMessage(message);
//            } catch (InterruptedException ignored) { }
                // tends to be faster than blocking
                while ((message = this.coordinatorQueue.poll(250, TimeUnit.MILLISECONDS)) != null) {
                    this.processVmsMessage(message);
                }
            } while (this.isRunning());
        } catch (Exception e){
            e.printStackTrace(System.out);
        }

        this.failSafeClose();
        LOGGER.log(INFO,"Leader: Finished execution.");
    }

    private Map<String, TransactionWorker.PrecedenceInfo> buildStarterPrecedenceMap() {
        Map<String, TransactionWorker.PrecedenceInfo> precedenceMap = new HashMap<>();
        for(var vms : this.vmsMetadataMap.entrySet()){
            precedenceMap.put(vms.getKey(), new TransactionWorker.PrecedenceInfo(0, 0, -1));
        }
        return precedenceMap;
    }

    private void setUpTransactionWorkers() {
        int numWorkers = this.options.getNumTransactionWorkers();
        long initTid = 1;

        var firstPrecedenceInputQueue = new ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>>();
        var precedenceMapInputQueue = firstPrecedenceInputQueue;
        ConcurrentLinkedDeque<Map<String, TransactionWorker.PrecedenceInfo>> precedenceMapOutputQueue;

        var starterPrecedenceMap = buildStarterPrecedenceMap();
        firstPrecedenceInputQueue.add(starterPrecedenceMap);

        int idx = 1;
        do {
            if(idx < numWorkers){
                precedenceMapOutputQueue = new ConcurrentLinkedDeque<>();
            } else {
                // complete the ring
                precedenceMapOutputQueue = firstPrecedenceInputQueue;
            }
            Deque<TransactionInput> txInputQueue = this.transactionInputDeques[idx-1];
            TransactionWorker txWorker = TransactionWorker.build(idx, txInputQueue, initTid,
                    this.options.getMaxTransactionsPerBatch(), this.options.getBatchWindow(),
                    numWorkers, precedenceMapInputQueue, precedenceMapOutputQueue, this.transactionMap,
                    this.vmsIdentifiersPerDAG, this.vmsWorkerContainerMap, this.coordinatorQueue, this.serdesProxy);
            Thread txWorkerThread = Thread.ofPlatform()
                    .name("tx-worker-"+idx)
                    .inheritInheritableThreadLocals(false)
                    .unstarted(txWorker);

            initTid = initTid + this.options.getMaxTransactionsPerBatch();
            precedenceMapInputQueue = precedenceMapOutputQueue;
            idx++;
            this.transactionWorkers.add(Tuple.of( txWorker, txWorkerThread ));
        } while (idx <= numWorkers);

        // start them all
        for(var txWorker : this.transactionWorkers){
            txWorker.t2().start();
        }
    }

    /**
     * Process all VMS_IDENTIFIER first before submitting transactions
     */
    private void waitForAllStarterVMSs() {
        LOGGER.log(DEBUG,"Leader: Waiting for all starter VMSs to connect");
        do {
            this.processMessagesSentByVmsWorkers();
        } while (this.vmsMetadataMap.size() < this.starterVMSs.size());
    }

    private void preprocessDAGs() {
        int numWorkersPerVms = this.options.getNumWorkersPerVms();
        LOGGER.log(DEBUG,"Leader: Preprocessing transaction DAGs");
        Set<String> inputVMSsSeen = new HashSet<>();
        // build list of VmsIdentifier per transaction DAG
        for(var dag : this.transactionMap.entrySet()){
            this.vmsIdentifiersPerDAG.put(dag.getKey(), BatchAlgo.buildTransactionDagVmsList( dag.getValue(), this.vmsMetadataMap ));

            // set up additional connection with vms that start transactions
            for(var inputVms : dag.getValue().inputEvents.entrySet()){
                var vmsNode = this.vmsMetadataMap.get(inputVms.getValue().targetVms);
                if(vmsNode == null || inputVMSsSeen.contains(vmsNode.identifier)){
                    continue;
                }
                inputVMSsSeen.add(vmsNode.identifier);
                for(int i = 1; i < numWorkersPerVms; i++){
                    if(this.vmsWorkerContainerMap.containsKey(inputVms.getValue().targetVms)) {
                        try {
                            VmsWorker newWorker = VmsWorker.build(this.me, vmsNode, this.coordinatorQueue,
                                    () -> JdkAsyncChannel.create(this.group),
                                    new VmsWorkerOptions(
                                            true,
                                            this.options.isCompressing(),
                                            this.options.logging(),
                                            this.options.getMaxVmsWorkerSleep(),
                                            this.options.getNetworkBufferSize(),
                                            this.options.getNetworkSendTimeout(),
                                            this.options.getNumQueuesVmsWorker(),
                                            false),
                                    this.serdesProxy);
                            if(this.vmsWorkerContainerMap.get(inputVms.getValue().targetVms) instanceof VmsWorkerContainer o){
                                o.addWorker(newWorker);
                                Thread vmsWorkerThread = Thread.ofPlatform().factory().newThread(newWorker);
                                vmsWorkerThread.setName("vms-worker-" + vmsNode.identifier + "-" + (i));
                                vmsWorkerThread.start();
                            } else {
                                LOGGER.log(WARNING, "Leader: "+vmsNode.identifier+" type is unknown: "+this.vmsWorkerContainerMap.get(inputVms.getValue().targetVms).getClass().getName()+ ". Cannot spawn the worker thread!");
                            }
                        } catch (Exception e) {
                            LOGGER.log(WARNING, "Leader: Could not connect to VMS "+vmsNode.identifier, e);
                        }
                    }
                }
            }
        }
    }

    /**
     * A container of vms workers for the same VMS
     * Make a circular buffer. so events are spread among the workers (i.e., channels)
     */
    static final class VmsWorkerContainer implements IVmsWorker {
        private final int numVmsWorkers;
        private final VmsWorker[] vmsWorkers;
        private int nextPos;
        private final Consumer<TransactionEvent.PayloadRaw> queueFunc;

        VmsWorkerContainer(VmsWorker initialVmsWorker, int numVmsWorkers) {
            this.numVmsWorkers = numVmsWorkers;
            this.vmsWorkers = new VmsWorker[numVmsWorkers];
            this.vmsWorkers[0] = initialVmsWorker;
            this.nextPos = 1;
            if(numVmsWorkers > 1){
                this.queueFunc = (payload) -> {
                    int pos = ThreadLocalRandom.current().nextInt(0, this.numVmsWorkers);
                    this.vmsWorkers[pos].queueTransactionEvent(payload);
                };
            } else {
                this.queueFunc = (payload) -> this.vmsWorkers[0].queueTransactionEvent(payload);
            }
        }

        public void addWorker(VmsWorker vmsWorker) {
            this.vmsWorkers[this.nextPos] = vmsWorker;
            this.nextPos++;
        }

        @Override
        public void queueMessage(Object object){
            // always goes to first
            this.vmsWorkers[0].queueMessage(object);
        }

        @Override
        public void queueTransactionEvent(TransactionEvent.PayloadRaw payload){
            this.queueFunc.accept(payload);
        }
    }

    /**
     * After a leader election, it makes more sense that
     * the leader connects to all known virtual microservices.
     * No need to track the thread created because they will be
     * later mapped to the respective vms identifier by the thread
     */
    private void setupStarterVMSs() {
        var inputsVMSs = new HashSet<String>();
        for(var entry : this.transactionMap.entrySet()){
            for(var input : entry.getValue().inputEvents.entrySet()){
                inputsVMSs.add(input.getValue().targetVms);
            }
        }
        try {
            for (IdentifiableNode vmsNode : this.starterVMSs.values()) {
                // is this a VMS that receives transaction input?
                boolean active = inputsVMSs.contains(vmsNode.identifier);
                // coordinator will later keep track of this thread when
                // the connection with the VMS is fully established
                VmsWorker vmsWorker = VmsWorker.build(this.me, vmsNode, this.coordinatorQueue,
                        () -> JdkAsyncChannel.create(this.group),
                        new VmsWorkerOptions(
                                active,
                                this.options.isCompressing(),
                                this.options.logging(),
                                this.options.getMaxVmsWorkerSleep(),
                                this.options.getNetworkBufferSize(),
                                this.options.getNetworkSendTimeout(),
                                this.options.getNumQueuesVmsWorker(),
                                true),
                        this.serdesProxy);
                // virtual thread leads to performance degradation
                Thread vmsWorkerThread = Thread.ofPlatform().factory().newThread(vmsWorker);
                vmsWorkerThread.setName("vms-worker-" + vmsNode.identifier + "-0");
                this.vmsWorkerContainerMap.put(vmsNode.identifier,
                        new VmsWorkerContainer(vmsWorker, this.options.getNumWorkersPerVms())
                );
                vmsWorkerThread.start();
            }
        } catch (Exception e){
            LOGGER.log(ERROR, "It was not possible to connect to one of the starter VMSs: " + e.getMessage());
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
        }
        this.waitForAllStarterVMSs();
    }

    /**
     * Match output of a vms with the input of another
     * for each vms input event (not generated by the coordinator),
     * find the vms that generated the output
     */
    private List<IdentifiableNode> findConsumerVMSs(String outputEvent){
        List<IdentifiableNode> list = new ArrayList<>();
        // can be the leader or a vms
        for(VmsNode vms : this.vmsMetadataMap.values()){
            if(vms.inputEventSchema.containsKey(outputEvent)){
                list.add(vms.asIdentifiableNode());
            }
        }
        // assumed to be terminal? maybe yes.
        // vms is already connected to leader, no need to return coordinator
        return list;
    }

    private void failSafeClose(){
        // safe close
        try { this.serverSocket.close(); } catch (IOException ignored) {}
    }

    /**
     * This is where I define whether the connection must be kept alive
     * Depending on the nature of the request:
     * <a href="https://www.baeldung.com/java-nio2-async-socket-channel">...</a>
     * The first read must be a presentation message, informing what is this server (follower or VMS)
     */
    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {
        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            ByteBuffer buffer = null;
            try {
                NetworkUtils.configure(channel, options.getOsBufferSize());

                // right now I cannot discern whether it is a VMS or follower. perhaps I can keep alive channels from leader election?
                buffer = MemoryManager.getTemporaryDirectBuffer(options.getNetworkBufferSize());

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                // this will be handled by another thread in the group
                channel.read(buffer, buffer, new CompletionHandler<>() {
                    @Override
                    public void completed(Integer result, ByteBuffer buffer) {
                        // set this thread free. release the thread that belongs to the channel group
                        processReadAfterAcceptConnection(channel, buffer);
                    }
                    @Override
                    public void failed(Throwable exc, ByteBuffer buffer) {
                        MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    }
                });
            } catch (Exception e) {
                LOGGER.log(WARNING,"Leader: Error on accepting connection on " + me);
                if (buffer != null) {
                    MemoryManager.releaseTemporaryDirectBuffer(buffer);
                }
            } finally {
                // continue listening
                if (serverSocket.isOpen()) {
                    serverSocket.accept(null, this);
                }
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()) {
                serverSocket.accept(null, this);
            }
        }

        /**
         * Process Accept connection request
         * Task for informing the server running for leader that a leader is already established
         * We would no longer need to establish connection in case the {@link dk.ku.di.dms.vms.coordinator.election.ElectionWorker}
         * maintains the connections.
         */
        private void processReadAfterAcceptConnection(AsynchronousSocketChannel channel, ByteBuffer buffer){

            // message identifier
            byte messageIdentifier = buffer.get(0);

            if(messageIdentifier == VOTE_REQUEST || messageIdentifier == VOTE_RESPONSE){
                // so I am leader, and I respond with a leader request to this new node
                // taskExecutor.submit( new ElectionWorker.WriteTask( LEADER_REQUEST, server ) );
                // would be better to maintain the connection open.....
                buffer.clear();

                if(channel.isOpen()) {
                    LeaderRequest.write(buffer, me);
                    buffer.flip();
                    try (channel) {
                        channel.write(buffer); // write and forget
                    } catch (IOException ignored) {
                    } finally {
                        MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    }
                }
                return;
            }

            if(messageIdentifier == LEADER_REQUEST){
                // buggy node intending to pose as leader...
                LOGGER.log(WARNING,"Leader: A node is trying to present itself as leader!");
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch(Exception ignored){}
                return;
            }

            // if it is not a presentation, drop connection
            if(messageIdentifier != PRESENTATION){
                buffer.flip();
                String request = StandardCharsets.UTF_8.decode(buffer).toString();
                if(isHttpClient(request)){
                    var readCompletionHandler = new HttpReadCompletionHandler(
                            new ConnectionMetadata("http_client".hashCode(),
                                    ConnectionMetadata.NodeType.HTTP_CLIENT,
                                    channel),
                            buffer,
                            MemoryManager.getTemporaryDirectBuffer(options.getNetworkBufferSize()),
                            httpHandler
                    );
                    try { NetworkUtils.configure(channel, options.getOsBufferSize()); } catch (IOException ignored) { }
                    readCompletionHandler.process(request);
                } else {
                    LOGGER.log(WARNING,"Leader: A node is trying to connect without a presentation message. \n" + request);
                    buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(buffer);
                    try { channel.close(); } catch (IOException ignored) { }
                }
                return;
            }

            // now let's do the work
            buffer.position(1);

            byte type = buffer.get();
            if(type == SERVER_TYPE){
                this.processServerPresentationMessage(channel, buffer);
            } else {
                // simply unknown... probably a bug?
                LOGGER.log(WARNING,"Unknown type of client connection. Probably a bug? ");
                try (channel) { MemoryManager.releaseTemporaryDirectBuffer(buffer); } catch (Exception ignored){}
            }
        }

        /**
         * Still need to define what to do with connections from replicas....
         */
        private void processServerPresentationMessage(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            // server
            ServerNode newServer = Presentation.readServer(buffer);

            // check whether this server is known... maybe it has crashed... then we only need to update the respective channel
            if(servers.get(newServer.hashCode()) != null){
                // LockConnectionMetadata connectionMetadata = serverConnectionMetadataMap.get( newServer.hashCode() );
                // update metadata of this node
                servers.put( newServer.hashCode(), newServer );
                // connectionMetadata.channel = channel;
            } else { // no need for locking here
                servers.put( newServer.hashCode(), newServer );
                LockConnectionMetadata connectionMetadata = new LockConnectionMetadata(
                        newServer.hashCode(),
                        SERVER,
                        buffer,
                        MemoryManager.getTemporaryDirectBuffer(options.getNetworkBufferSize()),
                        channel,
                        new Semaphore(1) );
                serverConnectionMetadataMap.put( newServer.hashCode(), connectionMetadata );
            }
        }
    }

    /**
     * a vms, although receiving an event from a "next" batch, cannot yet commit, since
     * there may have additional events to arrive from the current batch
     * so the batch request must contain the last tid of the given vms
     * if an internal/terminal vms do not receive an input event in this batch, it won't be able to
     * progress since the precedence info will never arrive
     * this way, we need to send in this event the precedence info for all downstream VMSs of this event
     * having this info avoids having to contact all internal/terminal nodes to inform the precedence of events
     */
    public void queueTransactionInput(TransactionInput transactionInput){
        this.transactionInputConsumer.accept(transactionInput);
    }

    /**
     * This task assumes the channels are already established
     * Cannot have two threads writing to the same channel at the same time
     * A transaction manager is responsible for assigning TIDs to incoming transaction requests
     * This task also involves making sure the writes are performed successfully
     * A writer manager is responsible for defining strategies, policies, safety guarantees on
     * writing concurrently to channels.
     * <a href="https://web.mit.edu/6.005/www/fa14/classes/20-queues-locks/message-passing/">Message passing in Java</a>
     */
    private void processMessagesSentByVmsWorkers() {
        Object message;
        while((message = this.coordinatorQueue.poll()) != null) {
            this.processVmsMessage(message);
        }
    }

    private void processVmsMessage(Object message) {
        switch (message) {
            // receive metadata from all microservices
            case BatchContext batchContext -> this.processNewBatchContext(batchContext);
            case VmsNode vmsNode -> this.processVmsIdentifier(vmsNode);
            case TransactionAbort.Payload txAbort -> this.processTransactionAbort(txAbort);
            case BatchComplete.Payload batchComplete -> this.processBatchComplete(batchComplete);
            case BatchCommitAck.Payload msg ->
                // let's just ignore the ACKs. since the terminals have responded, that means the VMSs before them have processed the transactions in the batch
                // not sure if this is correct since we have to wait for all VMSs to respond...
                // only when all vms respond with BATCH_COMMIT_ACK we move this ...
                //        this.batchOffsetPendingCommit = batchContext.batchOffset;
                    LOGGER.log(INFO, "Leader: Batch (" + msg.batch() + ") commit ACK received from " + msg.vms());
            default ->
                    LOGGER.log(WARNING, "Leader: Received an unidentified message type: " + message.getClass().getName());
        }
    }

    private void processTransactionAbort(TransactionAbort.Payload txAbort) {
        // send abort to all VMSs...
        // later we can optimize the number of messages since some VMSs may not need to receive this abort
        // cannot commit the batch unless the VMS is sure there will be no aborts...
        // this is guaranteed by design, since the batch complete won't arrive unless all events of the batch arrive at the terminal VMSs
        this.batchContextMap.get(txAbort.batch()).tidAborted = txAbort.tid();
        // can reuse the same buffer since the message does not change across VMSs like the commit request
        for (VmsNode vms : this.vmsMetadataMap.values()) {
            // don't need to send to the vms that aborted
            // if(vms.getIdentifier().equalsIgnoreCase( msg.vms() )) continue;
            this.vmsWorkerContainerMap.get(vms.identifier).queueMessage(txAbort.tid());
        }
    }

    private void processBatchComplete(BatchComplete.Payload batchComplete) {
        // what if ACKs from VMSs take too long? or never arrive?
        // need to deal with intersecting batches? actually just continue emitting for higher throughput
        LOGGER.log(DEBUG,"Leader: Processing batch ("+ batchComplete.batch()+") complete from: "+ batchComplete.vms());
        BatchContext batchContext = this.batchContextMap.get( batchComplete.batch() );
        // only if it is not a duplicate vote
        batchContext.missingVotes.remove( batchComplete.vms() );
        if(batchContext.missingVotes.isEmpty()){
            LOGGER.log(INFO,"Leader: Received all missing votes of batch: "+ batchContext.batchOffset);
            this.updateBatchOffsetPendingCommit(batchContext);
        }
    }

    private final AtomicLong numTIDsCommitted = new AtomicLong(0);

    private void updateBatchOffsetPendingCommit(BatchContext batchContext) {
        if(batchContext.batchOffset == this.batchOffsetPendingCommit){
            this.numTIDsCommitted.updateAndGet(i -> i + batchContext.numTIDsOverall);
            this.sendCommitCommandToVMSs(batchContext);
            this.batchOffsetPendingCommit = batchContext.batchOffset + 1;
            // making this implementation order-independent, so not assuming batch commit are received in order
            BatchContext nextBatchContext = this.batchContextMap.get( this.batchOffsetPendingCommit );
            if(nextBatchContext != null && nextBatchContext.missingVotes.isEmpty()){
                this.updateBatchOffsetPendingCommit(nextBatchContext);
            }
            return;
        }
        // probably some batch complete message got lost or received out of order
        LOGGER.log(WARNING,"Leader: Batch ("+ batchContext.batchOffset +
                ") is not the pending one.\nStill has to wait for the pending batch ("+
                this.batchOffsetPendingCommit+") to finish before progressing. Nodes that still need to vote:\n"+
                (this.batchContextMap.containsKey( this.batchOffsetPendingCommit ) ?
                        this.batchContextMap.get( this.batchOffsetPendingCommit )
                                .missingVotes : Set.of()));
    }

    // seal batch and send batch complete to all terminals...
    private void processNewBatchContext(BatchContext batchContext) {
        this.batchContextMap.put(batchContext.batchOffset, batchContext);
        // after storing batch context, send to vms workers
        for(var entry : batchContext.terminalVMSs) {
            this.vmsWorkerContainerMap.get(entry).queueMessage(
                    BatchCommitInfo.of(batchContext.batchOffset,
                            batchContext.previousBatchPerVms.get(entry),
                            batchContext.numberOfTIDsPerVms.get(entry)));
        }
    }

    private void processVmsIdentifier(VmsNode vmsIdentifier_) {
        LOGGER.log(INFO,"Leader: Received a VMS_IDENTIFIER from: "+ vmsIdentifier_.identifier);
        // update metadata of this node so coordinator can reason about data dependencies
        this.vmsMetadataMap.put( vmsIdentifier_.identifier, vmsIdentifier_);

        if(this.vmsMetadataMap.size() < this.starterVMSs.size()) {
            LOGGER.log(INFO,"Leader: "+(this.starterVMSs.size() - this.vmsMetadataMap.size())+" starter(s) VMSs remain to be processed.");
            return;
        }
        // if all metadata, from all starter vms have arrived, then send the signal to them

        LOGGER.log(INFO,"Leader: All VMS starters have sent their VMS_IDENTIFIER");

        // new VMS may join, requiring updating the consumer set
        Map<String, List<IdentifiableNode>> vmsConsumerSet;

        for(VmsNode vmsNode : this.vmsMetadataMap.values()) {
            // if we reuse the hashmap, the entries get mixed and lead to incorrect consumer set
            vmsConsumerSet = new HashMap<>();

            IdentifiableNode vms = this.starterVMSs.get(vmsNode.identifier);
            if(vms == null) {
                LOGGER.log(WARNING,"Leader: Could not identify "+vmsNode.identifier+" from set of starter VMSs");
                continue;
            }

            // build global view of vms dependencies/interactions
            // build consumer set dynamically
            // for each output event, find the consumer VMSs
            for (VmsEventSchema eventSchema : vmsNode.outputEventSchema.values()) {
                List<IdentifiableNode> nodes = this.findConsumerVMSs(eventSchema.eventName);
                if (!nodes.isEmpty())
                    vmsConsumerSet.put(eventSchema.eventName, nodes);
            }

            String consumerSetStr = "";
            if (vmsConsumerSet.isEmpty()) {
                LOGGER.log(WARNING,"Leader: No consumer set built for "+vmsNode.identifier);
            } else {
                consumerSetStr = this.serdesProxy.serializeConsumerSet(vmsConsumerSet);
                LOGGER.log(INFO,"Leader: Consumer set built for "+vmsNode.identifier+": \n"+consumerSetStr);
            }

            if(!this.vmsWorkerContainerMap.containsKey(vmsNode.identifier)){
                LOGGER.log(ERROR,"Leader: Cannot find identifier ("+ vmsNode.identifier +") in worker container map.");
                continue;
            }

            this.vmsWorkerContainerMap.get(vmsNode.identifier).queueMessage(consumerSetStr);
        }
    }

    /**
     * Only send to non-terminals
     */
    private void sendCommitCommandToVMSs(BatchContext batchContext){
        for(VmsNode vms : this.vmsMetadataMap.values()){
            if(batchContext.terminalVMSs.contains(vms.identifier)) {
                LOGGER.log(DEBUG,"Leader: Batch ("+batchContext.batchOffset+") commit command not sent to "+ vms.identifier + " (terminal)");
                continue;
            }
            // has this VMS participated in this batch?
            if(!batchContext.numberOfTIDsPerVms.containsKey(vms.identifier)){
                //noinspection StringTemplateMigration
                LOGGER.log(DEBUG,"Leader: Batch ("+batchContext.batchOffset+") commit command will not be sent to "+ vms.identifier + " because this VMS has not participated in this batch.");
                continue;
            }
            this.vmsWorkerContainerMap.get(vms.identifier).queueMessage(
                    new BatchCommitCommand.Payload(
                        batchContext.batchOffset,
                        batchContext.previousBatchPerVms.get(vms.identifier),
                        batchContext.numberOfTIDsPerVms.get(vms.identifier)
            ));
        }
        // send to sse client if connected
        if(!SSE_CLIENTS.isEmpty()){
            SSE_CLIENTS.getFirst().sendToSseClient(this.numTIDsCommitted.get());
        }
    }

    public long getNumTIDsCommitted() {
        return this.numTIDsCommitted.get();
    }

    public long getNumTIDsSubmitted(){
        long sumTIDs = 0;
        for(var txWorker : this.transactionWorkers){
            sumTIDs += txWorker.t1().getNumTIDsSubmitted();
        }
        return sumTIDs;
    }

    public Map<String, VmsNode> getConnectedVMSs() {
        return this.vmsMetadataMap;
    }

    public long getBatchOffsetPendingCommit() {
        return this.batchOffsetPendingCommit;
    }

    public Map<String, IdentifiableNode> getStarterVMSs(){
        return this.starterVMSs;
    }

}
