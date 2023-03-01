package dk.ku.di.dms.vms.coordinator.election;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.election.schema.VoteRequest;
import dk.ku.di.dms.vms.coordinator.election.schema.VoteResponse;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * An election task is a thread that encapsulates all subtasks (i.e., threads)
 * necessary to complete a leader election. Only after this thread finishes,
 * a server can act as a leader or follower.
 * -
 * Define leader based on highest offset and timestamp
 * while election is not defined and election timeout has not timed out, continue
 * -
 * We assume the nodes are fixed. Later we revisit this choice.
 * TODO Cluster membership management (e.g., removing nodes, replacing nodes)
 * -
 * Protocol SCTP is maybe a better fit for leader election since it is message-oriented, rather than stream oriented
 * On the other hand, the UDP allows multicast, which is good for leader election (sending messages to all nodes
 * by design instead of iterating over the nodes to send individual messages)
 */
public final class ElectionWorker extends SignalingStoppableRunnable {

    private volatile int state;
    public static final int NEW          = 0;
    public static final int CANDIDATE    = 1; // running the protocol
    public static final int LEADER       = 2; // has received the ACKs from a majority
    public static final int FOLLOWER     = 3;

    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to VMSs and other servers
    private final ExecutorService taskExecutor;

    // the identification of this server
    private final ServerIdentifier me;

    // can be == me
    // only one thread modifying it, no need for atomic reference
    private volatile ServerIdentifier leader;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerIdentifier> servers;

    private final Map<Integer, LockConnectionMetadata> connectionMetadataMap;

    // a bounded time in which a leader election must occur, otherwise it should restart. in milliseconds
    // only one thread modifying, no need to use AtomicLong
    private volatile long timeout;

    // https://stackoverflow.com/questions/3786825/volatile-boolean-vs-atomicboolean
    private final AtomicBoolean voted;

    private final Object responseMapLock = new Object();
    private final Map<Integer, VoteResponse.Payload> responses;

    // making sure read-after-write atomicity for writers
    private final AtomicInteger opN = new AtomicInteger(0);
    // volatile to make sure all threads don't read from their cpu cache
    private volatile int N; // only gets updated when a round terminates

    private final ElectionOptions options;

    // for broadcaster
    private final BlockingQueue<Byte> actionQueue;

    // for simple sender
    private final BlockingQueue<VoteMessageContext> voteMessagesToSend;

    private static class VoteMessageContext {
        byte type; // vote or response
        ServerIdentifier source;
        ServerIdentifier target;
        boolean response;

        public VoteMessageContext(byte type, ServerIdentifier target, boolean response) {
            this.type = type;
            this.target = target;
            this.response = response;
        }

        public VoteMessageContext(ServerIdentifier source, ServerIdentifier target) {
            this.type = VOTE_REQUEST;
            this.source = source;
            this.target = target;
        }
    }

    public ElectionWorker(AsynchronousServerSocketChannel serverSocket,
                          AsynchronousChannelGroup group,
                          ExecutorService taskExecutor,
                          ServerIdentifier me,
                          Map<Integer, ServerIdentifier> servers,
                          ElectionOptions options){
        super();
        this.state = NEW;
        this.serverSocket = serverSocket;
        this.group = group;
        this.taskExecutor = taskExecutor;
        this.me = me;
        this.servers = servers;
        this.connectionMetadataMap = new ConcurrentHashMap<>();

        this.state = CANDIDATE;
        this.voted = new AtomicBoolean(false);

        this.N = servers.size();
        this.responses = new ConcurrentHashMap<>();

        this.options = options;
        this.actionQueue = new LinkedBlockingQueue<>();
        this.voteMessagesToSend = new LinkedBlockingQueue<>();
    }

    private void runRound(){

        // start round sending vote requests
        actionQueue.add( VOTE_REQUEST );

        int state_ = state;

        // force read from memory only once
        long timeout_ = timeout;

        /* while majority cannot be established, we cannot proceed safely */
        long elapsed = 0L;
        long startTime = System.currentTimeMillis();
        while(elapsed < timeout_ && state_ == CANDIDATE){
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {} // this can make messages not being handled. it is the timeout making this happen
            elapsed = System.currentTimeMillis() - startTime;
            state_ = state;
        }

        // round is over, responses received no longer hold
        // this is the same in all servers
        if(state_ == CANDIDATE){
            resetVoteResponses();// data races may still allow old votes to be computed
            voted.set(false); // make sure the next round this server is able to vote
        }

        logger.info("Event loop has finished. I am "+me.host+":"+me.port+" and my state is "+state_);

    }

    /**
     *  Have to cancel all read completion handlers and queued
     *  writes after leader is elected
     */
    @Override
    public void run() {

        logger.info("Initializing election round. I am "+me.host+":"+me.port);

        Broadcaster broadcaster = new Broadcaster();
        SimpleSender simpleSender = new SimpleSender();

        initialize(broadcaster, simpleSender);

        long timeout_;

        while(isRunning()){

            // the round is an abstraction to avoid a vote given from holding forever (e.g., the requesting node may have crashed)
            runRound();

            if(state == CANDIDATE) {
                // define a new delta since defining a leader is taking too long
                // increment since nothing has been defined
                timeout_ = timeout; // avoid two reads from memory
                timeout = (long) (timeout_ + (timeout_ * options.getRoundDeltaIncrease()));
            } else {
                // election process is over
                break;
            }

        }

        finish(broadcaster, simpleSender);

    }

    private void finish(Broadcaster broadcaster, SimpleSender simpleSender){
        simpleSender.stop();
        broadcaster.stop();

        // signal the server
        signal.add(FINISHED);
    }

    private void initialize(Broadcaster broadcaster, SimpleSender simpleSender) {

        // being single thread makes it easier to avoid data races
        serverSocket.accept( null, new AcceptCompletionHandler() );

        // then start the broadcaster and vote response threads
        taskExecutor.submit( broadcaster );
        taskExecutor.submit( simpleSender );

        timeout = options.getInitRoundTimeout();

    }

    private LockConnectionMetadata connectToServer(ServerIdentifier server){

        LockConnectionMetadata connectionMetadata = null;

        try {

            InetSocketAddress address = new InetSocketAddress(server.host, server.port);
            AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);

            channel.setOption( TCP_NODELAY, Boolean.TRUE );
            channel.setOption( SO_KEEPALIVE, Boolean.TRUE );

            connectionMetadata = new LockConnectionMetadata(
                    server.hashCode(),
                    LockConnectionMetadata.NodeType.SERVER,
                    MemoryManager.getTemporaryDirectBuffer(128),
                    MemoryManager.getTemporaryDirectBuffer(128),
                    channel,
                    new Semaphore(1)
                    );

            channel.connect(address).get();

            logger.info("Connected to node "+
                    server.host+":"+server.port);

            connectionMetadata.writeBuffer.clear();
            connectionMetadata.readBuffer.clear();

            connectionMetadataMap.put(server.hashCode(), connectionMetadata);

            // setup read handler
            channel.read( connectionMetadata.readBuffer, connectionMetadata, new ReadCompletionHandler());

            return connectionMetadata;

        } catch(InterruptedException | IOException | ExecutionException ignored){

            logger.warn("It was not possible to connect to node "+
                    server.host+":"+server.port);

            if(connectionMetadata != null) {
                MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.readBuffer);
                MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.writeBuffer);

                if(connectionMetadata.channel.isOpen()){
                    try {
                        connectionMetadata.channel.close();
                    } catch (IOException ignored1) { }
                    finally {
                        if(connectionMetadataMap.get(connectionMetadata.key) != null ){
                            connectionMetadataMap.remove( connectionMetadata.key );
                        }
                    }
                }

            }

        }

        return null;

    }

    private void resetVoteResponses(){
        synchronized (responseMapLock) {
            this.responses.clear();
            // do we have any new node?
            this.N += opN.get();
        }
        this.opN.set(0);
    }

    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            // logger.info("I am "+me.host+":"+me.port+". Initializing message handler for "+server.host+":"+server.port);

            try {
                InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
                String host = remoteAddress.getHostName();
                int port = remoteAddress.getPort();
                int key = Objects.hash( host, port );

                // is in the list of known servers?
                if (servers.get( key ) != null) {

                    LockConnectionMetadata connMeta = connectionMetadataMap.get(key);
                    if (connMeta == null) {

                        connMeta = new LockConnectionMetadata(
                                key,
                                LockConnectionMetadata.NodeType.SERVER,
                                MemoryManager.getTemporaryDirectBuffer(128),
                                MemoryManager.getTemporaryDirectBuffer(128),
                                channel,
                                new Semaphore(1)
                        );

                        connectionMetadataMap.put(key, connMeta);

                    } else {
                        // update channel if not active
                        if(!connMeta.channel.isOpen()) {
                            connMeta.channel = channel;
                            servers.get( key ).on();
                        } else {

                            // disconnect because it was a concurrent connection made
                            channel.close();

                        }
                    }

                    channel.read(connMeta.readBuffer, connMeta, new ReadCompletionHandler());

                } else {

                    // new server added dynamically

                    ServerIdentifier newServer = new ServerIdentifier( host, port);
                    servers.put(key, newServer);

                    opN.addAndGet(1);

                    LockConnectionMetadata connMeta = new LockConnectionMetadata(
                            key,
                            LockConnectionMetadata.NodeType.SERVER,
                            MemoryManager.getTemporaryDirectBuffer(128),
                            MemoryManager.getTemporaryDirectBuffer(128),
                            channel,
                            new Semaphore(1)
                    );

                    connectionMetadataMap.put(key, connMeta);

                    channel.read( connMeta.readBuffer, connMeta, new ReadCompletionHandler() );

                }

            } catch(IOException ignored) {}

        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            // nothing to do
        }

    }

    /**
     * Thread responsible for handling messages related to leader election process.
     * I cannot set up the timeout here because I may spawn more than a thread to
     * handle messages. The timeout must be external to this thread, it is, the
     * thread that "owns" this one.
     * <a href="https://blog.gceasy.io/2021/02/24/java-threads-may-not-be-memory-efficient/">...</a>
     * Single thread, so no need to deal with data races.
     */
    private class ReadCompletionHandler implements CompletionHandler<Integer, LockConnectionMetadata> {

        @Override
        public void completed(Integer result, LockConnectionMetadata connectionMetadata) {

            ByteBuffer readBuffer = connectionMetadata.readBuffer;
            readBuffer.position(0);

            byte messageIdentifier = readBuffer.get();

            logger.info("Message read. I am "+me.host+":"+me.port+" identifier is "+messageIdentifier);

            switch (messageIdentifier) {
                case VOTE_RESPONSE -> {

                    VoteResponse.Payload payload = VoteResponse.read(readBuffer);

                    logger.info("Vote response received: "+ payload.response +". I am " + me.host + ":" + me.port);

                    int serverId = Objects.hash(payload.host, payload.port);

                    // is it a yes? add to responses
                    if (!payload.response) {
                        break;
                    }

                    synchronized (responseMapLock) { // the responses are perhaps being reset
                        if (!responses.containsKey(serverId)) { // avoid duplicate vote

                            responses.put(serverId, payload);

                            // do I have the majority of votes?
                            // !voted prevent two servers from winning the election... but does not prevent
                            if (!voted.get() && responses.size() + 1 > (N / 2)) {
                                logger.info("I am leader. I am " + me.host + ":" + me.port);
                                leader = me; // only this thread is writing

                                actionQueue.add(LEADER_REQUEST);
                                state = LEADER;
                            }

                        }
                    }

                }

                case VOTE_REQUEST -> {

                    logger.info("Vote request received. I am " + me.host + ":" + me.port);

                    ServerIdentifier serverRequestingVote = VoteRequest.read(readBuffer);

                    if (voted.get()) {
                        // taskExecutor.submit(new Broadcaster(VOTE_RESPONSE, serverRequestingVote, false));
                        logger.info("Vote not granted, already voted. I am " + me.host + ":" + me.port);
                    } else {

                        // TO AVOID TWO (OR MORE) LEADERS!!!
                        // if a server is requesting a vote, it means this server is
                        // in another round, so I need to remove its vote from my (true) responses
                        // because this server may give a vote to another server
                        // in this case I will send a vote request again (or solely remove it)
                        boolean previousVoteReceived = false;
                        synchronized (responseMapLock) { // the responses are perhaps being reset, cannot count on this vote anymore
                            if (responses.get(serverRequestingVote.hashCode()) != null) {
                                responses.remove( serverRequestingVote.hashCode() );
                                previousVoteReceived = true;
                            }
                        }

                        if (serverRequestingVote.lastOffset > me.lastOffset) {
                            // grant vote
                            voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, true ) );
                            voted.set(true);
                            logger.info("Vote granted. I am " + me.host + ":" + me.port);
                        } else if (serverRequestingVote.lastOffset < me.lastOffset) {
                            voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, false ) );
                            logger.info("Vote not granted. I am " + me.host + ":" + me.port);
                        } else { // equal

                            if (serverRequestingVote.hashCode() > me.hashCode()) {
                                // grant vote
                                voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, true ) );
                                voted.set(true);
                                logger.info("Vote granted. I am " + me.host + ":" + me.port);
                            } else {
                                voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, false ) );
                                logger.info("Vote not granted. I am " + me.host + ":" + me.port);
                            }

                        }

                        // this is basically attempt to refresh the vote in case of intersecting distinct rounds in different servers
                        if(!voted.get() && previousVoteReceived){
                            voteMessagesToSend.add( new VoteMessageContext( me, serverRequestingVote ) );
                        }

                    }
                }

                case LEADER_REQUEST -> {
                    logger.info("Leader request received. I am " + me.host + ":" + me.port);
                    LeaderRequest.LeaderRequestPayload leaderRequest = LeaderRequest.read(readBuffer);
                    leader = servers.get(leaderRequest.hashCode());
                    state = FOLLOWER;
                }

                default -> logger.warn("Message identifier is unknown.");
            }

            readBuffer.clear();
            connectionMetadata.channel.read( readBuffer, connectionMetadata, this );

        }

        @Override
        public void failed(Throwable exc, LockConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel.isOpen()){
                connectionMetadata.readBuffer.clear();
                connectionMetadata.channel.read( connectionMetadata.readBuffer, connectionMetadata, this );
            }
            // else, the node will try to contact again, and we will update the connection metadata
        }

        // logger.info("Message handler is finished. I am "+me.host+":"+me.port);

    }

    private static final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private static final class WriteCompletionHandler implements CompletionHandler<Integer, LockConnectionMetadata> {

        @Override
        public void completed(Integer result, LockConnectionMetadata connectionMetadata) {
            unlockSafely(connectionMetadata);
        }

        @Override
        public void failed(Throwable exc, LockConnectionMetadata connectionMetadata) {
            unlockSafely(connectionMetadata);
        }

        private void unlockSafely(LockConnectionMetadata connectionMetadata){
            connectionMetadata.writeBuffer.rewind();
            connectionMetadata.writeLock.release();
        }

    }

    /**
     * Responsible for sending voting and leader requests
     * Each action in the action queue means the respective message
     * must be sent to all nodes
     */
    private class Broadcaster extends StoppableRunnable {

        @Override
        public void run() {

            while (isRunning()) {

                try {

                    byte messageType = actionQueue.take();

                    // now send the request
                    if (messageType == VOTE_REQUEST) {

                        for (ServerIdentifier server : servers.values()) {

                            LockConnectionMetadata connMeta = getConnection(server);

                            if (connMeta != null) {
                                connMeta.writeLock.acquire();
                                VoteRequest.write(connMeta.writeBuffer, me);
                                connMeta.writeBuffer.position(0);
                                connMeta.channel.write(connMeta.writeBuffer, connMeta, writeCompletionHandler);
                            }

                        }

                    } else if (messageType == LEADER_REQUEST) {

                        for (ServerIdentifier server : servers.values()) {

                            LockConnectionMetadata connMeta = getConnection(server);
                            if (connMeta != null) {
                                connMeta.writeLock.acquire();
                                LeaderRequest.write(connMeta.writeBuffer, me);
                                connMeta.writeBuffer.position(0);
                                connMeta.channel.write(connMeta.writeBuffer, connMeta, writeCompletionHandler);
                            }

                        }

                    } else {
                        logger.warn("Unknown message Type");
                    }

                } catch (Exception ignored) {
                    logger.warn("Error on write. I am " + me.host + ":" + me.port + " message type is ...");
                }

            }

        }

    }

    /**
     * Get connection. Connect if there is not previous established connection
     */
    private LockConnectionMetadata getConnection(ServerIdentifier server){
        LockConnectionMetadata connMeta = connectionMetadataMap.get( server.hashCode() );
        if(connMeta == null){
            return connectToServer(server);
        }
        return connMeta;
    }

    /**
     * Send individual messages to a particular node
     */
    private class SimpleSender extends StoppableRunnable {

        @Override
        public void run() {

            while (isRunning()){

                try {
                    VoteMessageContext msgContext = voteMessagesToSend.take();

                    LockConnectionMetadata connMeta = getConnection( msgContext.target );

                    if(connMeta != null) {

                        connMeta.writeLock.acquire();

                        if(msgContext.type == VOTE_RESPONSE) {
                            VoteResponse.write(connMeta.writeBuffer, me, msgContext.response);
                        } else if(msgContext.type == VOTE_REQUEST) {
                            VoteRequest.write(connMeta.writeBuffer, me);
                        }

                        connMeta.writeBuffer.position(0);
                        connMeta.channel.write(connMeta.writeBuffer, connMeta, writeCompletionHandler);

                    } else {
                        logger.warn("Could not connect to server: "+
                                msgContext.target.host+":"+msgContext.target.port);
                    }

                } catch (InterruptedException ignored) { }

            }

        }
    }

    public int getState(){
        return state;
    }

    public ServerIdentifier getLeader(){
        return this.leader;
    }

    /**
     * To reuse connections already established and buffers
     * @return connection metadata
     */
    public Map<Integer, LockConnectionMetadata> getServerConnectionMetadata() {
        return this.connectionMetadataMap;
    }

}
