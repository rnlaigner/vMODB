package dk.ku.di.dms.vms.coordinator.election;

import dk.ku.di.dms.vms.coordinator.election.schema.LeaderRequest;
import dk.ku.di.dms.vms.coordinator.election.schema.VoteRequest;
import dk.ku.di.dms.vms.coordinator.election.schema.VoteResponse;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.ProducerWorker;
import dk.ku.di.dms.vms.web_common.channel.*;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static dk.ku.di.dms.vms.coordinator.election.Constants.*;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

/**
 * An election task is a thread that encapsulates all subtasks (i.e., threads)
 * necessary to complete a leader election. Only after this thread finishes,
 * a server can act as a leader or follower.
 * -
 * Define leader based on highest offset and timestamp
 * while election is not defined and election timeout has not timed out, continue
 * -
 * We assume the nodes are fixed. Later we revisit this choice.
 * Desirable feature: Cluster membership management (e.g., removing nodes, replacing nodes)
 * -
 * Protocol SCTP is maybe a better fit for leader election since it is message-oriented, rather than stream oriented
 * SCTP also allows multicast, preventing UDP usage
 */
public final class ElectionWorker extends ProducerWorker {

    private static final System.Logger logger = System.getLogger(ElectionWorker.class.getName());

    private volatile int state;
    public static final int NEW          = 0;
    public static final int CANDIDATE    = 1; // running the protocol
    public static final int LEADER       = 2; // has received the ACKs from a majority
    public static final int FOLLOWER     = 3;

    private final IServerChannel serverSocket;

    // general tasks, like sending info to VMSs and other servers
    private final ExecutorService taskExecutor;

    // the identification of this server
    private final ServerNode me;

    // can be == me
    // only one thread modifying it, no need for atomic reference
    private volatile ServerNode leader;

    // even though we can start with a known number of servers, their payload may have changed after a crash
    private final Map<Integer, ServerNode> servers;

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
        ServerNode source;
        ServerNode target;
        boolean response;

        public VoteMessageContext(byte type, ServerNode target, boolean response) {
            this.type = type;
            this.target = target;
            this.response = response;
        }

        public VoteMessageContext(ServerNode source, ServerNode target) {
            this.type = VOTE_REQUEST;
            this.source = source;
            this.target = target;
        }
    }

    public ElectionWorker(IServerChannel serverSocket,
                          ExecutorService taskExecutor,
                          ServerNode me,
                          Map<Integer, ServerNode> servers,
                          ElectionOptions options){
        super("election-worker", IdentifiableNode.build("election", me), VmsSerdesProxyBuilder.build(), false);
        this.state = NEW;
        this.serverSocket = serverSocket;
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

        logger.log(INFO,"Event loop has finished. I am "+me.host+":"+me.port+" and my state is "+state_);

    }

    /**
     *  Have to cancel all read completion handlers and queued
     *  writes after leader is elected
     */
    @Override
    public void run() {

        logger.log(INFO,"Initializing election round. I am "+me.host+":"+me.port);

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
    }

    private void initialize(Broadcaster broadcaster, SimpleSender simpleSender) {

        // being single thread makes it easier to avoid data races
        serverSocket.accept( new AcceptCompletionHandler() );

        // then start the broadcaster and vote response threads
        taskExecutor.submit( broadcaster );
        taskExecutor.submit( simpleSender );

        timeout = options.getInitRoundTimeout();

    }

    private LockConnectionMetadata connectToServer(ServerNode server){

        LockConnectionMetadata connectionMetadata = null;

        try {

            InetSocketAddress address = new InetSocketAddress(server.host, server.port);
            IChannel channel = ChannelBuilder.build(this.serverSocket);

            NetworkUtils.configure(channel, 4096);

            connectionMetadata = new LockConnectionMetadata(
                    server.hashCode(),
                    LockConnectionMetadata.NodeType.SERVER,
                    MemoryManager.getTemporaryDirectBuffer(128),
                    MemoryManager.getTemporaryDirectBuffer(128),
                    channel
                    );

            channel.connect(address).get();

            logger.log(INFO,"Connected to node "+
                    server.host+":"+server.port);

            connectionMetadata.writeBuffer.clear();
            connectionMetadata.readBuffer.clear();

            connectionMetadataMap.put(server.hashCode(), connectionMetadata);

            // setup read handler
            channel.read( connectionMetadata.readBuffer, connectionMetadata, new ReadCompletionHandler());

            return connectionMetadata;

        } catch(InterruptedException | IOException | ExecutionException ignored){

            logger.log(WARNING, "It was not possible to connect to node "+
                    server.host+":"+server.port);

            if(connectionMetadata != null) {
                MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.readBuffer);
                MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.writeBuffer);

                if(connectionMetadata.channel.isOpen()){
                    connectionMetadata.channel.close();
                    if(connectionMetadataMap.get(connectionMetadata.key) != null ){
                        connectionMetadataMap.remove( connectionMetadata.key );
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

    private class AcceptCompletionHandler implements CompletionHandler<IChannel, Void> {
        @Override
        public void completed(IChannel channel, Void void_) {
            // logger.log(INFO,"I am "+me.host+":"+me.port+". Initializing message handler for "+server.host+":"+server.port);
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
                            channel
                    );
                    connectionMetadataMap.put(key, connMeta);
                } else {
                    // update channel if not active
                    if(!connMeta.channel.isOpen()) {
                        // connMeta.channel = channel;
                        servers.get( key ).on();
                    } else {

                        // disconnect because it was a concurrent connection made
                        channel.close();

                    }
                }
                channel.read(connMeta.readBuffer, connMeta, new ReadCompletionHandler());
            } else {
                // new server added dynamically
                ServerNode newServer = new ServerNode( host, port);
                servers.put(key, newServer);
                opN.addAndGet(1);
                LockConnectionMetadata connMeta = new LockConnectionMetadata(
                        key,
                        LockConnectionMetadata.NodeType.SERVER,
                        MemoryManager.getTemporaryDirectBuffer(128),
                        MemoryManager.getTemporaryDirectBuffer(128),
                        channel
                );
                connectionMetadataMap.put(key, connMeta);
                channel.read( connMeta.readBuffer, connMeta, new ReadCompletionHandler() );
            }
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

            logger.log(INFO,"Message read. I am "+me.host+":"+me.port+" identifier is "+messageIdentifier);

            switch (messageIdentifier) {
                case VOTE_RESPONSE -> {

                    VoteResponse.Payload payload = VoteResponse.read(readBuffer);

                    logger.log(INFO,"Vote response received: "+ payload.response +". I am " + me.host + ":" + me.port);

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
                                logger.log(INFO,"I am leader. I am " + me.host + ":" + me.port);
                                leader = me; // only this thread is writing

                                actionQueue.add(LEADER_REQUEST);
                                state = LEADER;
                            }

                        }
                    }

                }

                case VOTE_REQUEST -> {

                    logger.log(INFO,"Vote request received. I am " + me.host + ":" + me.port);

                    ServerNode serverRequestingVote = VoteRequest.read(readBuffer);

                    if (voted.get()) {
                        // taskExecutor.submit(new Broadcaster(VOTE_RESPONSE, serverRequestingVote, false));
                        logger.log(INFO,"Vote not granted, already voted. I am " + me.host + ":" + me.port);
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
                            logger.log(INFO,"Vote granted. I am " + me.host + ":" + me.port);
                        } else if (serverRequestingVote.lastOffset < me.lastOffset) {
                            voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, false ) );
                            logger.log(INFO,"Vote not granted. I am " + me.host + ":" + me.port);
                        } else { // equal

                            if (serverRequestingVote.hashCode() > me.hashCode()) {
                                // grant vote
                                voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, true ) );
                                voted.set(true);
                                logger.log(INFO,"Vote granted. I am " + me.host + ":" + me.port);
                            } else {
                                voteMessagesToSend.add( new VoteMessageContext( VOTE_RESPONSE, serverRequestingVote, false ) );
                                logger.log(INFO,"Vote not granted. I am " + me.host + ":" + me.port);
                            }

                        }

                        // this is basically attempt to refresh the vote in case of intersecting distinct rounds in different servers
                        if(!voted.get() && previousVoteReceived){
                            voteMessagesToSend.add( new VoteMessageContext( me, serverRequestingVote ) );
                        }

                    }
                }

                case LEADER_REQUEST -> {
                    logger.log(INFO,"Leader request received. I am " + me.host + ":" + me.port);
                    LeaderRequest.LeaderRequestPayload leaderRequest = LeaderRequest.read(readBuffer);
                    leader = servers.get(leaderRequest.hashCode());
                    state = FOLLOWER;
                }

                default -> logger.log(WARNING, "Message identifier is unknown.");
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

        // logger.log(INFO,"Message handler is finished. I am "+me.host+":"+me.port);

    }

    private final WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler();

    private final class WriteCompletionHandler implements CompletionHandler<Integer, LockConnectionMetadata> {

        @Override
        public void completed(Integer result, LockConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
            releaseLock();
        }

        @Override
        public void failed(Throwable exc, LockConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
            releaseLock();
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

                        for (ServerNode server : servers.values()) {

                            LockConnectionMetadata connMeta = getConnection(server);

                            if (connMeta != null) {
                                acquireLock();
                                VoteRequest.write(connMeta.writeBuffer, me);
                                connMeta.writeBuffer.position(0);
                                connMeta.channel.write(connMeta.writeBuffer, connMeta, writeCompletionHandler);
                            }

                        }

                    } else if (messageType == LEADER_REQUEST) {

                        for (ServerNode server : servers.values()) {

                            LockConnectionMetadata connMeta = getConnection(server);
                            if (connMeta != null) {
                                acquireLock();
                                LeaderRequest.write(connMeta.writeBuffer, me);
                                connMeta.writeBuffer.position(0);
                                connMeta.channel.write(connMeta.writeBuffer, connMeta, writeCompletionHandler);
                            }

                        }

                    } else {
                        logger.log(WARNING, "Unknown message Type");
                    }

                } catch (Exception ignored) {
                    logger.log(WARNING, "Error on write. I am " + me.host + ":" + me.port + " message type is ...");
                }

            }

        }

    }

    /**
     * Get connection. Connect if there is not a previous established connection
     */
    private LockConnectionMetadata getConnection(ServerNode server){
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

                        acquireLock();

                        if(msgContext.type == VOTE_RESPONSE) {
                            VoteResponse.write(connMeta.writeBuffer, me, msgContext.response);
                        } else if(msgContext.type == VOTE_REQUEST) {
                            VoteRequest.write(connMeta.writeBuffer, me);
                        }

                        connMeta.writeBuffer.position(0);
                        connMeta.channel.write(connMeta.writeBuffer, connMeta, writeCompletionHandler);

                    } else {
                        logger.log(WARNING, "Could not connect to server: "+
                                msgContext.target.host+":"+msgContext.target.port);
                    }

                } catch (InterruptedException ignored) { }

            }

        }
    }

    public int getState(){
        return state;
    }

    public ServerNode getLeader(){
        return this.leader;
    }

}
