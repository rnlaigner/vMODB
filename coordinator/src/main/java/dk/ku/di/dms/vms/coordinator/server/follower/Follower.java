package dk.ku.di.dms.vms.coordinator.server.follower;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.Constants;
import dk.ku.di.dms.vms.modb.common.schema.batch.follower.BatchReplication;
import dk.ku.di.dms.vms.modb.common.schema.batch.follower.BatchReplicationAck;
import dk.ku.di.dms.vms.modb.common.schema.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.meta.LockConnectionMetadata;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

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

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * Follower
 * Initial design: sit idle and wait for new election in case heartbeat does not arrive on time
 * TODO finish when a new leader is elected needs to send a batch abort request. but this is in the coordinator class...
 */
public final class Follower extends SignalingStoppableRunnable {

    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    // general tasks, like sending info to VMSs and other servers
    private final ExecutorService taskExecutor;

    // required in order to send votes if new election process starts
    private final Map<Integer, ServerIdentifier> servers;

    // private final ConnectionMetadata serverConnectionMetadata;

    private final ServerIdentifier me;

    private final ServerIdentifier leader;

    private LockConnectionMetadata leaderConnectionMetadata;

    // are we using this?
    // private final Map<Long, BatchContext> batchContextMap;

    private volatile long lastBatchOffsetCommitted;

    // refers to last committed batch
    private Map<String, Long> lastTidOfBatchPerVms;

    private volatile long lastTimestamp;

    private final FollowerOptions options;

    private final Gson gson;

    public Follower(AsynchronousServerSocketChannel serverSocket,
                    AsynchronousChannelGroup group,
                    ExecutorService taskExecutor,
                    FollowerOptions options,
                    Map<Integer, ServerIdentifier> servers,
                    ServerIdentifier me,
                    ServerIdentifier leader,
                    Gson gson) {

        // network and executor
        this.serverSocket = Objects.requireNonNull(serverSocket);
        this.group = group;
        this.taskExecutor = Objects.requireNonNull(taskExecutor);

        // options
        this.options = options;

        this.servers = servers;
        this.me = me;
        this.leader = leader;

        // batch
        // this.batchContextMap = new HashMap<>();

        this.gson = gson;
    }

    @Override
    public void run() {

        // accept handler
        serverSocket.accept( null, new AcceptCompletionHandler());

        // connect to leader
        if(!connectToLeader()) {
            this.signal.add( NO_RESULT );
            return;
        }

        // start timestamp for heartbeat check
        lastTimestamp = System.nanoTime();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool( 1 );

        ScheduledFuture<?> heartbeatTask = scheduledExecutorService.
                scheduleAtFixedRate(this::checkHeartbeat, 0L,  options.getHeartbeatTimeout(), TimeUnit.MILLISECONDS);

        while(isRunning()) {

//            try {
//                Issue issue = issueQueue.take();
//            } catch (InterruptedException ignored) { }

        }

        heartbeatTask.cancel(true);

    }

    private void checkHeartbeat(){

        // if heartbeat timed out, leave loop
        // can just sleep until the next timestamp (slightly after is better due to network latency)

        // setup accept handler, since new servers may enter the system. besides
        // long timeout = options.getHeartbeatTimeout();

//        if (System.nanoTime() - lastTimestamp >= options.getHeartbeatTimeout()){
//            stop();
//            this.signal.add( NO_RESULT );
//        }

        // check whether leader has already connected
        if(leaderConnectionMetadata == null) return;

        if(!leaderConnectionMetadata.channel.isOpen()){
            stop();
            this.signal.add( NO_RESULT );
        }

    }

    private boolean connectToLeader(){

        ByteBuffer readBuffer = MemoryManager.getTemporaryDirectBuffer();
        ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer();

        // should try three times connection to leader, otherwise starts a new election...
        int maxAttempts = options.getMaximumLeaderConnectionAttempt();
        boolean finished = false;

        while(!finished && maxAttempts > 0) {

            try {

                InetSocketAddress address = new InetSocketAddress(leader.host, leader.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                channel.connect(address).get();

                LockConnectionMetadata connectionMetadata = new LockConnectionMetadata(
                        leader.hashCode(),
                        LockConnectionMetadata.NodeType.SERVER,
                        readBuffer,
                        writeBuffer,
                        channel,
                        // new ReentrantLock()
                        null // no need to lock, only one thread writing
                );

                Presentation.writeServer(connectionMetadata.writeBuffer, me);

                channel.write(connectionMetadata.writeBuffer).get();

                connectionMetadata.writeBuffer.clear();

                channel.read(connectionMetadata.readBuffer, connectionMetadata, new ReadCompletionHandler());

                // set up read completion handler for receiving heartbeats
                // perhaps good to introduce a delta (to reflect possible network latency introduced by the link)

                // if heartbeat threshold has been achieved, finish this class and return (signaling... false?)

                finished = true;

            } catch (Exception ignored) {
                logger.info("Error connecting to host. I am " + me.host + ":" + me.port + " and the target is " + leader.host + ":" + leader.port);
            }

            if(!finished) maxAttempts--;

        }

        return finished;

    }

    private class ReadCompletionHandler implements CompletionHandler<Integer, LockConnectionMetadata> {

        @Override
        public void completed(Integer result, LockConnectionMetadata connectionMetadata) {

            ByteBuffer readBuffer = connectionMetadata.readBuffer;

            byte type = readBuffer.get();

            if(type == Constants.HEARTBEAT){

                // this is the only thread updating it
                lastTimestamp = System.nanoTime();

                readBuffer.clear();

                connectionMetadata.channel.read( readBuffer, connectionMetadata, this );


            } else if(type == Constants.BATCH_REPLICATION){

                BatchReplication.Payload payload = BatchReplication.read( readBuffer );

                readBuffer.clear();

                try {

                    try {

                        lastTidOfBatchPerVms = gson.fromJson(payload.vmsTidMap(), new TypeToken<Map<String, Long>>() {}.getType());
                        // actually this is not yet sure... only after receiving the next one... but let's consider this for now
                        lastBatchOffsetCommitted = payload.batch();

                        // confirming
                        BatchReplicationAck.write( connectionMetadata.writeBuffer, payload.batch() );

                    } catch (JsonSyntaxException e) { // error in the json
                        BatchReplicationAck.write(connectionMetadata.writeBuffer, 0);
                    }
                    connectionMetadata.channel.write( connectionMetadata.writeBuffer ).get();
                    connectionMetadata.writeBuffer.clear();

                } catch (ExecutionException | InterruptedException ignored) { }
                finally {
                    if(connectionMetadata.channel != null && connectionMetadata.channel.isOpen()) {
                        try {
                            connectionMetadata.channel.close();
                        } catch (IOException ignored) {}
                    }
                    MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.writeBuffer);
                    MemoryManager.releaseTemporaryDirectBuffer(connectionMetadata.readBuffer);
                }

            }

        }

        @Override
        public void failed(Throwable exc, LockConnectionMetadata connectionMetadata) {
            AsynchronousSocketChannel channel = connectionMetadata.channel;
            if(channel != null && channel.isOpen()) {
                try {
                    if(!channel.getOption( SO_KEEPALIVE )) {
                        channel.close();
                        return;
                    }
                } catch (IOException ignored) {}
            }
            connectionMetadata.readBuffer.clear();
            // read again
            connectionMetadata.channel.read( connectionMetadata.readBuffer, connectionMetadata, this );
        }

    }

    /**
     * Who would like to connect?
     * The coordinator for batch replication. This is the only assumption for now.
     * If that changes, we must read the presentation header and add the node to the servers list.
     */
    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            ByteBuffer readBuffer = MemoryManager.getTemporaryDirectBuffer();
            ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer();

            // if it is a VMS, need to forward to the leader ? better to let the vms know
            try{
                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, false);

                leaderConnectionMetadata = new LockConnectionMetadata(
                        leader.hashCode(),
                        LockConnectionMetadata.NodeType.SERVER,
                        readBuffer,
                        writeBuffer,
                        channel,
                        null // no need to lock, only one thread writing
                );

                channel.read( readBuffer, leaderConnectionMetadata, new ReadCompletionHandler() );

            } catch(Exception e){
                if(channel != null && !channel.isOpen()){
                    MemoryManager.releaseTemporaryDirectBuffer(readBuffer);
                    MemoryManager.releaseTemporaryDirectBuffer(writeBuffer);
                }
            }

            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            }

        }

        @Override
        public void failed(Throwable exc, Void void_) {
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            }
        }

    }

}
