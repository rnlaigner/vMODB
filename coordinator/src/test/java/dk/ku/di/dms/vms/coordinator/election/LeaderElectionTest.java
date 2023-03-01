package dk.ku.di.dms.vms.coordinator.election;

import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.runnable.VmsDaemonThreadFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static dk.ku.di.dms.vms.coordinator.election.Constants.LEADER_REQUEST;
import static dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable.FINISHED;
import static dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable.NO_RESULT;
import static java.lang.Thread.sleep;

/**
 * Unit test for simple App.
 */
public class LeaderElectionTest
{

    final static Logger logger = LoggerFactory.getLogger(LeaderElectionTest.class);

    @Test
    public void testBufferRead(){

        ByteBuffer buffer = ByteBuffer.allocate(128);

        byte[] hostBytes = "localhost".getBytes();

        buffer.put(LEADER_REQUEST);
        buffer.putInt( 80 );
        buffer.putInt( hostBytes.length );
        buffer.put( hostBytes );

        assert true;

    }

    @Test
    public void leaderElectionTest() throws IOException, InterruptedException {

        AsynchronousServerSocketChannel serverSocket1 = AsynchronousServerSocketChannel.open();
        serverSocket1.bind( new InetSocketAddress(8080) );

        AsynchronousServerSocketChannel serverSocket2 = AsynchronousServerSocketChannel.open();
        serverSocket2.bind( new InetSocketAddress(8081) );

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 8080 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 8081 );
        ServerIdentifier serverEm3 = new ServerIdentifier( "localhost", 8082 );

        Map<Integer,ServerIdentifier> servers1 = new HashMap<>();
        servers1.put( serverEm2.hashCode(), serverEm2 );
        servers1.put( serverEm3.hashCode(), serverEm3 );

        Map<Integer, ServerIdentifier> servers2 = new HashMap<>();
        servers2.put( serverEm1.hashCode(), serverEm1 );
        servers2.put( serverEm3.hashCode(), serverEm3 );

        ElectionWorker em1 = new ElectionWorker(serverSocket1, null, Executors.newFixedThreadPool(2), serverEm1, servers1, new ElectionOptions() );
        ElectionWorker em2 = new ElectionWorker(serverSocket2, null, Executors.newFixedThreadPool(2), serverEm2, servers2, new ElectionOptions() );

        new Thread( em1 ).start();
        new Thread( em2 ).start();

        byte take1 = em1.getResult();
        byte take2 = em2.getResult();

        logger.info( "result 1: " + take1 );
        logger.info( "result 2: " + take2 );

        assert(take1 == FINISHED && take2 == FINISHED);

    }

    /**
     * In this test, a server may need to retry the leader election
     * FIXME it stopped working. add some logs to the workers to get visibility
     */
    @Test
    public void leaderElectionWithRetryTest() throws IOException, InterruptedException {

        AsynchronousServerSocketChannel serverSocket1 = AsynchronousServerSocketChannel.open();
        serverSocket1.bind( new InetSocketAddress(8083) );

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 8083 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 8084 );
        ServerIdentifier serverEm3 = new ServerIdentifier( "localhost", 8085 );

        Map<Integer,ServerIdentifier> servers1 = new HashMap<>();
        servers1.put( serverEm2.hashCode(), serverEm2 );
        servers1.put( serverEm3.hashCode(), serverEm3 );

        Map<Integer,ServerIdentifier> servers2 = new HashMap<>();
        servers2.put( serverEm1.hashCode(), serverEm1 );
        servers2.put( serverEm3.hashCode(), serverEm3 );

        var group1 = AsynchronousChannelGroup.withFixedThreadPool(2, new VmsDaemonThreadFactory());
        var group2 = AsynchronousChannelGroup.withFixedThreadPool(2, new VmsDaemonThreadFactory());

        ElectionWorker em1 = new ElectionWorker(serverSocket1, group1, Executors.newFixedThreadPool(2), serverEm1, servers1, new ElectionOptions() );

        // TODO that should be integrated into the class instantiation
        new Thread( em1 ).setUncaughtExceptionHandler( em1.exceptionHandler );

        new Thread( em1 ).start();

        sleep(10000);

        // server 1 must still be a candidate
        assert em1.getState() == 1;

        AsynchronousServerSocketChannel serverSocket2 = AsynchronousServerSocketChannel.open();
        serverSocket2.bind( new InetSocketAddress(8084) );

        ElectionWorker em2 = new ElectionWorker(serverSocket2, group2, Executors.newFixedThreadPool(2), serverEm2, servers2, new ElectionOptions() );
        new Thread( em2 ).start();

        byte take2 = em2.getResult();
        byte take1 = em1.getResult();

        logger.info( "result 1: " + take1 );
        logger.info( "result 2: " + take2 );

        boolean bothHasSameLeader = em1.getLeader().hashCode() == em2.getLeader().hashCode();

        assert ( take1 != NO_RESULT && take2 != NO_RESULT && bothHasSameLeader );

    }

    /**
     * In this test, a leader than has received the majority of votes, but not sent the leader request yet,
     * fails. That forces the nodes to initiate a
     */
    // @Test
    public void leaderElectedFailAndNewLeaderMustBeSetupTest() {

        // TODO create a message handler that makes the node fail after receiving the majority of votes

    }

    /**
     * In this test, a server running for leader must be informed that a leader already exists
     * We can assume the leader itself sends to this node a leader request message
     * so no different messages need to be created
     */
    // @Test
    public void leaderElectionWithLeaderElectedTest() throws IOException, InterruptedException {

        // TODO finish. a server must run for leader and then received the current leader info
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(2));
        AsynchronousServerSocketChannel serverSocket1 = AsynchronousServerSocketChannel.open(group);
        serverSocket1.bind( new InetSocketAddress(8086) );

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 8086 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 8087 );
        ServerIdentifier serverEm3 = new ServerIdentifier( "localhost", 8088 );

        Map<Integer,ServerIdentifier> servers1 = new HashMap<>();
        servers1.put( serverEm2.hashCode(), serverEm2 );
        servers1.put( serverEm3.hashCode(), serverEm3 );

        ElectionWorker em1 = new ElectionWorker(serverSocket1, null, Executors.newFixedThreadPool(2), serverEm1, servers1, new ElectionOptions() );

        new Thread( em1 ).start();

        byte take1 = em1.getResult();

        logger.info( "result 1: " + take1 );

        assert(take1 == NO_RESULT);

    }

}
