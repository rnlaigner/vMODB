package dk.ku.di.dms.vms.modb.service.server;

import dk.ku.di.dms.vms.modb.common.schema.meta.VmsEventSchema;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Sources of inspiration:
 * https://crunchify.com/java-nio-non-blocking-io-with-server-client-example-java-nio-bytebuffer-and-channels-selector-java-nio-vs-io/
 * https://www.eclipse.org/jetty/documentation/jetty-11/programming-guide/index.html#advanced-embedding
 * https://codingcraftsman.wordpress.com/2015/07/27/signalling-between-threads-in-java/
 * https://stackoverflow.com/questions/43163592/standalone-websocket-server-without-jee-application-server
 * https://stackoverflow.com/questions/8125507/how-can-i-send-and-receive-websocket-messages-on-the-server-side
 * https://stackoverflow.com/questions/12875418/client-server-connection-using-a-thread-pool
 * http://tutorials.jenkov.com/java-multithreaded-servers/thread-pooled-server.html
 *
 * TODO: think about mechanisms to avoid this thread consuming a CPU forever.
 *       if we just have one client (the runtime), as long as it connected, this thread can be put to sleep
 *       and awake only when a new websocket client connection is made (e.g., the runtime fails)
 *  This may be the answer: https://liakh-aliaksandr.medium.com/java-sockets-i-o-blocking-non-blocking-and-asynchronous-fb7f066e4ede
 *  ===> This thread cannot be put to sleep since it acts as a server, serving requests from both the coordinator and the sdk
 *  To fix what is explained above we need a proper thread pool and completion handlers...
 *
 * TODO: standardize log messages, cache query plans
 *
 */
public class AsyncVMSServer extends SignalingStoppableRunnable {

    private final ExecutorService executorService;

    private AsynchronousChannelGroup group;

    private AsynchronousServerSocketChannel serverSocket;

    private final Queue<Object> inputQueue;

    private final Queue<Object> outputQueue;

    private final IVmsSerdesProxy serdes;

    /********************* VMS *******************/
    Map<String, VmsEventSchema> eventSchemaMap;

    /********************* EVENT *******************/
    private final ByteBuffer eventReadBuffer;

    private final ByteBuffer eventWriteBuffer;

    public AsyncVMSServer(Queue<Object> inputQueue, Queue<Object> outputQueue, IVmsSerdesProxy serdes) {
        super();

        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.serdes = serdes;

        // thread pool
        // if less than 3, in the occurrence of two concurrent accept, there will be no progress
        // after connect, one will be mostly lying... in any case, the advice is to always use N + 1
        this.executorService = Executors.newFixedThreadPool(3); // one thread per channel

        this.eventReadBuffer = ByteBuffer.allocateDirect( 1024 );
        this.eventWriteBuffer = ByteBuffer.allocateDirect( 1024 );

        // a forkjoinpool for retrieving data and allowing multiple workers to parallelize the scans...
    }

    private void createServerSocket() {
        try {
            // socket channel group, both connect, read, and write share the same group
            this.group = AsynchronousChannelGroup.withThreadPool( executorService  );
        } catch (IOException e) {
            throw new RuntimeException("Cannot create channel group.", e);
        }

        try {
            this.serverSocket = AsynchronousServerSocketChannel.open( group );
            this.serverSocket.bind(new InetSocketAddress(80));

//            EventChannelConnectionHandler handler = new EventChannelConnectionHandler();
//            this.serverSocket.accept(null, handler);

            // TODO make log async?
            // TODO integrate memory handler into modb? https://docs.oracle.com/javase/10/core/java-logging-overview.htm
            logger.info("WebSocket Server has started on 127.0.0.1:80.\r\nWaiting for a connection...");
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port 80", e);
        }
    }

    public void run() {

        // run does not allow for throwing exceptions, so we encapsulate the exception in a method
        createServerSocket();

        // independent channels

        // how do I know which one is for event and data?
        //  the event socket channel is opened first followed by the data channel


        // handler for events  ---  this requires a data frame not so big
        handleEventChannelConnection();

        // handler for data  --- this requires a data frame bigger
        // handleDataChannelConnection();

        // TODO connect to coordinator
        // connectToCoordinator(); // the coordinator should be modeled as a pub/sub service... a pubsub of data queries, data, not only events...

        // no exception were thrown, we are ready to start
        while( isRunning() ){




            // TODO handle coordinator events, pull data from coordinator
            //  sidecar could do some local scheduling according to the global schedule received from the global scheduler
            //  will read from coordinator in batches: https://sites.google.com/site/gson/streaming

            // TODO push data to the sdk-runtime ????
            //  it is up to the client handler to do it, here we deal with the coordinator only

        }

        this.executorService.shutdown();
        logger.info("VMSServer Server stopped.");

    }



    private void handleEventChannelConnection() {

        EventChannelConnectionHandler eventChannelConnectionHandler = new EventChannelConnectionHandler(serdes, eventWriteBuffer);

        serverSocket.accept("", eventChannelConnectionHandler );

        eventSchemaMap = eventChannelConnectionHandler.get();

        //try {
            // I don't need to explicitly program an async accept, since the Java API gives me that by design
            // AsynchronousSocketChannel socketChannel = serverSocket.accept().get();


            // socketChannel.read();

            // executorService.submit(new AsyncSocketEventHandler(socketChannel, inputQueue, outputQueue, serdes));

//        } catch (ExecutionException | InterruptedException e) {
//            logger.info("Error obtaining future: "+e.getLocalizedMessage());
//            throw new RuntimeException("Cannot connect to event channel.");
//        }

    }





}
