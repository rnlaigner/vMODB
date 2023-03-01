package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.node.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.modb.transaction.api.CheckpointingAPI;
import dk.ku.di.dms.vms.modb.common.transaction.api.ReplicationAPI;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbeddedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.events.InputEventExample1;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample1;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample2;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample3;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static dk.ku.di.dms.vms.modb.common.schema.Constants.BATCH_OF_TRANSACTION_EVENTS;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * Test vms event handler
 * a. Are events being ingested correctly?
 * b. are events being outputted correctly?
 * TODO This list below is responsibility of another test class:
 * c. is the internal state being managed properly (respecting correctness semantics)?
 * d. all tables have data structures properly created? embed metadata loader
 * e. ingestion is being performed correctly?
 * f. repository facade is working properly?
 *  -
 *  For tests involving the leader, like batch, may need to have a fake leader thread
 *  For this class, transactions and isolation are not involved. We use simple dumb tasks
 *  that do not interact with database state
 */
public class EventHandlerTest {

    private record VmsCtx (
        EmbeddedVmsEventHandler eventHandler,
        VmsTransactionScheduler scheduler) {
        public void stop() {
            this.scheduler.stop();
            this.eventHandler.stop();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(EventHandlerTest.class);
    private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

    private static final class DumbCheckpointAPI implements CheckpointingAPI, ReplicationAPI {
        @Override
        public void checkpoint() {
             logger.info("Checkpoint called at: "+System.currentTimeMillis());
        }

        @Override
        public boolean addSubscription(String table) {
            return false;
        }

        @Override
        public boolean removeSubscription(String table) {
            return false;
        }

        @Override
        public void applyUpdates(Map<String, List<TransactionWrite>> updates) {
            // do nothing
        }

    }

    /**
     * Facility to load virtual microservice instances.
     * For tests, it may the case two VMSs are in the same package. That leads to
     * buggy metadata being loaded (i.e., swapped in-and-out events)
     * To avoid that, this method takes into consideration the inputs and output events
     * to be discarded for a given
     */
    private static VmsCtx loadMicroservice(NetworkNode node,
                                           Map<String, Set<ConsumerVms>> eventToConsumersMap,
                                           boolean eventHandlerActive,
                                           VmsEmbeddedInternalChannels vmsInternalPubSubService, String vmsName,
                                           List<String> inToDiscard, List<String> outToDiscard, List<String> inToSwap, List<String> outToSwap)
            throws Exception {

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata("dk.ku.di.dms.vms.sdk.embed");

        // discard events

        for (String in : inToDiscard)
            vmsMetadata.inputEventSchema().remove(in);

        for (String out : outToDiscard)
            vmsMetadata.outputEventSchema().remove(out);

        for (String in : inToSwap) {
            VmsEventSchema eventSchema = vmsMetadata.inputEventSchema().remove(in);
            vmsMetadata.outputEventSchema().put(in, eventSchema);
        }

        for (String in : outToSwap) {
            VmsEventSchema eventSchema = vmsMetadata.outputEventSchema().remove(in);
            vmsMetadata.inputEventSchema().put(in, eventSchema);
        }

        ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        VmsTransactionScheduler scheduler = new VmsTransactionScheduler(readTaskPool, vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(),null, null);

        VmsNode vmsIdentifier = new VmsNode(
                node.host, node.port, vmsName,
                1, 0, 0,
                vmsMetadata.tableSchema(), vmsMetadata.replicatedTableSchema(),
                vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        var dumb = new DumbCheckpointAPI();

        EmbeddedVmsEventHandler eventHandler = EmbeddedVmsEventHandler.buildWithDefaults(
                vmsIdentifier, eventToConsumersMap, dumb, dumb,
                vmsInternalPubSubService,  vmsMetadata, serdes, socketPool );

        if(eventHandlerActive) {
            Thread eventHandlerThread = new Thread(eventHandler);
            eventHandlerThread.start();
        }

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();

        return new VmsCtx(eventHandler,scheduler);

    }

    /**
     * In this test, the events are serialized and sent directly to
     * the internal channels, and not through the network
     * The idea is to isolate the bug if that appears.
     * I.e., is there a problem with the embed scheduler?
     */
    @Test
    public void testCrossVmsTransactionWithoutEventHandler() throws Exception {

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        // to avoid set up this thread as a producer
        VmsEmbeddedInternalChannels channelForAddingInput = new VmsEmbeddedInternalChannels();

        // microservice 1
        VmsCtx vmsCtx = loadMicroservice(
                new NetworkNode("localhost", 1080),
                null,
                false,
                channelForAddingInput,
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        List<String> outToSwap = List.of("out1");
        inToSwap = inToDiscard; // empty
        outToDiscard = inToDiscard;
        inToDiscard = List.of("in");

        // internal channel so this thread can read the output at some point without being a consumer
        VmsEmbeddedInternalChannels channelForGettingOutput = new VmsEmbeddedInternalChannels();

        // microservice 2
        VmsCtx vmsCtx2 = loadMicroservice(
                new NetworkNode("localhost", 1081),
                null,
                false,
                channelForGettingOutput,
                "example2",
                inToDiscard,
                outToDiscard,
                inToSwap,
                outToSwap);

        InputEventExample1 eventExample = new InputEventExample1(0);
        InboundEvent event = new InboundEvent(1,0,1,"in", eventExample, Collections.emptyMap());
        channelForAddingInput.transactionInputQueue().add(event);

        var input1ForVms2 = channelForAddingInput.transactionOutputQueue().poll(5, TimeUnit.SECONDS);
        assert input1ForVms2 != null;

        for(var res : input1ForVms2.resultTasks){
            // Class<?> clazz =  res.outputQueue().equalsIgnoreCase("out1") ? OutputEventExample1.class : OutputEventExample2.class;
            InboundEvent event_ = new InboundEvent(1,0,1, res.outputQueue(), res.output(), Collections.emptyMap());
            channelForGettingOutput.transactionInputQueue().add(event_);
        }

        // subscribe to output event out3
        VmsTransactionResult result = channelForGettingOutput.transactionOutputQueue().poll(5, TimeUnit.SECONDS);

        assert result != null && result.resultTasks != null && !result.resultTasks.isEmpty();

        vmsCtx.stop();
        vmsCtx2.stop();

        assert ((OutputEventExample3) result.resultTasks.get(0).output()).id == 2;
    }

    /**
     * Consumers are VMSs that intend to receive data from a given VMS
     * Originally the coordinator sends the set of consumer VMSs to each VMS
     * This is because the coordinator holds the transaction workflow definition
     * and from that it derives the consumers for each VMS
     * But for the purpose of this test, we open the interface of the event handler,
     * so we can pass the consumer without having to wait for a coordinator message
     */
    @Test
    public void testConnectionFromVmsToConsumer() throws Exception {

        // 1 - assemble a consumer network node
        ConsumerVms me = new ConsumerVms("localhost", 1082);

        // 2 - start a socket in the prescribed node
        AsynchronousServerSocketChannel serverSocket = AsynchronousServerSocketChannel.open();
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        final AsynchronousSocketChannel[] channel = {null};

        // 3 - setup accept connection
        serverSocket.accept(null, new CompletionHandler<>() {
            @Override
            public void completed(AsynchronousSocketChannel result, Object attachment) {
                channel[0] = result;
                success.getAndSet(true);
                latch.countDown();
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                latch.countDown();
            }
        });

        Set<ConsumerVms> consumerSet = new HashSet<>();
        consumerSet.add(me);
        Map<String, Set<ConsumerVms>> eventToConsumersMap = new HashMap<>();
        eventToConsumersMap.put("out1", consumerSet);
        eventToConsumersMap.put("out2", consumerSet);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        // 3 - start the vms 1. make sure the info that this is a vms consumer is passed as parameter
        VmsCtx vmsCtx = loadMicroservice(
                new NetworkNode("localhost", 1083),
                eventToConsumersMap,
                true,
                new VmsEmbeddedInternalChannels(),
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 3 - wait for vms 1 to connect
        latch.await();

        assert success.get();

        // 4 - read
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        channel[0].read(buffer).get();
        buffer.position(2);

        VmsNode producerVms = Presentation.readVms(buffer, VmsSerdesProxyBuilder.build());

        vmsCtx.stop();
        serverSocket.close();

        // 5 - check whether the presentation sent is correct
        assert producerVms.inputEventSchema.containsKey("in") &&
                producerVms.outputEventSchema.containsKey("out1") &&
                producerVms.outputEventSchema.containsKey("out2");

    }


    /**
     * Test sending an input data and receiving the results
     */
    @Test
    public void testReceiveBatchFromVmsAsConsumer() throws Exception {

        // 1 - assemble a consumer network node
        ConsumerVms me = new ConsumerVms("localhost", 1084);

        // 2 - start a socket in the prescribed node
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(2));
        AsynchronousServerSocketChannel serverSocket = AsynchronousServerSocketChannel.open(group);
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        // 3 - set consumer of events
        Set<ConsumerVms> consumerSet = new HashSet<>();
        consumerSet.add(me);
        Map<String, Set<ConsumerVms>> eventToConsumersMap = new HashMap<>();
        eventToConsumersMap.put("out1", consumerSet);
        eventToConsumersMap.put("out2", consumerSet);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        NetworkNode vmsToConnectTo =  new NetworkNode("localhost", 1085);
        // 4 - start the vms 1. don't need to pass producer info to vms 1, since vms is always waiting for producer
        VmsCtx vmsCtx = loadMicroservice(
                        vmsToConnectTo,
                        eventToConsumersMap,
                        true,
                        new VmsEmbeddedInternalChannels(),
                        "example1",
                        inToDiscard,
                        outToDiscard,
                        inToSwap,
                        inToDiscard);

        // 5 - accept connection but just ignore the payload. the above test is already checking this
        AsynchronousSocketChannel channel = serverSocket.accept().get(); // ignore result

        logger.info("Connection accepted from VMS");

        // 6 - read the presentation to set the writer of the producer free to write results
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        channel.read(buffer).get();
        buffer.clear(); // just ignore the presentation as we test this in the previous test

        logger.info("Presentation received from VMS");

        // 7 - send event input
        InputEventExample1 eventExample = new InputEventExample1(1);
        String inputPayload = serdes.serialize(eventExample, InputEventExample1.class);

        Map<String,Long> precedenceMap = new HashMap<>();
        precedenceMap.put("example1", 0L);

        TransactionEvent.Payload eventInput = TransactionEvent.Payload.of(1,1,"in", inputPayload, serdes.serializeMap(precedenceMap));
        TransactionEvent.write(buffer, eventInput);
        buffer.flip();
        channel.write(buffer).get(); // no need to wait
        buffer.clear();

        logger.info("Input event sent");

        /*
         8 - listen from the internal channel. may take some time because of the batch commit scheduler
         why also checking the output? to see if the event sent is correctly processed
         TODO hanging forever sometimes. not deterministic
          study possible problem: locking my thread pool: slide 44
          https://openjdk.org/projects/nio/presentations/TS-4222.pdf
          With an executor passed as a group, it started working....
        */
        channel.read(buffer).get( 25, TimeUnit.SECONDS );
        // channel.read(buffer).get( );

        logger.info("Batch received");

        // 9 - assert the batch of events is received
        buffer.position(0);
        byte messageType = buffer.get();
        assert messageType == BATCH_OF_TRANSACTION_EVENTS;
        int size = buffer.getInt();
        assert size == 2;

        TransactionEvent.Payload payload;
        for(int i = 0; i < size; i++){
            buffer.get(); // exclude event
            payload = TransactionEvent.readFromVMS(buffer);
            Class<?> clazz =  payload.event().equalsIgnoreCase("out1") ? OutputEventExample1.class : OutputEventExample2.class;
            Object obj = serdes.deserialize(payload.payload(), clazz);
            assert !payload.event().equalsIgnoreCase("out1") || obj instanceof OutputEventExample1;
            assert !payload.event().equalsIgnoreCase("out2") || obj instanceof OutputEventExample2;
        }

        vmsCtx.stop();
        // channel.close();
        serverSocket.close();
        assert true;
    }

    /**
     * Producers are VMSs that intend to send data
     */
    @Test
    public void testConnectionToVmsAsProducer() throws Exception {

        // 1 - assemble a producer network node
        NetworkAddress producer = new NetworkAddress("localhost", 1086);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        NetworkNode vmsToConnectTo =  new NetworkNode("localhost", 1087);
        // 2 - start the vms 1. don't need to pass producer info to vms 1, since vms is always waiting for producer
        VmsCtx vmsCtx = loadMicroservice(
                vmsToConnectTo,
                null,
                true,
                new VmsEmbeddedInternalChannels(),
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 2 - connect
        InetSocketAddress address = new InetSocketAddress(vmsToConnectTo.host, vmsToConnectTo.port);
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
        channel.setOption(TCP_NODELAY, true);
        channel.setOption(SO_KEEPALIVE, true);
        channel.connect(address).get();

        logger.info("Connected. Now sending presentation.");

        // 3 - the handshake protocol
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        String dataAndInputSchema = "{}";
        String outputSchema = "{\"in\":{\"eventName\":\"in\",\"columnNames\":[\"id\"],\"columnDataTypes\":[\"INT\"]}}";
        Presentation.writeVms(buffer,producer,"example-producer", 1,0,0, dataAndInputSchema,dataAndInputSchema,outputSchema);
        buffer.flip();

        int numberOfBytes = buffer.limit();

        Future<Integer> res = channel.write(buffer);

        logger.info("Presentation sent.");

        int result = res.get();

        vmsCtx.stop();

        // 4 - how to know it was successful?
        assert result == numberOfBytes;
    }

    @Test
    public void testConnectionToVmsAsLeader() throws Exception {
        // 1 - assemble a producer network node
        NetworkAddress fakeLeader = new NetworkAddress("localhost", 1086);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        NetworkNode vmsToConnectTo =  new NetworkNode("localhost", 1087);
        // 2 - start the vms 1. don't need to pass producer info to vms 1, since vms is always waiting for producer
        VmsCtx vmsCtx = loadMicroservice(
                vmsToConnectTo,
                null,
                true,
                new VmsEmbeddedInternalChannels(),
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 2 - connect
        InetSocketAddress address = new InetSocketAddress(vmsToConnectTo.host, vmsToConnectTo.port);
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
        channel.setOption(TCP_NODELAY, true);
        channel.setOption(SO_KEEPALIVE, true);
        channel.connect(address).get();

        logger.info("Connected. Now sending presentation.");

        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        Presentation.writeServer( buffer, new ServerIdentifier(fakeLeader.host, fakeLeader.port),  true);

        // write output queues
        Set<String> queues = Set.of("out1","out2");
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
        Presentation.writeQueuesToSubscribeTo(buffer, queues, serdes);
        buffer.flip();
        channel.write(buffer).get();
        buffer.clear();

        // 4 - read metadata sent by vms
        channel.read(buffer).get();
        buffer.position(2);
        VmsNode vms = Presentation.readVms(buffer, serdes);

        vmsCtx.stop();

        // 5 - check whether the presentation sent is correct
        assert vms.inputEventSchema.containsKey("in") &&
                vms.outputEventSchema.containsKey("out1") &&
                vms.outputEventSchema.containsKey("out2");

    }

    @Test
    public void testReceiveBatchFromVmsAsLeader() throws Exception {

        ConsumerVms fakeLeader = new ConsumerVms("localhost", 1088);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        NetworkNode vmsToConnectTo =  new NetworkNode("localhost", 1089);
        // 2 - start the vms 1. don't need to pass producer info to vms 1, since vms is always waiting for producer
        VmsCtx vmsCtx = loadMicroservice(
                vmsToConnectTo,
                null,
                true,
                new VmsEmbeddedInternalChannels(),
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 2 - connect
        InetSocketAddress address = new InetSocketAddress(vmsToConnectTo.host, vmsToConnectTo.port);
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
        channel.setOption(TCP_NODELAY, true);
        channel.setOption(SO_KEEPALIVE, true);
        channel.connect(address).get();

        logger.info("Connected. Now sending presentation.");

        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        Presentation.writeServer( buffer, new ServerIdentifier(fakeLeader.host, fakeLeader.port),  true);

        // write output queues
        Set<String> queues = Set.of("out1","out2");
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
        Presentation.writeQueuesToSubscribeTo(buffer, queues, serdes);
        buffer.flip();
        channel.write(buffer).get();
        buffer.clear();

        // 4 - read metadata sent by vms
        channel.read(buffer).get();
        buffer.clear(); //can discard since the previous test already checks the correctness

        // 5 - send event input
        InputEventExample1 eventExample = new InputEventExample1(1);
        String inputPayload = serdes.serialize(eventExample, InputEventExample1.class);

        Map<String,Long> precedenceMap = new HashMap<>();
        precedenceMap.put("example1", 0L);

        TransactionEvent.Payload eventInput = TransactionEvent.Payload.of(1,0,"in", inputPayload, serdes.serializeMap(precedenceMap));
        TransactionEvent.write(buffer, eventInput);
        buffer.flip();
        channel.write(buffer).get(); // no need to wait

        logger.info("Input event sent");

        // 6 - read batch of events
        buffer.clear();
        channel.read(buffer).get();

        logger.info("Batch received");

        // 9 - assert the batch of events is received
        buffer.position(0);
        byte messageType = buffer.get();
        assert messageType == BATCH_OF_TRANSACTION_EVENTS;
        int size = buffer.getInt();
        assert size == 2;

        TransactionEvent.Payload payload;

        for(int i = 0; i < size; i++){
            buffer.get(); // exclude event
            payload = TransactionEvent.readFromLeader(buffer);
            Class<?> clazz =  payload.event().equalsIgnoreCase("out1") ? OutputEventExample1.class : OutputEventExample2.class;
            Object obj = serdes.deserialize(payload.payload(), clazz);
            assert !payload.event().equalsIgnoreCase("out1") || obj instanceof OutputEventExample1;
            assert !payload.event().equalsIgnoreCase("out2") || obj instanceof OutputEventExample2;
        }

        channel.close();
        vmsCtx.stop();

    }

    // @Test
//    public void testCrossVmsTransactionWithEventHandler() {
//        // set up a consumer to subscribe to both out1 and out2
//        //  set up producer too, check if events are received end-to-end through the network
//        assert true;
//    }

    // @Test
//    public void testBatchCompletion(){
//        assert true;
//    }

    // @Test
//    public void testReceiveEventFromLeader(){
//        assert true;
//    }

}
