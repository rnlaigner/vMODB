package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.compressing.CompressingUtils;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.complex.VmsComplexTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.events.InputEventExample1;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample1;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample2;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample3;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.IChannel;
import dk.ku.di.dms.vms.web_common.channel.PipeChannel;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.COMPRESSED_BATCH_OF_EVENTS;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

/**
 * Test vms event handler
 * a) Are events being ingested correctly?
 * b) are events being outputted correctly?
 * This list below is responsibility of another test class:
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
        VmsEventHandler eventHandler,
        VmsComplexTransactionScheduler scheduler) {
        public void stop() {
            this.scheduler.stop();
            this.eventHandler.stop();
        }
    }

    private static final System.Logger logger = System.getLogger(EventHandlerTest.class.getName());
    private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

    @Test
    public void basicCompressedBatch(){
        var source = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        List<TransactionEvent.PayloadRaw> list = new ArrayList<>();
        var event = TransactionEvent.of(1, 1, "test",
                serdes.serialize( Map.of("test","test"), Map.class), "{ }");
        for(int i = 1; i <= 10; i++){
            list.add(event);
        }
        BatchUtils.assembleBatchOfEvents( 10, list, source);
        source.flip();
        var compressed = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        var decompressed = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        int maxSize = source.remaining();
        // CompressingUtils.compress(source, 0, source.limit(), compressed, 0, maxSize);
        CompressingUtils.compress(source, compressed);
        compressed.flip();
        CompressingUtils.decompress(compressed, 0,  decompressed, 0, maxSize);
        decompressed.limit(maxSize);
        decompressed.position(0);
        Assert.assertEquals( BATCH_OF_EVENTS, decompressed.get() );
    }

    @Test
    public void testCompressedBatch() throws ExecutionException, InterruptedException {
        Supplier<IChannel> supplier = new Supplier<>() {
            PipeChannel pipeChannel = PipeChannel.create();
            @Override
            public IChannel get() {
                return this.pipeChannel;
            }
        };

        ConsumerVmsWorker consumerVmsWorker = ConsumerVmsWorker.build(
                new VmsNode("0.0.0.0", 0, "test1", 0, 0, -1, Map.of(), Map.of(), Map.of()),
                new IdentifiableNode("test2", "0.0.0.0", 0),
                supplier,
                new VmsEventHandler.VmsHandlerOptions(0, MemoryUtils.DEFAULT_PAGE_SIZE, 0, "default", 1, 0, 1, true, false, false),
                serdes
        );

        Thread.ofPlatform().name("vms-consumer-test")
                .inheritInheritableThreadLocals(false)
                .start(consumerVmsWorker);

        // generate few events
        var event = TransactionEvent.of(1, 1, "test",
                serdes.serialize( Map.of("test","test"), Map.class), "{ }");
        consumerVmsWorker.queue(event);
        consumerVmsWorker.queue(event);
        consumerVmsWorker.queue(event);
        consumerVmsWorker.queue(event);
        consumerVmsWorker.queue(event);

        sleep(3000);

        // and then get compressed output
        var compressed = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        supplier.get().read(compressed).get();
        compressed.flip();

        byte messageType = compressed.get();
        byte nodeType = compressed.get();
        Presentation.readVms(compressed, serdes);

        messageType = compressed.get();
        Assert.assertEquals(COMPRESSED_BATCH_OF_EVENTS, messageType);
        int bufferSize = compressed.getInt();
        int count = compressed.getInt();
        int maxLength = compressed.getInt();
        while(compressed.remaining() < bufferSize - (1 + Integer.BYTES + Integer.BYTES + Integer.BYTES)){
            supplier.get().read(compressed).get();
        }

        ByteBuffer decompressedBuffer = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        decompressedBuffer.limit(maxLength);
        decompressedBuffer.position(0);
        CompressingUtils.decompress(compressed, compressed.position(), decompressedBuffer, 0, maxLength);
        decompressedBuffer.limit(maxLength);
        decompressedBuffer.flip();

        TransactionEvent.PayloadRaw payload = null;
        int i = 0;
        while (i < count) {
            payload = TransactionEvent.read(decompressedBuffer);
            String eventStr = new String(payload.event(), StandardCharsets.UTF_8);
            i++;
        }

        Assert.assertNotNull(payload);
    }

    /**
     * Facility to load virtual microservice instances.
     * For tests, it may the case two VMSs are in the same package. That leads to
     * buggy metadata being loaded (i.e., swapped in-and-out events)
     * To avoid that, this method takes into consideration the inputs and output events
     * to be discarded for a given
     */
    private static VmsCtx loadMicroservice(NetworkNode node,
                                           boolean eventHandlerActive,
                                           VmsEmbedInternalChannels vmsInternalPubSubService, String vmsName,
                                           List<String> inToDiscard, List<String> outToDiscard, List<String> inToSwap, List<String> outToSwap)
            throws Exception {

        VmsRuntimeMetadata vmsMetadata = VmsMetadataLoader.load("dk.ku.di.dms.vms.sdk.embed");

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

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        VmsComplexTransactionScheduler scheduler = VmsComplexTransactionScheduler.build(
                "test", vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(), null);

        VmsNode vmsIdentifier = new VmsNode(
                node.host, node.port, vmsName,
                1, 0, 0,
                vmsMetadata.dataModel(),
                vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

        VmsEventHandler eventHandler = VmsEventHandler.build(
                vmsIdentifier, new ITransactionManager() {
                },
                vmsInternalPubSubService, vmsMetadata,
                VmsApplicationOptions.build(null, 0, null),
                new IHttpHandler() { },
                serdes);

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
        VmsEmbedInternalChannels channelForAddingInput = new VmsEmbedInternalChannels();

        // microservice 1
        VmsCtx vmsCtx = loadMicroservice(
                new NetworkNode("localhost", 1080),
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
        VmsEmbedInternalChannels channelForGettingOutput = new VmsEmbedInternalChannels();

        // microservice 2
        VmsCtx vmsCtx2 = loadMicroservice(
                new NetworkNode("localhost", 1081),
                false,
                channelForGettingOutput,
                "example2",
                inToDiscard,
                outToDiscard,
                inToSwap,
                outToSwap);

        InputEventExample1 eventExample = new InputEventExample1(0);
        InboundEvent event = new InboundEvent(1,0,1,"in", InputEventExample1.class, eventExample);
        channelForAddingInput.transactionInputQueue().add(event);

        var input1ForVms2 = channelForAddingInput.transactionOutputQueue().poll();
        assert input1ForVms2 != null;

        for(var res : input1ForVms2.getOutboundEventResults()){
            Class<?> clazz =  res.outputQueue().equalsIgnoreCase("out1") ? OutputEventExample1.class : OutputEventExample2.class;
            InboundEvent event_ = new InboundEvent(1,0,1,res.outputQueue(),clazz,res.output());
            channelForGettingOutput.transactionInputQueue().add(event_);
        }

        // subscribe to output event out3
        IVmsTransactionResult result = channelForGettingOutput.transactionOutputQueue().poll();

        assert result != null && result.getOutboundEventResults() != null && !result.getOutboundEventResults().isEmpty();

        vmsCtx.stop();
        vmsCtx2.stop();

        assert ((OutputEventExample3) result.getOutboundEventResults().getFirst().output()).id == 2;
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
        IdentifiableNode me = new IdentifiableNode("unknown", "localhost", 1082);

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

        Deque<IdentifiableNode> consumerSet = new ArrayDeque<>();
        consumerSet.add(me);
        Map<String, Deque<IdentifiableNode>> eventToConsumersMap = new HashMap<>();
        eventToConsumersMap.put("out1", consumerSet);
        eventToConsumersMap.put("out2", consumerSet);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        // 3 - start the vms 1. make sure the info that this is a vms consumer is passed as parameter
        VmsCtx vmsCtx = loadMicroservice(
                new NetworkNode("localhost", 1083),
                true,
                new VmsEmbedInternalChannels(),
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
        IdentifiableNode me = new IdentifiableNode("unknown","localhost", 1084);

        // 2 - start a socket in the prescribed node
        AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(2));
        AsynchronousServerSocketChannel serverSocket = AsynchronousServerSocketChannel.open(group);
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        // 3 - set consumer of events
        Deque<IdentifiableNode> consumerSet = new ArrayDeque<>();
        consumerSet.add(me);
        Map<String, Deque<IdentifiableNode>> eventToConsumersMap = new HashMap<>();
        eventToConsumersMap.put("out1", consumerSet);
        eventToConsumersMap.put("out2", consumerSet);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        NetworkNode vmsToConnectTo =  new NetworkNode("localhost", 1085);
        // 4 - start the vms 1. don't need to pass producer info to vms 1, since vms is always waiting for producer
        VmsCtx vmsCtx = loadMicroservice(
                        vmsToConnectTo,
                        true,
                        new VmsEmbedInternalChannels(),
                        "example1",
                        inToDiscard,
                        outToDiscard,
                        inToSwap,
                        inToDiscard);

        // 5 - accept connection but just ignore the payload. the above test is already checking this
        AsynchronousSocketChannel channel = serverSocket.accept().get(); // ignore result

        logger.log(INFO, "Connection accepted from VMS");

        // 6 - read the presentation to set the writer of the producer free to write results
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        channel.read(buffer).get();
        buffer.clear(); // just ignore the presentation as we test this in the previous test

        logger.log(INFO, "Presentation received from VMS");

        // 7 - send event input
        InputEventExample1 eventExample = new InputEventExample1(1);
        var inputPayload = serdes.serialize(eventExample, InputEventExample1.class);

        Map<String,Long> precedenceMap = new HashMap<>();
        precedenceMap.put("example1", 0L);

        TransactionEvent.PayloadRaw eventInput = TransactionEvent.of(1,1,"in", inputPayload, serdes.serializeMap(precedenceMap));
        TransactionEvent.writeWithinBatch(buffer, eventInput);
        buffer.flip();
        channel.write(buffer).get(); // no need to wait
        buffer.clear();

        logger.log(INFO, "Input event sent");

        /*
         8 - listen from the internal channel. may take some time because of the batch commit scheduler
         why also checking the output? to see if the event sent is correctly processed
          hanging forever sometimes. not deterministic
          study possible problem: locking my thread pool: slide 44
          https://openjdk.org/projects/nio/presentations/TS-4222.pdf
          With an executor passed as a group, it started working....
        */
        channel.read(buffer).get( 25, TimeUnit.SECONDS );
        // channel.read(buffer).get( );

        logger.log(INFO, "Batch received");

        // 9 - assert the batch of events is received
        buffer.position(0);
        byte messageType = buffer.get();
        assert messageType == BATCH_OF_EVENTS;
        int size = buffer.getInt();
        assert size == 2;

        TransactionEvent.PayloadRaw payload;
        for(int i = 0; i < size; i++){
            buffer.get(); // exclude event
            payload = TransactionEvent.read(buffer);
            String event = new String(payload.event(), StandardCharsets.UTF_8);
            Class<?> clazz = event.equalsIgnoreCase("out1") ? OutputEventExample1.class : OutputEventExample2.class;
            Object obj = serdes.deserialize(payload.payload(), clazz);
            assert !event.equalsIgnoreCase("out1") || obj instanceof OutputEventExample1;
            assert !event.equalsIgnoreCase("out2") || obj instanceof OutputEventExample2;
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
                true,
                new VmsEmbedInternalChannels(),
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 2 - connect
        InetSocketAddress address = new InetSocketAddress(vmsToConnectTo.host, vmsToConnectTo.port);
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
        NetworkUtils.configure(channel, 4096);
        channel.connect(address).get();

        logger.log(INFO, "Connected. Now sending presentation.");

        // 3 - the handshake protocol
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        String dataAndInputSchema = "{}";
        String outputSchema = "{\"in\":{\"eventName\":\"in\",\"columnNames\":[\"id\"],\"columnDataTypes\":[\"INT\"]}}";
        Presentation.writeVms(buffer,producer,"example-producer", 1,0,0, dataAndInputSchema,dataAndInputSchema,outputSchema);
        buffer.flip();

        int numberOfBytes = buffer.limit();

        Future<Integer> res = channel.write(buffer);

        logger.log(INFO, "Presentation sent.");

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
                true,
                new VmsEmbedInternalChannels(),
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 2 - connect
        InetSocketAddress address = new InetSocketAddress(vmsToConnectTo.host, vmsToConnectTo.port);
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
        NetworkUtils.configure(channel, 4096);
        channel.connect(address).get();

        logger.log(INFO, "Connected. Now sending presentation.");

        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        Presentation.writeServer( buffer, new ServerNode(fakeLeader.host, fakeLeader.port),  true);

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

        IdentifiableNode fakeLeader = new IdentifiableNode("unknown", "localhost", 1088);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        NetworkNode vmsToConnectTo =  new NetworkNode("localhost", 1089);
        // 2 - start the vms 1. don't need to pass producer info to vms 1, since vms is always waiting for producer
        VmsCtx vmsCtx = loadMicroservice(
                vmsToConnectTo,
                true,
                new VmsEmbedInternalChannels(),
                "example1",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 2 - connect
        InetSocketAddress address = new InetSocketAddress(vmsToConnectTo.host, vmsToConnectTo.port);
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
        NetworkUtils.configure(channel, 4096);
        channel.connect(address).get();

        logger.log(INFO, "Connected. Now sending presentation.");

        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        Presentation.writeServer( buffer, new ServerNode(fakeLeader.host, fakeLeader.port),  true);

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
        var inputPayload = serdes.serialize(eventExample, InputEventExample1.class);

        Map<String,Long> precedenceMap = new HashMap<>();
        precedenceMap.put("example1", 0L);

        TransactionEvent.PayloadRaw eventInput = TransactionEvent.of(1,0,"in", inputPayload, serdes.serializeMap(precedenceMap));
        TransactionEvent.writeWithinBatch(buffer, eventInput);
        buffer.flip();
        channel.write(buffer).get(); // no need to wait

        logger.log(INFO, "Input event sent");

        // 6 - read batch of events
        buffer.clear();
        channel.read(buffer).get();

        logger.log(INFO, "Batch received");

        // 9 - assert the batch of events is received
        buffer.position(0);
        byte messageType = buffer.get();
        assert messageType == BATCH_OF_EVENTS;
        int size = buffer.getInt();
        assert size == 2;

        TransactionEvent.PayloadRaw payload;

        for(int i = 0; i < size; i++){
            buffer.get(); // exclude event
            payload = TransactionEvent.read(buffer);
            String event = new String( payload.event(), StandardCharsets.UTF_8);
            Class<?> clazz = event.equalsIgnoreCase("out1") ? OutputEventExample1.class : OutputEventExample2.class;
            Object obj = serdes.deserialize(payload.payload(), clazz);
            assert !event.equalsIgnoreCase("out1") || obj instanceof OutputEventExample1;
            assert !event.equalsIgnoreCase("out2") || obj instanceof OutputEventExample2;
        }

        channel.close();
        vmsCtx.stop();

    }

}
