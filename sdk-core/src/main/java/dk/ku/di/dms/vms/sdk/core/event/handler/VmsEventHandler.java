package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.net.StandardSocketOptions.*;

/**
 * Rate of consumption of events and data are different, as well as their payload
 */
public final class VmsEventHandler extends SignalingStoppableRunnable {

    /** EXECUTOR SERVICE (for socket channels) **/
    private final ExecutorService executorService;
    private final String vmsName;

    /** SOCKET CHANNEL OPERATIONS **/
    private Future<Integer> futureEvent;

    private Future<Integer> futureData;

    private AsynchronousSocketChannel eventChannel;

    private AsynchronousSocketChannel dataChannel;

    // store off-heap to avoid overhead
    private final ByteBuffer eventReadBuffer;

    // to avoid allocation of byte buffer on every write
    private final ByteBuffer eventWriteBuffer;

    private final ByteBuffer dataSocketBuffer;

    private final Map<Long, DataRequestEvent> pendingRequestsMap;

    /** METADATA **/
    private final VmsRuntimeMetadata vmsMetadata;

    /** SERIALIZATION **/
    private final IVmsSerdesProxy serdes;

    public VmsEventHandler(String vmsName, IVmsInternalChannels vmsInternalQueues, VmsRuntimeMetadata vmsMetadata, IVmsSerdesProxy serdes, ExecutorService executorService) {

        super();
        this.vmsName = vmsName;
        this.vmsMetadata = vmsMetadata;

        // event
        this.eventReadBuffer = null; //BufferManager.loanByteBuffer();
        this.eventWriteBuffer = null; //BufferManager.loanByteBuffer();

        // data
        this.dataSocketBuffer = null; //BufferManager.loanByteBuffer();

        this.serdes = serdes;

        this.executorService = executorService;

        this.pendingRequestsMap = new HashMap<>();
    }

    /**
     * Maybe a new design would better fit this class.
     * We can reuse the socket threads to parse the incoming data and deliver back to the correct queues.
     * Otherwise, the parsing of incoming bytes becomes a bottleneck (single threaded).
     * This thread would make the job to be a conciliator between the application code and the external world
     * For instance, delivering events to the socket threads and returning data to the code...
     */
    @Override
    public void run() {

        initialize();

        // while not stopped
        while(isRunning()){

            // do we have an input payload to process?
            processIncomingEvent();

            // do we have an input data (from sidecar) to process?
            processIncomingData();

            // do we have an output payload to dispatch? can be another thread, the sdk is dominated by IOs
            dispatchOutputEvents();

            // do we have data requests to the sidecar?
            dispatchDataRequests();

        }

    }

    private void processIncomingEvent(){

        if(futureEvent.isDone()){

            try {
                futureEvent.get();

//                TransactionalEvent event = serdes.deserializeToTransactionalEvent( eventReadBuffer.array() );
//                inputQueue.add( event );

            } catch (InterruptedException | ExecutionException e) {
                logger.info("ERROR: "+e.getLocalizedMessage());
            } finally {
                eventReadBuffer.clear();
                futureEvent = eventChannel.read(eventReadBuffer);
            }

        }

    }

    /**
     * Everything should be a DTO
     */
    private void processIncomingData() {

        if(futureData.isDone()){

            try {
                futureData.get();

                // that would be the fulfillment of the future
                DataResponseEvent dataResponse = serdes.deserializeToDataResponseEvent( dataSocketBuffer.array() );

//                responseMap.put( dataResponse.identifier, dataResponse );

                DataRequestEvent dataRequestEvent = pendingRequestsMap.remove( dataResponse.identifier );

                // notify the thread that is waiting for this data
                synchronized (dataRequestEvent){
                    dataRequestEvent.notify();
                }

            } catch (InterruptedException | ExecutionException e) {
                logger.warn("ERR: on reading the new data: "+ e.getLocalizedMessage());
            } finally {

                dataSocketBuffer.clear();
                futureData = eventChannel.read(eventReadBuffer);
            }

        }

    }

    private void dispatchOutputEvents() {

//        while( !outputQueue.isEmpty() ){
//
//           TransactionalEvent outputEvent = outputQueue.poll();
//
//            byte[] bytes = serdes.serializeTransactionalEvent( outputEvent );
//            eventChannel.write(ByteBuffer.wrap(bytes));
//
//        }

    }

    private void dispatchDataRequests() {

//        while( !requestQueue.isEmpty() ){
//
//            DataRequestEvent dataRequestEvent = requestQueue.poll();
//
//            pendingRequestsMap.put( dataRequestEvent.identifier, dataRequestEvent );
//
//            byte[] bytes = serdes.serializeDataRequestEvent( dataRequestEvent );
//
//            eventWriteBuffer.clear();
//            eventWriteBuffer.put( bytes );
//
//            eventChannel.write(eventWriteBuffer);
//
//        }

    }

    /**
     * If no exception is thrown, the initialization worked
     * In other words, the workers can exchange data
     */
    private void initialize() {

        // PHASE 0 -- initialize asynchronous channel group
        AsynchronousChannelGroup eventGroup;
        AsynchronousChannelGroup dataGroup;

        try {
            eventGroup = AsynchronousChannelGroup.withThreadPool( executorService );
            dataGroup = AsynchronousChannelGroup.withThreadPool( executorService );
        } catch (IOException e) {
            throw new RuntimeException("ERROR on setting up asynchronous channel group.");
        }

        // PHASE 1 - connect both channels
        try {

            // event first
            connectToSidecar("event", eventGroup).get();

            // then data schema
            connectToSidecar("data", dataGroup).get();

        } catch( ExecutionException | InterruptedException e ){
            throw new RuntimeException("ERROR on connecting to sidecar.");
        }


        // PHASE 2- send schemas, this is compared to a handshake in a WebSocket protocol

        // send event schema
        String eventSchemaStr = serdes.serializeEventSchema( vmsMetadata.inputEventSchema() );
        eventChannel.write( ByteBuffer.wrap(eventSchemaStr.getBytes(StandardCharsets.UTF_8)) );

        // send data schema
        String dataSchemaStr = serdes.serializeDataSchema( vmsMetadata.tableSchema() );
        dataChannel.write( ByteBuffer.wrap(dataSchemaStr.getBytes(StandardCharsets.UTF_8)) );

        // PHASE 3 -  get confirmation whether the schemas are fine

        // to avoid checking for nullability in the payload loop
        // besides, the framework must wait for events either way
        eventReadBuffer.clear();
        futureEvent = eventChannel.read(eventReadBuffer);

        dataSocketBuffer.clear();
        futureData = dataChannel.read(dataSocketBuffer);

        try {
            futureEvent.get();
            futureData.get();
        } catch( ExecutionException | InterruptedException e ){
            throw new RuntimeException("ERROR on retrieving handshake from sidecar.");
        }

//        SystemEvent eventSchemaResponse = serdes.deserializeSystemEvent( eventReadBuffer.array() );
//
//        if(eventSchemaResponse.op == 0){
//            throw new RuntimeException(eventSchemaResponse.message);
//        }
//
//        SystemEvent dataSchemaResponse = serdes.deserializeSystemEvent( dataSocketBuffer.array() );
//
//        if(dataSchemaResponse.op == 0){
//            throw new RuntimeException(dataSchemaResponse.message);
//        }

        // prepare futures
        eventReadBuffer.clear();
        futureEvent = eventChannel.read(eventReadBuffer);

        dataSocketBuffer.clear();
        futureData = dataChannel.read(dataSocketBuffer);

    }

    private void setDefaultSocketOptions( AsynchronousSocketChannel socketChannel ) throws IOException {

        socketChannel.setOption( TCP_NODELAY, Boolean.TRUE ); // false is the default value
        socketChannel.setOption( SO_KEEPALIVE, Boolean.TRUE );

        socketChannel.setOption( SO_SNDBUF, 1024 );
        socketChannel.setOption( SO_RCVBUF, 1024 );

        // TODO revisit these options
        socketChannel.setOption( SO_REUSEADDR, true );
        socketChannel.setOption( SO_REUSEPORT, true );
    }

    /**
     * TODO does this thread pool should be shared among web tasks and app-logic tasks?
     * @param channelName data or event
     * @param group socket group
     * @return Connection future
     */
    private Future<Void> connectToSidecar(String channelName, AsynchronousChannelGroup group){

        try {

            InetSocketAddress address = new InetSocketAddress("localhost", 80);

            if (channelName.equalsIgnoreCase("data")) {

                eventChannel = AsynchronousSocketChannel.open(group);
                setDefaultSocketOptions( eventChannel );
                return eventChannel.connect(address);
            } else{
                dataChannel = AsynchronousSocketChannel.open(group);
                setDefaultSocketOptions( dataChannel );

                // FIXME send vmsmetada as part of the connection

                String eventSchemaStr = serdes.serializeEventSchema( vmsMetadata.inputEventSchema() );
//                eventChannel.write( ByteBuffer.wrap(eventSchemaBytes) );
                eventWriteBuffer.put( eventSchemaStr.getBytes(StandardCharsets.UTF_8) );
                // TODO send only one vms schema, later we figure out how to deal with many... these are the case to explore many-core architectures...


                dataChannel.connect(address, eventWriteBuffer, new CompletionHandler<>() {

                    @Override
                    public void completed(Void result, ByteBuffer attachment) {

                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment) {

                    }

                });

                return null;

            }

        } catch (IOException e) {
            logger.info("ERROR: "+e.getLocalizedMessage());
            throw new RuntimeException("Error on setting socket options for channel "+channelName);
        }

    }

}
