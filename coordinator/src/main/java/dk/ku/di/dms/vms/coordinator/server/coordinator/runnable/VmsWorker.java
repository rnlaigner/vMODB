package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitAck;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitCommand;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchCommitInfo;
import dk.ku.di.dms.vms.modb.common.schema.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.control.ConsumerContext;
import dk.ku.di.dms.vms.modb.common.schema.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

import static dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.IVmsWorker.State.*;
import static dk.ku.di.dms.vms.modb.common.schema.Constants.*;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

final class VmsWorker extends StoppableRunnable implements IVmsWorker {

    private final Logger logger;

    private final ServerIdentifier me;

    // the vms this worker is responsible for
    private final NetworkAddress consumerVms;

    // defined after presentation being sent by the actual vms
    private VmsNode vmsNode;

    private State state;

    private final IVmsSerdesProxy serdesProxy;

    private ByteBuffer readBuffer;

    private ByteBuffer writeBuffer;

    /**
     * Queues to inform coordinator about an event
     */

    private final BlockingQueue<Coordinator.Message> coordinatorQueue;

    private final AsynchronousChannelGroup group;

    private AsynchronousSocketChannel channel;

    static VmsWorker buildAsStarter(// coordinator reference
                                    ServerIdentifier me,
                                    // the vms this thread is responsible for
                                    NetworkAddress consumerVms,
                                    // shared data structure to communicate messages to coordinator
                                    BlockingQueue<Coordinator.Message> coordinatorQueue,
                                    // the group for the socket channel
                                    AsynchronousChannelGroup group,
                                    IVmsSerdesProxy serdesProxy) {
        return new VmsWorker(me, consumerVms, coordinatorQueue, null, group, null, serdesProxy);
    }

    static VmsWorker build(
            ServerIdentifier me,
            NetworkAddress consumerVms,
            BlockingQueue<Coordinator.Message> coordinatorQueue,
            // the socket channel already established
            AsynchronousSocketChannel channel,
            AsynchronousChannelGroup group,
            ByteBuffer buffer, // to continue reading presentation
            IVmsSerdesProxy serdesProxy) {
        return new VmsWorker(me, consumerVms, coordinatorQueue, channel, group, buffer, serdesProxy);
    }

    private VmsWorker(// coordinator reference
                      ServerIdentifier me,
                      // the vms this thread is responsible for
                      NetworkAddress consumerVms,
                      // events to share with coordinator
                      BlockingQueue<Coordinator.Message> coordinatorQueue,
                      // the group for socket channel
                      AsynchronousSocketChannel channel,
                      AsynchronousChannelGroup group,
                      ByteBuffer readBuffer,
                      IVmsSerdesProxy serdesProxy) {
        this.me = me;
        this.state = State.NEW;
        this.consumerVms = consumerVms;

        // shared by many vms workers
        this.coordinatorQueue = coordinatorQueue;

        this.channel = channel;
        this.group = group;

        this.readBuffer = readBuffer;
        this.serdesProxy = serdesProxy;

        this.logger = LoggerFactory.getLogger("vms-worker-"+consumerVms.toString());
    }

    public void initHandshakeProtocol(){

        // a vms has tried to connect
        if(this.channel != null) {
            this.state = CONNECTION_ESTABLISHED;
            processVmsIdentifier();
            this.readBuffer.clear();
            this.channel.read( this.readBuffer, null, new VmsReadCompletionHandler() );
            return;
        }

        if(this.readBuffer == null) {
            this.readBuffer = MemoryManager.getTemporaryDirectBuffer();
        }

        // connect to starter vms
        try {
            this.channel = AsynchronousSocketChannel.open(this.group);
            this.channel.setOption(TCP_NODELAY, true);
            this.channel.setOption(SO_KEEPALIVE, true);
            this.channel.connect(this.consumerVms.asInetSocketAddress()).get();

            this.state = CONNECTION_ESTABLISHED;

            this.readBuffer.clear();

            // write presentation
            Presentation.writeServer(this.readBuffer, this.me, true );
            this.readBuffer.flip();
            this.channel.write(this.readBuffer).get();

            this.state = State.LEADER_PRESENTATION_SENT;
            this.readBuffer.clear();

            // set read handler here
            this.channel.read( this.readBuffer, null, new VmsReadCompletionHandler() );

        } catch (ExecutionException | InterruptedException e) {

            if (this.state == State.NEW) {
                // forget about it, let the vms connect then...
                this.logger.warn("Failed to connect to a known VMS: " + consumerVms);
                this.state = State.CONNECTION_FAILED;
            } else if(this.state == CONNECTION_ESTABLISHED) {
                this.state = LEADER_PRESENTATION_SEND_FAILED;
                // check if connection is still online. if so, try again
                // otherwise, retry connection in a few minutes
                if(this.channel.isOpen()){
                    // try again? what is he problem?
                    logger.warn("It was not possible to send a presentation message, although the channel is open. The connection will be closed now.");
                    try { this.channel.close(); } catch (IOException ignored) { }
                } else {
                    this.logger.warn("It was not possible to send a presentation message and the channel is not open. Check the consumer VMS: " + consumerVms);
                }
            } else {
                this.logger.warn("Cannot find the root problem. Please have a look: "+e.getMessage());
            }

            // important for consistency of state (if debugging, good to see the code controls the thread state)
            this.stop();

        } catch (Exception e){
            this.logger.warn("Cannot find the root problem. Please have a look: "+e.getMessage());
            this.stop();
        }

    }

    @Override
    public void run() {
        initHandshakeProtocol();
        eventLoop();
    }

    /**
     * Event loop. Put in another method to avoid a long run method
     */
    private void eventLoop() {
        while (this.isRunning()){
            try {
                Message workerMessage = this.workerQueue.take();
                switch (workerMessage.type()){
                    // in order of probability
                    case SEND_BATCH_OF_EVENTS -> this.sendBatchOfEvents(workerMessage, false);
                    case SEND_BATCH_OF_EVENTS_WITH_COMMIT_INFO -> this.sendBatchOfEvents(workerMessage, true);
                    case SEND_BATCH_COMMIT_COMMAND -> this.sendBatchCommitRequest(workerMessage);
                    case SEND_TRANSACTION_ABORT -> this.sendTransactionAbort(workerMessage);
                    case SEND_CONSUMER_CTX -> this.sendConsumerSet(workerMessage);
                }
            } catch (InterruptedException e) {
                logger.warn("This thread has been interrupted. Cause: "+e.getMessage());
                this.stop();
            }
        }
    }

    private void sendTransactionAbort(Message workerMessage) {
        TransactionAbort.Payload tidToAbort = workerMessage.asTransactionAbort();
        TransactionAbort.write(this.writeBuffer, tidToAbort);
        this.writeBuffer.flip();
        try {
            this.channel.write(this.writeBuffer).get();
            this.logger.warn("Transaction abort sent to: " + this.consumerVms);
        } catch (InterruptedException | ExecutionException e){
            if(channel.isOpen()){
                this.logger.warn("Transaction abort write has failed but channel is open. Trying to write again to: "+consumerVms+" in a while");
                this.workerQueue.add(workerMessage);
            } else {
                this.logger.warn("Transaction abort write has failed and channel is closed: "+this.consumerVms);
                this.stop(); // no reason to continue the loop
            }
        } finally {
            this.writeBuffer.clear();
        }

    }

    private void sendBatchCommitRequest(Message workerMessage) {
        BatchCommitCommand.Payload commitRequest = workerMessage.asBatchCommitCommand();
        BatchCommitCommand.write(this.writeBuffer, commitRequest);
        this.writeBuffer.flip();

        try {
            this.channel.write(writeBuffer).get();
            this.logger.warn("Commit request sent to: " + consumerVms);
        } catch (InterruptedException | ExecutionException e){
            if(channel.isOpen()){
                this.logger.warn("Commit request write has failed but channel is open. Trying to write again to: "+consumerVms+" in a while");
                this.workerQueue.add(workerMessage);
            } else {
                this.logger.warn("Commit request write has failed and channel is closed: "+consumerVms);
                this.stop(); // no reason to continue the loop
            }
        } finally {
            this.writeBuffer.clear();
        }
    }

    private void sendConsumerSet(Message workerMessage) {
        // the first or new information
        if(this.state == VMS_PRESENTATION_PROCESSED) {
            // now initialize the write buffer
            this.writeBuffer = MemoryManager.getTemporaryDirectBuffer();
            this.state = CONSUMER_CTX_READY_FOR_SENDING;
            this.logger.info("Consumer set will be established for the first time: "+consumerVms);
        } else if(this.state == CONSUMER_EXECUTING){
            this.logger.info("Consumer set is going to be updated for: "+consumerVms);
        } else if(this.state == CONSUMER_CTX_SENDING_FAILED){
            this.logger.info("Consumer set, another attempt to write to: "+consumerVms);
        } // else, nothing...

        String vmsConsumerSet = workerMessage.asVmsConsumerSet();

        ConsumerContext.write(this.writeBuffer, vmsConsumerSet);
        this.writeBuffer.flip();

        try {
            Integer result = this.channel.write(this.writeBuffer).get();
            if (result == this.writeBuffer.limit()) {
                if (this.state == CONSUMER_CTX_READY_FOR_SENDING) // or != CONSUMER_EXECUTING
                    this.state = CONSUMER_EXECUTING;
            } else {
                this.state = CONSUMER_CTX_SENDING_FAILED;
                this.workerQueue.add(workerMessage);
            }

            this.writeBuffer.clear();

        } catch (InterruptedException | ExecutionException e){
            this.state = CONSUMER_CTX_SENDING_FAILED;
            if (channel.isOpen()) {
                this.logger.warn("Write has failed but channel is open. Trying to write again to: " + consumerVms + " in a while");
                // just queue again
                this.workerQueue.add(workerMessage);
            } else {
                this.logger.warn("Write has failed and channel is closed: " + consumerVms);
                this.stop(); // no reason to continue the loop
            }
        }

    }

    /**
     * Reuses the thread from the socket thread pool, instead of assigning a specific thread
     * Removes thread context switching costs.
     * This thread should not block.
     * The idea is to decode the message and deliver back to socket loop as soon as possible
     * This thread must be set free as soon as possible, should not do long-running computation
     */
    private class VmsReadCompletionHandler implements CompletionHandler<Integer, Object> {

        // is it an abort, a commit response?
        // it cannot be replication because have opened another channel for that

        @Override
        public void completed(Integer result, Object connectionMetadata) {

            // decode message by getting the first byte
            byte type = readBuffer.get(0);
            readBuffer.position(1);

            switch (type) {

                case PRESENTATION -> {
                    if(vmsNode != null){
                        // in the future it can be an update of the vms schema
                        logger.warn("Presentation already received from this VMS.");
                    } else {
                       state = VMS_PRESENTATION_RECEIVED;// for the first time
                    }
                    processVmsIdentifier();
                    state = VMS_PRESENTATION_PROCESSED;
                }

                // from all terminal VMSs involved in the last batch
                case BATCH_COMPLETE -> {
                    // don't actually need the host and port in the payload since we have the attachment to this read operation...
                    BatchComplete.Payload response = BatchComplete.read(readBuffer);
                    // must have a context, i.e., what batch, the last?
                    coordinatorQueue.add( new Coordinator.Message( Coordinator.Type.BATCH_COMPLETE, response));
                    // if one abort, no need to keep receiving
                    // actually it is unclear in which circumstances a vms would respond no... probably in case it has not received an ack from an aborted commit response?
                    // because only the aborted transaction will be rolled back
                }
                case BATCH_COMMIT_ACK -> {
                    BatchCommitAck.Payload response = BatchCommitAck.read(readBuffer);
                    logger.info("Just logging it, since we don't necessarily need to wait for that. "+response);
                    coordinatorQueue.add( new Coordinator.Message( Coordinator.Type.BATCH_COMMIT_ACK, response));
                }
                case TX_ABORT -> {
                    // get information of what
                    TransactionAbort.Payload response = TransactionAbort.read(readBuffer);
                    coordinatorQueue.add( new Coordinator.Message( Coordinator.Type.TRANSACTION_ABORT, response));
                }
                case TRANSACTION_EVENT ->
                        logger.info("New event received from VMS");
                case BATCH_OF_TRANSACTION_EVENTS -> //
                        logger.info("New batch of events received from VMS");
                default ->
                        logger.warn("Unknown message received.");

            }
            readBuffer.clear();
            channel.read( readBuffer, null, this );
        }

        @Override
        public void failed(Throwable exc, Object attachment) {

            if(state == LEADER_PRESENTATION_SENT){
                state = VMS_PRESENTATION_RECEIVE_FAILED;
                if(channel.isOpen()){
                    logger.warn("It was not possible to receive a presentation message, although the channel is open.");
                }
                logger.warn("It was not possible to receive a presentation message and the channel is not open. Check the consumer VMS: "+consumerVms);
            } else {
                if (channel.isOpen()) {
                    logger.warn("Read has failed but channel is open. Trying to read again from: " + consumerVms);

                } else {
                    logger.warn("Read has failed and channel is closed: " + consumerVms);
                }
            }

            readBuffer.clear();
            channel.read(readBuffer, null, this);

        }

    }

    private void processVmsIdentifier() {
        // always a vms
        this.readBuffer.position(2);
        this.vmsNode = Presentation.readVms(readBuffer, serdesProxy);
        this.state = State.VMS_PRESENTATION_PROCESSED;
        // let coordinator aware this vms worker already has the vms identifier
        this.coordinatorQueue.add(new Coordinator.Message( Coordinator.Type.VMS_IDENTIFIER, new VmsContext(this.vmsNode, this)));
    }

    /**
     * Need to send the last batch too so the vms can safely start the new batch
     */
    private void sendBatchOfEvents(Message message, boolean includeCommitInfo) {
        BatchCommitInfo.Payload batchCommitInfo = message.asBatchOfEventsRequest();
        boolean thereAreEventsToSend = this.transactionEventsPerBatch(batchCommitInfo.batch()) == null;
        if(thereAreEventsToSend){
            if(includeCommitInfo){
                this.sendBatchedEvents(this.transactionEventsPerBatch(batchCommitInfo.batch()), batchCommitInfo);
            } else {
                this.sendBatchedEvents(this.transactionEventsPerBatch(batchCommitInfo.batch()));
            }
        } else if(includeCommitInfo){
            this.sendBatchCommitInfo(batchCommitInfo);
        }
    }

    private final List<TransactionEvent.Payload> events = new ArrayList<>();

    private void sendBatchCommitInfo(BatchCommitInfo.Payload batchCommitInfo){
        // then send only the batch commit info
        try {
            BatchCommitInfo.write(this.writeBuffer, batchCommitInfo);
            this.writeBuffer.flip();
            this.channel.write(this.writeBuffer).get();
            this.writeBuffer.clear();
        } catch (InterruptedException | ExecutionException e) {
            if(!this.channel.isOpen()) {
                this.vmsNode.off();
            }
            this.writeBuffer.clear();
        }

    }

    private void sendBatchedEvents(BlockingDeque<TransactionEvent.Payload> eventsToSendToVms){
        eventsToSendToVms.drainTo(this.events);
        int remaining = BatchUtils.assembleBatchPayload( this.events.size(), this.events, this.writeBuffer);
        while(remaining > 0) {
            try {
                this.writeBuffer.flip();
                this.channel.write(this.writeBuffer).get();
                this.writeBuffer.clear();
            } catch (InterruptedException | ExecutionException e) {
                // return events to the deque
                for(TransactionEvent.Payload event : this.events) {
                    eventsToSendToVms.offerFirst(event);
                }
                if(!this.channel.isOpen()){
                    this.vmsNode.off();
                    remaining = 0; // force exit loop
                }
            } finally {
                this.writeBuffer.clear();
            }
            remaining = BatchUtils.assembleBatchPayload( remaining, this.events, this.writeBuffer);
        }
    }

    /**
     * If the target VMS is a terminal in the current batch,
     * then the batch commit info must be appended
     */
    private void sendBatchedEvents(BlockingDeque<TransactionEvent.Payload> eventsToSendToVms, BatchCommitInfo.Payload batchCommitInfo){
        eventsToSendToVms.drainTo(this.events);
        int remaining = BatchUtils.assembleBatchPayload( this.events.size(), this.events, this.writeBuffer);
        while(true) {
            try {
                if(remaining > 0) {
                    this.writeBuffer.flip();
                    this.channel.write(this.writeBuffer).get();
                    this.writeBuffer.clear();
                } else {
                    // now must append the batch commit info
                    // do we space in the buffer?
                    if (this.writeBuffer.remaining() < BatchCommitInfo.size) {
                        this.writeBuffer.flip();
                        this.channel.write(this.writeBuffer).get();
                        this.writeBuffer.clear();
                    }
                    BatchCommitInfo.write(this.writeBuffer, batchCommitInfo);

                    // update number of events
                    writeBuffer.mark();
                    int currCount = writeBuffer.getInt(1);
                    currCount++;
                    writeBuffer.putInt(1, currCount);
                    writeBuffer.reset();

                    this.writeBuffer.flip();
                    this.channel.write(this.writeBuffer).get();
                    this.writeBuffer.clear();
                    break;
                }
            } catch (InterruptedException | ExecutionException e) {
                // return events to the deque
                for(TransactionEvent.Payload event : this.events) {
                    eventsToSendToVms.offerFirst(event);
                }
                if(!this.channel.isOpen()){
                    this.vmsNode.off();
                    remaining = 0; // force exit loop
                }
            } finally {
                this.writeBuffer.clear();
            }
            remaining = BatchUtils.assembleBatchPayload( remaining, this.events, this.writeBuffer);
        }
    }

}
