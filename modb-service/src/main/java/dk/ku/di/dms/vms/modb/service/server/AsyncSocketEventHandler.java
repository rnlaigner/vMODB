package dk.ku.di.dms.vms.modb.service.server;

import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * TODO in the future, if there is no new events, this thread can be put to sleep
 *          and be woken up when new events arrive
 *          Sources:
 *          https://stackoverflow.com/questions/18368130/how-to-parse-and-validate-a-websocket-frame-in-java
 *          https://github.com/FirebaseExtended/TubeSock/blob/master/src/main/java/com/firebase/tubesock/WebSocketReceiver.java
 *          https://blog.sessionstack.com/how-javascript-works-deep-dive-into-websockets-and-http-2-with-sse-how-to-pick-the-right-path-584e6b8e3bf7
 */
public class AsyncSocketEventHandler extends StoppableRunnable {

    private final Logger logger = LoggerFactory.getLogger(AsyncSocketEventHandler.class.getName());

    /** EVENT QUEUES **/
    private final Queue<Object> inputQueue;

    private final Queue<Object> outputQueue;

    /** SOCKET CHANNEL */
    private final AsynchronousSocketChannel socketChannel;

    private final ByteBuffer readBuffer;

    private final ByteBuffer writeBuffer;

    private Future<Integer> futureInput;

    private Future<Integer> futureOutput;

    /** SERIALIZATION **/
    private final IVmsSerdesProxy serdes;

    public AsyncSocketEventHandler(AsynchronousSocketChannel clientSocket,
                                   Queue<Object> inputQueue,
                                   Queue<Object> outputQueue, IVmsSerdesProxy serdes) {

        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;

        this.socketChannel = clientSocket;

        this.serdes = serdes;

        // TODO revisit this size later
        // The idea is to have an initial large enough buffer to accommodate
        // the majority of incoming request payload sizes
        this.readBuffer = ByteBuffer.allocateDirect( 1024 );
        this.writeBuffer = ByteBuffer.allocateDirect( 1024 );
    }

    @Override
    public void run() {

        logger.info("Running new client handler...");


        initialize();

        // while not closed connection, continue
        while(isRunning() && socketChannel.isOpen()){

            read();


            // TODO communicate message to coordinator

        }

        shutdown();

    }

    /**
     * Initialize buffer and channels
     */
    private void initialize(){
        //  read buffer
        readBuffer.clear();
        futureInput = socketChannel.read(readBuffer);

        // write buffer

    }

    private void read(){

        if( futureInput.isDone() ){

            try {
                int length = futureInput.get();

                if (length == -1) {
                    logger.warn("ERR: something went wrong on reading the buffer");
                    readBuffer.flip();
                    return;
                }

                // FIXME i dont need to deserialize .... simply forward to coordinator???
                //  i may need to deserialize to store what have been processed so far...
                //  but i can store the byte directly and deserialize on occasion...
                //  but for now I can just forward...
                // Object event = serdes.deserialize(readBuffer.array());

            } catch (ExecutionException | InterruptedException e) {
                logger.warn(e.getMessage());
            } finally {
                readBuffer.clear();
                futureInput = socketChannel.read(readBuffer);
            }

        }

    }

    private void shutdown() {

        // first make sure buffer is cleaned
        readBuffer.clear();
        readBuffer.limit(0);
        // buffer = ByteBuffer.allocateDirect(0);

        try{
            if(socketChannel.isOpen()) socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
