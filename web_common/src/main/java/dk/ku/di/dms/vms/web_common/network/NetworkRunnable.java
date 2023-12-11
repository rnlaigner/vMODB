package dk.ku.di.dms.vms.web_common.network;

import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;

import static java.net.StandardSocketOptions.*;

/**
 * Responsible for batching messages in a buffer before
 * sending over the network
 * It is agnostic to data type, only dealing with bytes
 *
 * I don't know how useful this class is! Still need to figure out whether I need it
 */
public abstract class NetworkRunnable extends SignalingStoppableRunnable {

    // https://stackoverflow.com/questions/39406603/what-is-the-default-value-of-so-sndbuff-and-so-rcvbuff-set-in-the-os-x-system
    // https://stackoverflow.com/questions/4508798/how-to-get-maximum-tcp-receive-send-window-in-mac-os-x
    private static final int DEFAULT_BUFFER_SIZE = 16384;

    // every 30 seconds a new batch is delivered or when a batch fulfills the entire buffer
    private static final int DEFAULT_BATCH_SEND_TIMEOUT = 30000;

    protected final long batchSendTimeout;

    protected final long batchBufferSize;

    public NetworkRunnable(){
        super();
        this.batchSendTimeout = DEFAULT_BATCH_SEND_TIMEOUT;
        this.batchBufferSize = DEFAULT_BUFFER_SIZE;
    }

    public NetworkRunnable(long batchSendTimeout){
        super();
        this.batchSendTimeout = batchSendTimeout;
        this.batchBufferSize = DEFAULT_BUFFER_SIZE;
    }

    public NetworkRunnable(long batchSendTimeout, int batchBufferSize){
        super();
        this.batchSendTimeout = batchSendTimeout;
        this.batchBufferSize = batchBufferSize;
    }

    /**
     * This link may help to decide:
     * https://www.ibm.com/docs/en/oala/1.3.5?topic=SSPFMY_1.3.5/com.ibm.scala.doc/config/iwa_cnf_scldc_kfk_prp_exmpl_c.html
     *
     * Look for socket.send.buffer.bytes
     * https://kafka.apache.org/08/documentation.html
     *
     * https://developpaper.com/analysis-of-kafka-network-layer/
     *
     */
    protected void withDefaultSocketOptions( AsynchronousSocketChannel socketChannel ) throws IOException {

        // true disables the nagle's algorithm. not useful to have coalescence of messages in election
        socketChannel.setOption( TCP_NODELAY, Boolean.TRUE ); // false is the default value

        socketChannel.setOption( SO_KEEPALIVE, Boolean.TRUE );

        socketChannel.setOption( SO_SNDBUF, DEFAULT_BUFFER_SIZE );
        socketChannel.setOption( SO_RCVBUF, DEFAULT_BUFFER_SIZE );

        // for blocking mode only, does not apply to async
        // socketChannel.setOption(SO_LINGER, )

        // does that apply to our system?
        // socketChannel.setOption( SO_REUSEADDR, true );
        // socketChannel.setOption( SO_REUSEPORT, true );
    }

}
