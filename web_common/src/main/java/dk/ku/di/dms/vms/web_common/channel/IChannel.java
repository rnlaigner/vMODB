package dk.ku.di.dms.vms.web_common.channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface IChannel {

    default void write(ByteBuffer src) throws Exception {
        throw new RuntimeException("Not supported");
    }

    default <A> void write(ByteBuffer src,
                           A attachment,
                           CompletionHandler<Integer,? super A> handler) {
        this.write(src, 0L, TimeUnit.MILLISECONDS, attachment, handler);
    }

    default <A> void write(ByteBuffer src,
                                   long timeout,
                                   TimeUnit unit,
                                   A attachment,
                                   CompletionHandler<Integer,? super A> handler) {
        throw new RuntimeException("Not supported");
    }

    default <A> void write(ByteBuffer[] srcs,
                           int offset,
                           A attachment,
                           CompletionHandler<Long, ? super A> handler) {
        throw new RuntimeException("Not supported");
    }

    boolean isOpen();

    default <A> void read(ByteBuffer dst,
                          A attachment,
                          CompletionHandler<Integer,? super A> handler) {
        throw new RuntimeException("Not supported");
    }

    default Future<Integer> read(ByteBuffer dst) {
        throw new RuntimeException("Not supported");
    }

    default Future<Void> connect(InetSocketAddress inetSocketAddress) {
        throw new RuntimeException("Not supported");
    }

    default void close() {
        throw new RuntimeException("Not supported");
    }

    default SocketAddress getRemoteAddress(){
        throw new RuntimeException("Not supported");
    }

    default NetworkChannel getNetworkChannel() {
        throw new RuntimeException("Not supported");
    }

}
