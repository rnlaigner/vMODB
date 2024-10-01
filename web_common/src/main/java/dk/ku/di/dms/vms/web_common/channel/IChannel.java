package dk.ku.di.dms.vms.web_common.channel;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface IChannel {

    default Future<Integer> write(ByteBuffer src) { return CompletableFuture.completedFuture(0); }

    default <A> void write(ByteBuffer src,
                                   long timeout,
                                   TimeUnit unit,
                                   A attachment,
                                   CompletionHandler<Integer,? super A> handler) {}

    default boolean isOpen() {
        return true;
    }

    default <A> void read(ByteBuffer dst,
                          A attachment,
                          CompletionHandler<Integer,? super A> handler) { }

    default Future<Integer> read(ByteBuffer dst) { return CompletableFuture.completedFuture(0); }

    default Future<Void> connect(InetSocketAddress inetSocketAddress) { return CompletableFuture.completedFuture(null); }

    default void close() { }

}
