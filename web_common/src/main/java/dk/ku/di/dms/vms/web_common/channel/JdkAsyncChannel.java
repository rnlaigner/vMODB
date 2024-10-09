package dk.ku.di.dms.vms.web_common.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper for AsynchronousSocketChannel
 */
public final class JdkAsyncChannel implements IChannel {

    private final AsynchronousSocketChannel channel;

    private JdkAsyncChannel(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    public static JdkAsyncChannel build(AsynchronousSocketChannel asynchronousSocketChannel){
        return new JdkAsyncChannel(asynchronousSocketChannel);
    }

    public static JdkAsyncChannel build(JdkServerAsyncChannel jdkServerAsync) {
        try {
            return new JdkAsyncChannel(AsynchronousSocketChannel.open(jdkServerAsync.getGroup()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Future<Integer> write(ByteBuffer src){
        return this.channel.write(src);
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        this.channel.write(src, timeout, unit, attachment, handler);
    }

    @Override
    public <A> void write(ByteBuffer[] srcs,
                           A attachment,
                           CompletionHandler<Long,? super A> handler) {
        this.channel.write(srcs, 0, srcs.length, 0L, TimeUnit.MILLISECONDS, attachment, handler);
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        this.channel.read(dst, attachment, handler);
    }

    @Override
    public Future<Void> connect(InetSocketAddress inetSocketAddress) {
        return this.channel.connect(inetSocketAddress);
    }

    @Override
    public void close() {
        try { this.channel.close(); } catch (IOException ignored) { }
    }

    @Override
    public SocketAddress getRemoteAddress() {
        try {
            return this.channel.getRemoteAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isOpen() {
        return this.channel.isOpen();
    }

}
