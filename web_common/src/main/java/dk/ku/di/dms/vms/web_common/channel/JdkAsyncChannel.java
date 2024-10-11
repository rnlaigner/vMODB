package dk.ku.di.dms.vms.web_common.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.util.concurrent.ExecutionException;
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
    public void write(ByteBuffer src) throws ExecutionException, InterruptedException {
        do {
            this.channel.write(src).get();
        } while(src.hasRemaining());
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        this.channel.write(src, timeout, unit, attachment, handler);
    }

    @Override
    public <A> void write(ByteBuffer[] srcs, int offset, A attachment, CompletionHandler<Long,? super A> handler) {
        this.channel.write(srcs, offset, srcs.length - offset, 0L, TimeUnit.MILLISECONDS, attachment, handler);
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
    public NetworkChannel getNetworkChannel() {
        return this.channel;
    }

    @Override
    public boolean isOpen() {
        return this.channel.isOpen();
    }

}
