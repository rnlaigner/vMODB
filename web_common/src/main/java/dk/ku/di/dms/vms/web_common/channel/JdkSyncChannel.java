package dk.ku.di.dms.vms.web_common.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class JdkSyncChannel implements IChannel {

    private final SocketChannel channel;
    private final ExecutorService readWriteExecutor;

    private JdkSyncChannel(SocketChannel channel, ExecutorService readWriteExecutor) {
        this.channel = channel;
        this.readWriteExecutor = readWriteExecutor;
    }

    public static JdkSyncChannel build(SocketChannel socket, ExecutorService readWriteExecutor) {
        return new JdkSyncChannel(socket, readWriteExecutor);
    }

    public static JdkSyncChannel build(ExecutorService readWriteExecutor){
        try {
            var channel = SocketChannel.open();
            channel.configureBlocking(true);
            return new JdkSyncChannel(channel, readWriteExecutor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(ByteBuffer src) throws IOException {
        do {
            this.channel.write(src);
        } while (src.hasRemaining());
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        this.readWriteExecutor.submit(()->
        {
            try {
                do {
                    this.channel.write(src);
                } while (src.hasRemaining());
                handler.completed(src.limit(), attachment);
            } catch (Exception e){
                handler.failed(e, attachment);
            }
        });
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        this.readWriteExecutor.submit(()-> {
            try {
                handler.completed(this.channel.read(dst), attachment);
            } catch (IOException e) {
                handler.failed(e, attachment);
            }
        });
    }

    @Override
    public Future<Void> connect(InetSocketAddress inetSocketAddress) {
        try {
            this.channel.connect(inetSocketAddress);
            return CompletableFuture.completedFuture(null);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
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

    @Override
    public NetworkChannel getNetworkChannel() {
        return this.channel;
    }

}
