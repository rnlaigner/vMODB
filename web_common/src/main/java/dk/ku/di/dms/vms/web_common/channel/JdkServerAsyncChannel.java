package dk.ku.di.dms.vms.web_common.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

public final class JdkServerAsyncChannel implements IServerChannel {

    private final AsynchronousServerSocketChannel channel;

    // group for channels
    private final AsynchronousChannelGroup group;

    public JdkServerAsyncChannel(AsynchronousServerSocketChannel channel, AsynchronousChannelGroup group) {
        this.channel = channel;
        this.group = group;
    }

    public static IServerChannel build(InetSocketAddress address, int networkThreadPoolSize) {
        return build(address, networkThreadPoolSize, "default");
    }

    public static IServerChannel build(InetSocketAddress address, int networkThreadPoolSize, String networkThreadPoolType) {
        AsynchronousChannelGroup group;
        AsynchronousServerSocketChannel serverSocket;
        try {
            // network and executor
            switch (networkThreadPoolType){
                case "default" -> group = AsynchronousChannelGroup.withFixedThreadPool(
                        networkThreadPoolSize > 0 ? networkThreadPoolSize : Runtime.getRuntime().availableProcessors(),
                        Thread.ofPlatform().name("vms-network-thread").factory()
                );
                case "vthread" -> group = AsynchronousChannelGroup.withFixedThreadPool(
                        networkThreadPoolSize > 0 ? networkThreadPoolSize : Runtime.getRuntime().availableProcessors(),
                        Thread.ofVirtual().name("vms-network-vthread").factory()
                );
                case null, default -> group = null;
            }
            serverSocket = AsynchronousServerSocketChannel.open(group);
            serverSocket.bind(address);
            return new JdkServerAsyncChannel(serverSocket, group);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accept(CompletionHandler<IChannel, Void> handler) {
        this.channel.accept(null, new CompletionHandler<>() {
            @Override
            public void completed(AsynchronousSocketChannel asynchronousSocketChannel, Object attachment) {
                handler.completed(JdkAsyncChannel.build(asynchronousSocketChannel), null);
            }
            @Override
            public void failed(Throwable exc, Object attachment) {
                handler.failed(exc, null);
            }
        });
    }

    public AsynchronousChannelGroup getGroup() {
        return this.group;
    }

    @Override
    public boolean isOpen() {
        return this.channel.isOpen();
    }

    public void close() {
        try { this.channel.close(); } catch (IOException ignored) { }
    }

}
