package dk.ku.di.dms.vms.web_common.channel;

import dk.ku.di.dms.vms.web_common.NetworkUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

public final class JdkServerSyncChannel implements IServerChannel {

    private final ExecutorService acceptorThreadPool = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon(false).inheritInheritableThreadLocals(false).factory()
    );

    private final ServerSocketChannel channel;

    private final ExecutorService readWriteExecutor;

    public static JdkServerSyncChannel build(InetSocketAddress address, int networkThreadPoolSize, int networkBufferSize){
        try {
            ServerSocketChannel serverSocket = ServerSocketChannel.open();
            serverSocket.configureBlocking(true);
            NetworkUtils.configure(serverSocket, 0);
            serverSocket.bind(address, networkThreadPoolSize);
            return new JdkServerSyncChannel(serverSocket,
                    Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                                    .inheritInheritableThreadLocals(false)
                                    .factory())
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JdkServerSyncChannel(ServerSocketChannel channel, ExecutorService readWriteExecutor){
        this.channel = channel;
        this.readWriteExecutor = readWriteExecutor;
    }

    private static final ReentrantLock NO_LOCK = new NoLock();

    @Override
    public void accept(CompletionHandler<IChannel, Void> handler) {
        this.acceptorThreadPool.submit(() -> {
            try {
                final SocketChannel socketChannel = this.channel.accept();
                //socket.setReuseAddress(true); why?
                socketChannel.configureBlocking(true);

                Class<?> socketChannelClass = socketChannel.getClass();
                Field readLockField = socketChannelClass.getDeclaredField("readLock");
                readLockField.setAccessible(true);
                readLockField.set(socketChannel, NO_LOCK);

                Field writeLockField = socketChannelClass.getDeclaredField("writeLock");
                writeLockField.setAccessible(true);
                writeLockField.set(socketChannel, NO_LOCK);

                this.readWriteExecutor.execute(()-> handler.completed(
                        JdkSyncChannel.build(socketChannel, this.readWriteExecutor), null)
                );
            } catch (Exception e) {
                handler.failed(e, null);
            }
        });
    }

    public void close() {
        try { this.channel.close(); } catch (IOException ignored) { }
    }

    public ExecutorService readWriteExecutor() {
        return this.readWriteExecutor;
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
