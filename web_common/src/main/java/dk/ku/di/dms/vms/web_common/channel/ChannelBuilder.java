package dk.ku.di.dms.vms.web_common.channel;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.net.InetSocketAddress;

public final class ChannelBuilder {

    private static final boolean sync;

    static {
        // properties
        var properties= ConfigUtils.loadProperties();
        var networkMode = properties.getProperty("network_mode");
        sync = networkMode != null && networkMode.equalsIgnoreCase("sync");
    }

    public static IServerChannel buildServer(InetSocketAddress address, int networkThreadPoolSize, String networkThreadPoolType, int networkBufferSize){
        if(sync) return JdkServerSyncChannel.build(address, networkThreadPoolSize, networkBufferSize);
        return JdkServerAsyncChannel.build(address, networkThreadPoolSize, networkThreadPoolType, networkBufferSize);
    }

    public static IChannel build(IServerChannel serverAsyncChannel){
        if(serverAsyncChannel instanceof JdkServerAsyncChannel jdkServerAsyncChannel) {
            return JdkAsyncChannel.build(jdkServerAsyncChannel);
        }
        return JdkSyncChannel.build(((JdkServerSyncChannel) serverAsyncChannel).readWriteExecutor());
    }

}
