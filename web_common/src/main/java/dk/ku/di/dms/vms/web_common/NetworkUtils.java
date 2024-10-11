package dk.ku.di.dms.vms.web_common;

import jdk.net.ExtendedSocketOptions;

import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.ServerSocketChannel;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.net.StandardSocketOptions.*;

public final class NetworkUtils {

    private static final System.Logger LOGGER = System.getLogger(NetworkUtils.class.getName());

    public static void configure(NetworkChannel channel, int osBufferSize) {
        try {
            channel.setOption(SO_REUSEPORT, false);
            if(channel instanceof AsynchronousServerSocketChannel || channel instanceof ServerSocketChannel) return;
            channel.setOption(TCP_NODELAY, true);
            channel.setOption(SO_KEEPALIVE, true);
            if (channel.supportedOptions().contains(ExtendedSocketOptions.TCP_QUICKACK)) {
                channel.setOption(ExtendedSocketOptions.TCP_QUICKACK, true);
            }
            if (osBufferSize > 0) {
                LOGGER.log(DEBUG, "Configuring channel " + channel + " with " + osBufferSize + " as size of SO_SNDBUF and SO_RCVBUF");
                channel.setOption(SO_SNDBUF, osBufferSize);
                channel.setOption(SO_RCVBUF, osBufferSize);
            }
        } catch (Exception e){
            LOGGER.log(ERROR, e);
        }
    }

}
