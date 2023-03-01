package dk.ku.di.dms.vms.modb.common.schema.meta;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class NetworkAddress {

    public final String host;
    public final int port;

    public NetworkAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public NetworkAddress(SocketAddress address) {
        this.host = ((InetSocketAddress)address).getHostName();
        this.port = ((InetSocketAddress)address).getPort();
    }

    @Override
    public String toString() {
        return "{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    public InetSocketAddress asInetSocketAddress(){
        return new InetSocketAddress(this.host, this.port);
    }

}
