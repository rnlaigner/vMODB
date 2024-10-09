package dk.ku.di.dms.vms.web_common.meta;

import dk.ku.di.dms.vms.web_common.channel.IChannel;

public class ConnectionMetadata {

    /**
     * generic, serves for both servers and VMSs, although the key
     * may change across different classes (e.g., use of vms name or <host+port>)
     */
    public final int key;

    public final NodeType nodeType;

    // unfortunately cannot set to final because of the election protocol in coordinator
    // see processServerPresentationMessage
    public final IChannel channel;

    public enum NodeType {
        SERVER,
        VMS,
        HTTP_CLIENT
    }

    public ConnectionMetadata(int key, NodeType nodeType, IChannel channel) {
        this.key = key;
        this.nodeType = nodeType;
        this.channel = channel;
    }

}
