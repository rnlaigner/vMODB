package dk.ku.di.dms.vms.modb.common.schema.node;

/**
 * Basic data of a server running for leader
 */
public final class ServerIdentifier extends NetworkNode {

    // maybe volatile?
    public long lastOffset;

    // maybe also last batch?

    public ServerIdentifier(String host, int port) {
        super(host, port);
        this.lastOffset = 0L;
    }

    public ServerIdentifier(String host, int port, long lastOffset) {
        super(host,port);
        this.lastOffset = lastOffset;
    }

}
