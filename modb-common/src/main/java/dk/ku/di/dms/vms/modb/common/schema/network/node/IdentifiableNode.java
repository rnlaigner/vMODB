package dk.ku.di.dms.vms.modb.common.schema.network.node;

import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;

public class IdentifiableNode extends NetworkAddress {

    // identifier is the vms name
    public String identifier;

    // for JSON parsing
    public IdentifiableNode(){ }

    public IdentifiableNode(String identifier, String host, int port) {
        super(host, port);
        this.identifier = identifier;
    }

    public static IdentifiableNode build(String identifier, NetworkAddress address){
        return new IdentifiableNode(identifier, address.host, address.port);
    }

    @Override
    public String toString() {
        return "{" +
                "identifier='"+ identifier + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IdentifiableNode that)) return false;
        return this.identifier.contentEquals(that.identifier) && super.equals(that);
    }

}
