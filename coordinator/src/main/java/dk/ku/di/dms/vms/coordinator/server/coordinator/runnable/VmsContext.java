package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;

/**
 * The context of a managed virtual microservice
 * Contains the node information and the associated worker
 */
public record VmsContext(VmsNode node, IVmsWorker worker) {

    /**
     * Methods to use with Java stream library
     */
    public long getLastTidOfBatch(){
        return this.node.lastTidOfBatch;
    }

    public long getPreviousBatch(){
        return this.node.previousBatch;
    }

    public String getVmsName(){
        return this.node.name;
    }

}

