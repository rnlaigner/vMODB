package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A container of consumer VMS workers to facilitate
 * scalable pushing of transaction events
 */
public final class MultiVmsContainer implements IVmsContainer {

    private final IdentifiableNode node;
    private final ConsumerVmsWorker[] consumerVmsWorkers;

    // init a container with the initial consumer VMS
    MultiVmsContainer(ConsumerVmsWorker initialConsumerVms, IdentifiableNode node, int numVmsWorkers) {
        this.consumerVmsWorkers = new ConsumerVmsWorker[numVmsWorkers];
        this.consumerVmsWorkers[0] = initialConsumerVms;
        this.node = node;
    }

    public synchronized void addConsumerVms(int idx, ConsumerVmsWorker vmsWorker) {
        this.consumerVmsWorkers[idx] = vmsWorker;
    }

    @Override
    public boolean queue(TransactionEvent.PayloadRaw payload){
        int pos = ThreadLocalRandom.current().nextInt(0, this.consumerVmsWorkers.length);
        return this.consumerVmsWorkers[pos].queue(payload);
    }

    @Override
    public String identifier(){
        return this.node.identifier;
    }
}


