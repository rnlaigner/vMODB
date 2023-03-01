package dk.ku.di.dms.vms.modb.common.schema.node;

import dk.ku.di.dms.vms.modb.common.schema.meta.VmsReplicatedTableSchema;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsTableSchema;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsEventSchema;

import java.util.Map;

/**
 * The identification of a connecting DBMS daemon
 * ideally one vms per dbms proxy, but in the future may be many.
 * but who knows... the sdk are already sending a map...
 * I'm relying on the fact that VMSs do not switch name, host, and port
 * This class is supposed to be used by the coordinator to find about
 * producers and consumers and then form and send the consumer set
 * to each virtual microservice
 */
public class VmsNode extends NetworkNode {

    // identifier is the vms name
    public final String name;

    /**
     * The batch offset, monotonically increasing.
     * to avoid vms to process transactions from the
     * next batch while the current has not finished yet
     */
    public long batch;

    // last tid of current batch. may not participate in all TIDs of the batch
    public long lastTidOfBatch;

    /**
     * A vms may not participate in all possible batches
     * In other words, may have gaps
     * This value informs the batch that precedes the
     * current batch
     */
    public long previousBatch;

    // data model
    public final Map<String, VmsTableSchema> tableSchema;

    public final Map<String, VmsReplicatedTableSchema> replicatedTableSchema;

    // event data model
    public final Map<String, VmsEventSchema> inputEventSchema;

    public final Map<String, VmsEventSchema> outputEventSchema;

    public VmsNode(String host, int port, String name,
                   long batch, long lastTidOfBatch, long previousBatch,
                   Map<String, VmsTableSchema> tableSchema,
                   Map<String, VmsReplicatedTableSchema> replicatedTableSchema,
                   Map<String, VmsEventSchema> inputEventSchema,
                   Map<String, VmsEventSchema> outputEventSchema) {
        super(host, port);
        this.name = name;
        this.batch = batch;
        this.lastTidOfBatch = lastTidOfBatch;
        this.previousBatch = previousBatch;
        this.tableSchema = tableSchema;
        this.replicatedTableSchema = replicatedTableSchema;
        this.inputEventSchema = inputEventSchema;
        this.outputEventSchema = outputEventSchema;
    }

    @Override
    public String toString() {
        return "{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", identifier='" + name + '\'' +
                ", batch=" + batch +
                ", lastTidOfBatch=" + lastTidOfBatch +
                ", previousBatch=" + previousBatch +
                '}';
    }

}
