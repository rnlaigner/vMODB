package dk.ku.di.dms.vms.web_common.meta;

import java.util.Map;

/**
 * The identification of a connecting DBMS daemon
 * ideally one vms per dbms proxy, but in the future may be many.
 * but who knows... the sdk are already sending a map...
 *
 * I'm relying on the fact that VMSs do not switch name, host, and port
 */
public class VmsIdentifier extends NetworkObject {

    // identifier is the vms name

    public long lastTid;

    // batch offset, also monotonically increasing.
    // to avoid vms to process transactions from the
    // next batch while the current has not finished yet
    public long lastBatch;

    // data model
    // public List<VmsDataSchema> dataSchema;
    public VmsDataSchema dataSchema;

    // event data model
    public Map<String, VmsEventSchema> eventSchema;

    public VmsIdentifier(String host, int port, long lastTid, long lastBatch, VmsDataSchema dataSchema, Map<String, VmsEventSchema> eventSchema) {
        super(host,port);
        this.lastTid = lastTid;
        this.lastBatch = lastBatch;
        this.dataSchema = dataSchema;
        this.eventSchema = eventSchema;
    }

    public String getIdentifier(){
        return dataSchema.virtualMicroservice;
    }

    public long getLastTid(){
        return lastTid;
    }

}
