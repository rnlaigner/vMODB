package dk.ku.di.dms.vms.modb.common.transaction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data class shared among different components
 * to store information related to concurrency control
 * and versioning of data items
 */
public class TransactionContext {

    public TransactionId tid;

    public TransactionId lastTid;

    public boolean readOnly;

    // mapping of table to writes made by this transaction
    // this module has no access to modb so deletes have their PK attached as object[]
    public final Map<String, List<TransactionWrite>> writes;

    public TransactionContext(TransactionId tid, TransactionId lastTid, boolean readOnly) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.readOnly = readOnly;
        this.writes = readOnly ? Map.of() : new HashMap<>();
    }

}
