package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.modb.common.transaction.WriteType;
import dk.ku.di.dms.vms.modb.transaction.internal.SingleWriterMultipleReadersFIFO;

/**
 * The set of operations applied to a given index key
 * Maybe the references to DeleteOp and InsertOp are not necessary.
 * Since we have the last write type and the cached entity,
 * they naturally reference the insert (or last updated)...
 */
public class OperationSetOfKey {

    /**
     * Maybe the entry of this map can be a thread local variable?
     * To save the log(n) on the subsequent operations...
     * Contains the write (insert, delete, update) operations of records.
     * If delete operation, no new records can be added to the key.
     */
    public SingleWriterMultipleReadersFIFO<TransactionId, TransactionWrite> updateHistoryMap;

    /**
     * Nothing impedes the user from deleting and inserting again the same record.
     * A RW/W thread are sequentially spawned, this value always returns correct results.
     * Serves a cache for the last stable write for this key. To avoid traversing all writes performed by TIDs in the history map.
     * It is written during RW/W tasks, so can only be used by on-flight RW/W tasks
     */
    public WriteType lastWriteType;

    /**
     * Entity from the last write operation (insert or update)
     * cached, so we can extract the fields changed from the last version
     * to speed up the checking of constraints
     * Only used by on-flight RW/W tasks
     */
    public Object[] lastVersion;

    // version stored in main memory, from the last snapshot state
    // the logic to keep everything in sync requires changes to write operations
    // better to look for that later
    // another design is making the index itself cache that for us
    // public Object[] snapshotVersion;

    public OperationSetOfKey(){
        this.updateHistoryMap = new SingleWriterMultipleReadersFIFO<>();
    }

}
