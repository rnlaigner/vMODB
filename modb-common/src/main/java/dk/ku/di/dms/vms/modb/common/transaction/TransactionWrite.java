package dk.ku.di.dms.vms.modb.common.transaction;

/**
 * A carrier to facilitate data transfer across modules
 * at the same time keeping metadata (i.e., type) about
 * the update to be performed in a state
 */
public class TransactionWrite {

    public WriteType type; // if delete operation, record is null
    public Object[] record; // the whole record

    public TransactionWrite(WriteType type, Object[] record) {
        this.type = type;
        this.record = record;
    }
}
