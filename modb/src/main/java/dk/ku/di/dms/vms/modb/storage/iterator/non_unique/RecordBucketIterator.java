package dk.ku.di.dms.vms.modb.storage.iterator.non_unique;

import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.CachingKeyIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;

/**
 * Encapsulates an iteration over records that belong
 * to a non-unique index hash bucket
 */
public final class RecordBucketIterator
        extends CachingKeyIterator
        implements IRecordIterator<Long> {

    private long currAddress;

    private int progress;

    private int size;

    // how many records have been iterated so far
    // private int progress;

    public RecordBucketIterator(OrderedRecordBuffer buffer) {
        this.currAddress = buffer.address();
        this.size = buffer.size();
        this.progress = 0;
    }

    /**
     * Follows the linked list
     * @return whether there is a next element
     */
    @Override
    public boolean hasNext() {
        return this.progress < size;
        // return UNSAFE.getBoolean(null, address);
    }

    public IKey key(){
        return this.keyOf(UNSAFE.getInt(this.currAddress + OrderedRecordBuffer.deltaKey));
    }

    /**
     *
     * @return the primary key of the record in this bucket
     */
    public IKey primaryKey(){
        long srcAddress = UNSAFE.getLong(this.currAddress + OrderedRecordBuffer.deltaOffset);
        return this.keyOf(UNSAFE.getInt(srcAddress + Header.SIZE));
    }

    public long address() {
        return this.currAddress;
    }

    @Override
    public Long next() {
        this.progress++;
        // src address
        if(progress < size) {
            this.currAddress = UNSAFE.getLong(this.currAddress + OrderedRecordBuffer.deltaNext);
        }
        return this.currAddress;
    }

}
