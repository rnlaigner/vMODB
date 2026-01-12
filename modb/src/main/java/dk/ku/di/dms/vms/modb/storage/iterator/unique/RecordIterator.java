package dk.ku.di.dms.vms.modb.storage.iterator.unique;

import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * Iterator tamed for verification of records in sequential positions
 * Cannot be used for iterating over ordered record buffer
 * Does not consider a record may have multiple items.
 * In other words, it reads from the last consistent snapshot
 */
public final class RecordIterator implements IRecordIterator<IKey> {

    private long nextAddress;
    private final int recordSize;
    private final long capacity;
    private int progress; // how many records have been iterated so far

    public RecordIterator(long address, int recordSize, long capacity){
        this.nextAddress = address;
        this.recordSize = recordSize;
        this.capacity = capacity;
        this.progress = 0;
    }

    @Override
    public boolean hasNext() {
        // checks for an active bit
        while(this.progress < this.capacity && UNSAFE.getByte(null, this.nextAddress) != Header.ACTIVE_BYTE){
            this.progress++;
            this.nextAddress += recordSize;
        }
        return this.progress < this.capacity;
    }

    @Override
    public IKey next() {
        SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.nextAddress + 1));
        this.progress++;
        this.nextAddress += this.recordSize;
        return res;
    }

    @Override
    public long address(){
        return this.nextAddress;
    }

}