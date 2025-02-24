package dk.ku.di.dms.vms.modb.storage.iterator.unique;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public final class KeyRecordIterator implements IRecordIterator<IKey> {

    public final IKey[] keys;

    public final ReadOnlyBufferIndex<IKey> index;

    private int position;

    private final int numberOfKeys;

    private IKey currentKey;

    private long currentAddress;

    public KeyRecordIterator(ReadOnlyBufferIndex<IKey> index, IKey[] keys){
        this.index = index;
        this.keys = keys;

        // initialize the first
        this.position = 0;
        this.numberOfKeys = keys.length;
        this.currentKey = keys[position];
        this.currentAddress = index.address(currentKey);
    }

    @Override
    public boolean hasNext() {
        // iterate until find one feasible entry
        while(!index.exists(this.currentAddress) && position < numberOfKeys){
            position++;
            currentKey = keys[position];
            currentAddress = index.address(currentKey);
        }
        return index.exists(currentAddress);
    }

    @Override
    public IKey next() {
        var toReturn = this.currentKey;
        position++;
        currentKey = keys[position];
        currentAddress = index.address(currentKey);
        return toReturn;
    }

    @Override
    public long address(){
        return this.currentAddress;
    }

}
