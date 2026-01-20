package dk.ku.di.dms.vms.modb.storage.iterator;

import java.util.Iterator;

public interface IRecordIterator<K> extends Iterator<K> {

    // current address
    default long address() {
        throw new IllegalStateException("This iterator implementation does not provide access to memory address.");
    }

}
