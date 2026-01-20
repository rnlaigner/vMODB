package dk.ku.di.dms.vms.modb.index.interfaces;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public interface ReadOnlyIndex<K> extends IIndex<K> {

    default Object[] record(IKey key){
        throw new RuntimeException("No support for checking condition on key in this index.");
    }

    default IRecordIterator<IKey> iterator(){
        throw new RuntimeException("No support for checking condition on key in this index.");
    }

    /**
     * For hash probing on a set of keys
     * Can also be implemented by non-unique hash indexes,
     * but may be expensive
     */
    default IRecordIterator<IKey> iterator(IKey[] keys) {
        throw new RuntimeException("No support for set of keys iteration in this index.");
    }

    default IRecordIterator<IKey> iterator(IKey key) {
        throw new RuntimeException("No support for key iteration in this index.");
    }

    default boolean checkCondition(K key, FilterContext filterContext){
        throw new RuntimeException("No support for checking condition on key in this index.");
    }

}
