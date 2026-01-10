package dk.ku.di.dms.vms.modb.index.interfaces;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;

public abstract class ReadWriteIndex<K> extends AbstractIndex<K> {

    public ReadWriteIndex(Schema schema, int[] columnsIndex) {
        super(schema, columnsIndex);
    }

    public abstract void insert(K key, Object[] record);

    public abstract void update(K key, Object[] record);

    public abstract void delete(K key);

    public abstract Object[] lookupByKey(K key);

    public abstract void upsert(K key, Object[] record);

    public void lock(){
        throw new RuntimeException("Not supported.");
    }

    public void unlock(){
        throw new RuntimeException("Not supported.");
    }

    public void reset() {
        throw new RuntimeException("Not supported.");
    }

    // flush updates
    public void flush(){
        throw new RuntimeException("Not supported.");
    }

}
