package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * This class is just a placeholder for obtaining schema information in main memory execution
 */
public final class UniqueHashMapIndex extends ReadWriteIndex<IKey> {

    private final Map<IKey, Object[]> store;

    public UniqueHashMapIndex(Schema schema, int[] columns) {
        super(schema, columns);
        this.store = Collections.emptyMap();
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.UNIQUE;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean exists(IKey key) {
        return false;
    }

    @Override
    public void insert(IKey key, Object[] record) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void update(IKey key, Object[] record) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void upsert(IKey key, Object[] record) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void delete(IKey key) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public Object[] lookupByKey(IKey key) {
        return null;
    }

    @Override
    public Object[] record(IKey key) {
        // not ideal, but just to avoid unexpected exceptions
        return null;
    }

    @Override
    public Iterator<IKey> iterator() {
        return this.store.keySet().iterator();
    }

}
