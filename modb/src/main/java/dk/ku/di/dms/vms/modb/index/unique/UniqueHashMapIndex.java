package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * This class is just a placeholder for obtaining schema information in main memory execution
 */
public final class UniqueHashMapIndex extends ReadWriteIndex<IKey> {

    public UniqueHashMapIndex(Schema schema, int[] columns) {
        super(schema, columns);
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
    public void insert(IKey key, Object[] record) { }

    @Override
    public void update(IKey key, Object[] record) { }

    @Override
    public void upsert(IKey key, Object[] record) { }

    @Override
    public void delete(IKey key) { }

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
    public void reset() { }

    private static final IRecordIterator<IKey> IT = new IRecordIterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public IKey next() {
            return null;
        }
    };

    @Override
    public IRecordIterator<IKey> iterator() {
        return IT;
    }

}
