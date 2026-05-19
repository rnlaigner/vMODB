package dk.ku.di.dms.vms.modb.index.non_unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public final class NonUniqueSortedIndex extends ReadWriteIndex<IKey> {

    // queue to allow concurrent inserts
    private final Map<IKey, NavigableSet<Object[]>> store;

    private final Comparator<Object[]> comparator;

    public NonUniqueSortedIndex(Schema schema, int[] columnsIndex, Comparator<Object[]> comparator) {
        super(schema, columnsIndex);
        this.store = new ConcurrentHashMap<>();
        this.comparator = comparator;
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.NON_UNIQUE;
    }

    @Override
    public int size() {
        return this.store.size();
    }

    @Override
    public boolean exists(IKey key) {
        return this.store.containsKey(key);
    }

    @Override
    public void insert(IKey key, Object[] record) {
        this.store.computeIfAbsent(key, _ -> new ConcurrentSkipListSet<>(this.comparator)).add(record);
    }

    @Override
    public void update(IKey key, Object[] record) {
        this.store.get(key).add(record);
    }

    @Override
    public void upsert(IKey key, Object[] record) {
        this.store.computeIfAbsent(key, _ -> new ConcurrentSkipListSet<>(this.comparator)).add(record);
    }

    @Override
    public void delete(IKey key) {
        this.store.remove(key);
    }

    @Override
    public Object[] lookupByKey(IKey key) {
        return this.store.get(key).toArray();
    }

    @Override
    public IRecordIterator<IKey> iterator(IKey key) {
        final Iterator<Object[]> rr = this.store.get(key).iterator();
        return new IRecordIterator<>() {
            @Override
            public boolean hasNext() {
                return rr.hasNext();
            }

            @Override
            public IKey next() {
                return KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), rr.next());
            }
        };
    }

    @Override
    public void flush() { }

    @Override
    public void reset() {
        this.store.clear();
    }

}
