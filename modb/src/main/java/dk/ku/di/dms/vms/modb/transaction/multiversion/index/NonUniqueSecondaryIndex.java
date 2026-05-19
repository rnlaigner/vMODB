package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueSortedIndex;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.WriteType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiFunction;

/**
 * Wrapper of a non unique index for multi versioning concurrency control
 */
public final class NonUniqueSecondaryIndex implements IMultiVersionIndex {

    private static final Deque<Map<IKey, Tuple<Object[], WriteType>>> WRITE_SET_BUFFER = new ConcurrentLinkedDeque<>();

    private final Map<Long, Map<IKey, Tuple<Object[], WriteType>>> writeSet;

    // pointer to primary index
    // necessary because of concurrency control
    // secondary index point to records held in primary index
    private final PrimaryIndex primaryIndex;

    // a non-unique hash index
    private final ReadWriteIndex<IKey> underlyingIndex;

    public NonUniqueSecondaryIndex(PrimaryIndex primaryIndex, ReadWriteIndex<IKey> underlyingIndex) {
        this.writeSet = new ConcurrentHashMap<>(1024*100);
        this.primaryIndex = primaryIndex;
        this.underlyingIndex = underlyingIndex;
    }

    public ReadWriteIndex<IKey> underlyingIndex(){
        return this.underlyingIndex;
    }

    /**
     * Called by the primary key index
     * In this method, the secondary key is formed
     * and then cached for later retrieval.
     * A secondary key point to several primary keys in the primary index
     * @param primaryKey may have many secIdxKey associated
     */
    @Override
    public boolean insert(TransactionContext txCtx, IKey primaryKey, Object[] record){
        IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), record );
        Map<IKey, Tuple<Object[], WriteType>> txWriteSet = this.writeSet.computeIfAbsent(txCtx.tid, (ignored) -> Objects.requireNonNullElseGet(WRITE_SET_BUFFER.poll(), HashMap::new));
        txWriteSet.put(primaryKey, new Tuple<>(record, WriteType.INSERT));
        this.underlyingIndex.insert(secKey, record);
        return true;
    }

    @Override
    public void undoTransactionWrites(TransactionContext txCtx){
        Map<IKey, Tuple<Object[], WriteType>> txWriteSet = this.writeSet.remove(txCtx.tid);
        for(Map.Entry<IKey, Tuple<Object[], WriteType>> entry : txWriteSet.entrySet()){
            if(entry.getValue().t2() != WriteType.INSERT) continue;
            IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), entry.getValue().t1() );
            this.underlyingIndex.delete(secKey);
        }
        this.clearAndReturnWriteSetToBuffer(txWriteSet);
    }

    @Override
    public boolean update(TransactionContext txCtx, IKey key, Object[] record) {
        // KEY_WRITES.get().put(key, new Tuple<>(record, WriteType.UPDATE));
        // key already there
        throw new RuntimeException("Not supported");
    }

    @Override
    public boolean remove(TransactionContext txCtx, IKey key) {
        // how to know the sec idx if we don't have the record?
        throw new RuntimeException("Not supported");
    }

    public boolean remove(TransactionContext txCtx, IKey key, Object[] record){
        var txWriteSet = this.writeSet.computeIfAbsent(txCtx.tid, _ ->
                Objects.requireNonNullElseGet(WRITE_SET_BUFFER.poll(), HashMap::new));
        txWriteSet.put(key, new Tuple<>(record, WriteType.DELETE));
        return true;
    }

    @Override
    public Object[] lookupByKey(TransactionContext txCtx, IKey key) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void reset(){
        this.writeSet.clear();
        this.underlyingIndex.reset();
    }

    @Override
    public void installWrites(TransactionContext txCtx) {
        // just remove the delete since the insert is already in the keyMap
        Map<IKey, Tuple<Object[], WriteType>> writeSet = this.writeSet.remove(txCtx.tid);
        if(writeSet == null) {
            throw new RuntimeException("Transaction ID "+txCtx.tid+" could not be found in write set. Perhaps concurrent threads are set to the same TID?");
        }
        for(var entry : writeSet.entrySet()){
            if(entry.getValue().t2() != WriteType.DELETE) continue;
            IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), entry.getValue().t1() );
            this.underlyingIndex.delete(secKey);
        }
        this.clearAndReturnWriteSetToBuffer(writeSet);
    }

    private void clearAndReturnWriteSetToBuffer(Map<IKey, Tuple<Object[], WriteType>> writeSet) {
        writeSet.clear();
        WRITE_SET_BUFFER.addLast(writeSet);
    }

    private static final Iterator<Object[]> EMPTY_ITERATOR = Collections.emptyIterator();

    @Override
    public Iterator<Object[]> iterator(TransactionContext txCtx, IKey[] keys) {
        return new MultiKeySecondaryIndexIterator(txCtx.readOnly ? txCtx.lastTid : txCtx.tid, keys);
    }

    @Override
    public Iterator<Object[]> iterator(TransactionContext txCtx, IKey key) {
        if(!this.underlyingIndex.exists(key)) {
            return EMPTY_ITERATOR;
        }
        return new SecondaryIndexIterator(txCtx.readOnly ? txCtx.lastTid : txCtx.tid, this.underlyingIndex.iterator(key), this.primaryIndex::getRecord);
    }

    private static final class SecondaryIndexIterator implements Iterator<Object[]> {

        private final Iterator<IKey> iterator;
        private Object[] currRecord;
        private final long tid;
        private final BiFunction<Long, IKey, Object[]> getRecordFunc;

        public SecondaryIndexIterator(long tid, Iterator<IKey> iterator, BiFunction<Long, IKey, Object[]> getRecordFunc){
            this.tid = tid;
            this.iterator = iterator;
            this.getRecordFunc = getRecordFunc;
        }

        @Override
        public boolean hasNext() {
            while(this.iterator.hasNext()){
                this.currRecord = this.getRecordFunc.apply(this.tid, this.iterator.next());
                if(this.currRecord != null) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Object[] next() {
            return this.currRecord;
        }

    }

    private final class MultiKeySecondaryIndexIterator implements Iterator<Object[]> {

        private Iterator<IKey> currentIterator;
        private Object[] currRecord;
        private final IKey[] keys;
        private int idx;
        private final long tid;

        public MultiKeySecondaryIndexIterator(long tid, IKey[] keys){
            this.tid = tid;
            this.idx = 0;
            this.keys = keys;
            this.currentIterator = underlyingIndex.iterator(keys[this.idx]);
        }

        @Override
        public boolean hasNext() {
            while(this.currentIterator.hasNext()){
                this.currRecord = primaryIndex.getRecord(this.tid, this.currentIterator.next());
                if(this.currRecord != null) {
                    return true;
                }
            }
            if(this.idx < this.keys.length - 1){
                this.idx++;
                this.currentIterator = underlyingIndex.iterator(keys[this.idx]);
                return this.hasNext();
            }
            return false;
        }

        @Override
        public Object[] next() {
            return this.currRecord;
        }
    }

    @Override
    public int[] indexColumns() {
        return this.underlyingIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.underlyingIndex.containsColumn(columnPos);
    }

    @Override
    public boolean isSorted() {
        return this.underlyingIndex instanceof NonUniqueSortedIndex;
    }

}
