package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.WriteType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wrapper of a non unique index for multi versioning concurrency control
 */
public final class NonUniqueSecondaryIndex implements IMultiVersionIndex {

    private final ThreadLocal<Map<IKey, Tuple<Object[], WriteType>>> WRITE_SET = ThreadLocal.withInitial(HashMap::new);

    // pointer to primary index
    // necessary because of concurrency control
    // secondary index point to records held in primary index
    private final PrimaryIndex primaryIndex;

    // a non-unique hash index
    private final ReadWriteIndex<IKey> underlyingIndex;

    // key: formed by secondary indexed columns
    // value: the corresponding pks
    private final Map<IKey, Set<IKey>> keyMap;

    public NonUniqueSecondaryIndex(PrimaryIndex primaryIndex, ReadWriteIndex<IKey> underlyingIndex) {
        this.primaryIndex = primaryIndex;
        this.underlyingIndex = underlyingIndex;
        this.keyMap = new ConcurrentHashMap<>();
    }

    public ReadWriteIndex<IKey> getUnderlyingIndex(){
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
        Set<IKey> set = this.keyMap.computeIfAbsent(secKey, (ignored)-> new HashSet<>());
        if(!set.contains(primaryKey)) {
            WRITE_SET.get().put(primaryKey, new Tuple<>(record, WriteType.INSERT));
            set.add(primaryKey);
        }
        return true;
    }

    @Override
    public void undoTransactionWrites(TransactionContext txCtx){
        var writes = WRITE_SET.get().entrySet().stream().filter(p->p.getValue().t2()==WriteType.INSERT).toList();
        for(Map.Entry<IKey, Tuple<Object[], WriteType>> entry : writes){
            IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), entry.getValue().t1() );
            Set<IKey> set = this.keyMap.get(secKey);
            set.remove(entry.getKey());
        }
        WRITE_SET.get().clear();
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

    public boolean remove(IKey key, Object[] record){
        IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), record );
        Set<IKey> set = this.keyMap.get(secKey);
        return set.remove(key);
    }

    @Override
    public Object[] lookupByKey(TransactionContext txCtx, IKey key) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public void installWrites(TransactionContext txCtx) {
        // just remove the delete TODO separate INSERT and DELETE into different maps
//        Map<IKey, Tuple<Object[], WriteType>> writeSet = WRITE_SET.get();
//        for(Map.Entry<IKey, Tuple<Object[], WriteType>> entry : writeSet.entrySet()){
//            if(entry.getValue().t2() != WriteType.DELETE) continue;
//            IKey secKey = KeyUtils.buildRecordKey( this.underlyingIndex.columns(), entry.getValue().t1() );
//            Set<IKey> set = this.keyMap.get(secKey);
//            set.remove(entry.getKey());
//        }
//        writeSet.clear();
    }

    @Override
    public Iterator<Object[]> iterator(TransactionContext txCtx, IKey[] keys) {
        return new MultiKeyMultiVersionIterator(this.primaryIndex, txCtx, keys, this.keyMap);
    }

    private static class MultiKeyMultiVersionIterator implements Iterator<Object[]> {

        private final Map<IKey, Set<IKey>> keyMap;
        private final PrimaryIndex primaryIndex;
        private final TransactionContext txCtx;
        private Iterator<IKey> currentIterator;

        private final IKey[] keys;

        private int idx;

        public MultiKeyMultiVersionIterator(PrimaryIndex primaryIndex, TransactionContext txCtx, IKey[] keys, Map<IKey, Set<IKey>> keyMap){
            this.primaryIndex = primaryIndex;
            this.txCtx = txCtx;
            this.idx = 0;
            this.keys = keys;
            this.currentIterator = keyMap.computeIfAbsent(keys[this.idx], (ignored) -> new HashSet<>()).iterator();
            this.keyMap = keyMap;
        }

        @Override
        public boolean hasNext() {
            return this.currentIterator.hasNext() || this.idx < this.keys.length - 1;
        }

        @Override
        public Object[] next() {
            if(!this.currentIterator.hasNext()){
                this.idx++;
                this.currentIterator = this.keyMap.computeIfAbsent(this.keys[this.idx], (ignored) -> new HashSet<>()).iterator();
            }
            IKey key = this.currentIterator.next();
            return this.primaryIndex.getRecord(txCtx, key);
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

}
