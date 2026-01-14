package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.internal.Entry;
import dk.ku.di.dms.vms.modb.transaction.internal.OperationSetOfKey;
import dk.ku.di.dms.vms.modb.transaction.multiversion.IPrimaryKeyGenerator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.TransactionWrite;
import dk.ku.di.dms.vms.modb.transaction.multiversion.WriteType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static dk.ku.di.dms.vms.modb.common.constraint.ConstraintConstants.*;
import static java.lang.System.Logger.Level.WARNING;

/**
 * A consistent view over an index.
 * A wrapper that envelops the original index
 * <a href="https://refactoring.guru/design-patterns">...</a>
 * To implement a consistent view correctly,
 * we need correct access to versioned data items,
 * correct copying of data items to the result buffers.
 * So it is a mix of behavioral and state:
 * The operators need an iterator (to provide only the allowed data items' visibility)
 * The filter must take into account the correct data item version
 * The append operation must take into account the correct data item version
 * Q: Why implement a ReadOnlyIndex?
 * A: Because it is not supposed to modify the data in main-memory,
 * but rather keep versions in a cache on heap memory.
 * Thus, the API for writes are different. The ReadWriteIndex is only used for bulk writes,
 * when no transactional guarantees are necessary.
 */
public final class PrimaryIndex implements IMultiVersionIndex {

    private static final System.Logger LOGGER = System.getLogger(PrimaryIndex.class.getName());

    private static final Deque<Set<IKey>> WRITE_SET_BUFFER = new ConcurrentLinkedDeque<>();

    private final ReadWriteIndex<IKey> rawIndex;

    private final Map<IKey, OperationSetOfKey> updatesPerKeyMap;

    // for PK generation. for now, all strategies use sequence
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<IPrimaryKeyGenerator<?>> primaryKeyGenerator;

    // if without checkpointing, keeps track of entries for garbage collection
    private final Set<IKey> keysToFlush;

    // write set of transactions
    private final Map<Long, Set<IKey>> writeSetMap;

    public static PrimaryIndex build(ReadWriteIndex<IKey> primaryKeyIndex) {
        return new PrimaryIndex(primaryKeyIndex, null);
    }

    public static PrimaryIndex build(ReadWriteIndex<IKey> primaryKeyIndex, IPrimaryKeyGenerator<?> primaryKeyGenerator){
        return new PrimaryIndex(primaryKeyIndex, primaryKeyGenerator);
    }

    private PrimaryIndex(ReadWriteIndex<IKey> rawIndex, IPrimaryKeyGenerator<?> primaryKeyGenerator) {
        this.rawIndex = rawIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>(1024*100);
        this.primaryKeyGenerator = Optional.ofNullable(primaryKeyGenerator);
        this.writeSetMap = new ConcurrentHashMap<>(2048*10);
        this.keysToFlush = ConcurrentHashMap.newKeySet();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrimaryIndex && this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return this.rawIndex.hashCode();
    }

    /**
     * Same logic as {@link #lookupByKey}
     * @param key record key
     * @return whether the record exists for this transaction, respecting atomic visibility
     */
    public boolean exists(TransactionContext txCtx, IKey key) {
        OperationSetOfKey opSet = this.updatesPerKeyMap.get(key);
        if(opSet != null){
            // why checking first if I am a WRITE. because by checking if I am right, I don't need to pay O(log n)
            // 1 write thread at a time. if that is a writer thread, does not matter my lastTid. I can just check the last write for this entry
            if(!txCtx.readOnly){
                return opSet.lastWriteType != WriteType.DELETE;
            }
            Entry<Long, TransactionWrite> floorEntry = opSet.floorEntry(txCtx.lastTid);
            if(floorEntry == null) return this.rawIndex.exists(key);
            return floorEntry.val().type != WriteType.DELETE;
        }
        return this.rawIndex.exists(key);
    }

    public boolean meetPartialIndex(Object[] values, int column, Object expectedValue){
        Schema schema = this.rawIndex.schema();
        switch (schema.columnDataType(column)) {
            case CHAR -> {
                return ((String) values[column]).contentEquals((String) expectedValue);
            }
            case ENUM -> {
                return values[column].toString().contentEquals((String) expectedValue);
            }
        }
        return false;
    }

    // every condition checked is proceeded by a completion handler
    // can maintain the boolean return but all operators (for read queries)
    // necessarily require appending to a memory result space

    /**
     * Methods for insert/update operations.
     */
    private boolean nonPkConstraintViolation(Object[] values) {
        if(this.rawIndex.schema().constraints().isEmpty())
            return false;
        Schema schema = this.rawIndex.schema();
        boolean constraintHolds = true;
        for(Map.Entry<Integer, ConstraintReference> c : schema.constraints().entrySet()) {
            switch (c.getValue().constraint.type){
                case NUMBER -> {
                    switch (schema.columnDataType(c.getKey())) {
                        case INT -> constraintHolds = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , 0, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> constraintHolds = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , 0L, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> constraintHolds = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , 0f, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> constraintHolds = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , 0d, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }
                case NUMBER_WITH_VALUE -> {
                    Object valToCompare = c.getValue().asValueConstraint().value;
                    switch (schema.columnDataType(c.getKey())) {
                        case INT -> constraintHolds = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , (int)valToCompare, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> constraintHolds = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , (long)valToCompare, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> constraintHolds = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , (float)valToCompare, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> constraintHolds = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , (double)valToCompare, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }
                case NULLABLE ->
                        constraintHolds = NullableTypeConstraintHelper.eval(values[c.getKey()], c.getValue().constraint);
                case CHARACTER ->
                        constraintHolds = CharOrStringTypeConstraintHelper.eval((String) values[c.getKey()], c.getValue().constraint );
            }
            if(!constraintHolds) return true;
        }
        return false;
    }

    private static class NullableTypeConstraintHelper {
        public static <T> boolean eval(T v1, ConstraintEnum constraint){
            if (constraint == ConstraintEnum.NOT_NULL) {
                return v1 != null;
            }
            return v1 == null;
        }
    }

    private static class CharOrStringTypeConstraintHelper {
        public static boolean eval(String value, ConstraintEnum constraint){
            if (constraint == ConstraintEnum.NOT_BLANK) {
                for(int i = 0; i < value.length(); i++){
                    if(value.charAt(i) != ' ') return true;
                }
            } else {
                // TODO support pattern, non blank
                throw new IllegalStateException("Constraint cannot be applied to characters.");
            }
            return false;
        }
    }

    /**
     * Having the second parameter is necessary to avoid casting.
     */
    private static class NumberTypeConstraintHelper {
        public static <T> boolean eval(T v1, T v2, Comparator<T> comparator, ConstraintEnum constraint){
            switch (constraint) {
                case POSITIVE_OR_ZERO, MIN -> {
                    return comparator.compare(v1, v2) >= 0;
                }
                case POSITIVE -> {
                    return comparator.compare(v1, v2) > 0;
                }
                case NEGATIVE -> {
                    return comparator.compare(v1, v2) < 0;
                }
                case NEGATIVE_OR_ZERO, MAX -> {
                    return comparator.compare(v1, v2) <= 0;
                }
                default -> throw new IllegalStateException("Cannot compare the constraint "+constraint+" for number type.");
            }
        }
    }

    @Override
    public Object[] lookupByKey(TransactionContext txCtx, IKey key){
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        if (operationSet == null) {
            return this.rawIndex.lookupByKey(key);
        }
        if(txCtx.readOnly) {
            Entry<Long, TransactionWrite> entry = operationSet.floorEntry(txCtx.lastTid);
            return entry != null ? (entry.val().type != WriteType.DELETE ? entry.val().record : null) : null;
        }
        if(operationSet.peak().key > txCtx.tid){
            throw new RuntimeException("An attempt to read an entry with higher TID in a read-write transaction. Perhaps a misconfigured function annotation?");
        }
        if(operationSet.lastWriteType == WriteType.DELETE) return null;
        Entry<Long, TransactionWrite> entry = operationSet.floorEntry(txCtx.tid);
        if(entry != null) {
            return entry.val().record;
        }
        return null;
    }

    /**
     * possible optimization: if cached value is not null,
     * then extract the updated columns to make constraint violation check faster
     * @return whether it is allowed to proceed with the operation
     */
    public boolean insert(TransactionContext txCtx, IKey key, Object[] record) {
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get(key);
        // if not delete, violation (it means some tid has written to this PK before)
        if (operationSet != null){
            if(operationSet.lastWriteType != WriteType.DELETE) {
                LOGGER.log(WARNING, "Primary key violation found in multi version map: " + key);
                return false;
            }
        }
        /*
        unknown error related to this block. only occurs in processStockConfirmed and after an initial run
        either (a) the checkpoint is concurrently putting this entry or
        (b) the reset is not appropriately cleaning the buffer or
        (c) some hash buffer operation is buggy (although the record returned below indeed has the same key)
         */
        else if(this.rawIndex.exists(key)){
            Object[] existingRecord = this.rawIndex.lookupByKey(key);
            LOGGER.log(WARNING, "Primary key violation found in underlying primary key index: "+key+" Existing record:\n"+Arrays.stream(existingRecord).toList());
            return false;
        }
        if(this.nonPkConstraintViolation(record)) {
            LOGGER.log(WARNING, "Non PK violation found in underlying primary key index: "+key);
            return false;
        }
        this.doInsert(txCtx, key, record, operationSet);
        return true;
    }

    private void doInsert(TransactionContext txCtx, IKey key, Object[] values, OperationSetOfKey operationSet) {
        TransactionWrite entry = TransactionWrite.upsert(WriteType.INSERT, values);
        if(operationSet == null){
            operationSet = new OperationSetOfKey(WriteType.INSERT);
            this.updatesPerKeyMap.put(key, operationSet);
        } else {
            operationSet.lastWriteType = WriteType.INSERT;
        }
        operationSet.put(txCtx.tid, entry);
        this.appendWrite(txCtx, key);
    }

    public boolean upsert(TransactionContext txCtx, IKey key, Object[] values) {
        if(this.nonPkConstraintViolation(values)) {
            return false;
        }
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get(key);
        boolean exists;
        if (operationSet != null){
            exists = operationSet.lastWriteType != WriteType.DELETE;
            if(operationSet.peak().key > txCtx.tid) {
                throw new RuntimeException("An attempt to update with a lower TID than the original record. Perhaps a misconfigured function annotation?");
            }
        } else {
            exists = this.rawIndex.exists(key);
        }
        if(exists) {
            this.doUpdate(txCtx, key, values, operationSet);
        } else {
            this.doInsert(txCtx, key, values, operationSet);
        }
        return true;
    }

    @Override
    public boolean update(TransactionContext txCtx, IKey key, Object[] values) {
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        boolean pkConstraintViolation;
        if (operationSet != null){
            pkConstraintViolation = operationSet.lastWriteType == WriteType.DELETE;
        } else {
            pkConstraintViolation = !this.rawIndex.exists(key);
        }
        if(pkConstraintViolation || this.nonPkConstraintViolation(values)) {
            return false;
        }
        this.doUpdate(txCtx, key, values, operationSet);
        return true;
    }

    private void doUpdate(TransactionContext txCtx, IKey key, Object[] values, OperationSetOfKey operationSet) {
        TransactionWrite entry = TransactionWrite.upsert(WriteType.UPDATE, values);
        if(operationSet == null){
            operationSet = new OperationSetOfKey(WriteType.UPDATE);
            this.updatesPerKeyMap.put(key, operationSet);
        } else {
            operationSet.lastWriteType = WriteType.UPDATE;
        }
        operationSet.put(txCtx.tid, entry);
        this.appendWrite(txCtx, key);
    }

    public IKey insertAndGetKey(TransactionContext txCtx, Object[] values){
        if(this.primaryKeyGenerator.isPresent()) {
            Object key_ = this.primaryKeyGenerator.get().next();
            // set to record
            values[this.rawIndex.columns()[0]] = key_;
            IKey key = KeyUtils.buildRecordKey(this.rawIndex.schema().getPrimaryKeyColumns(), new Object[]{key_});
            if (this.insert(txCtx, key, values)) {
                return key;
            }
            return null;
        }
        IKey key = KeyUtils.buildRecordKey(this.rawIndex.schema().getPrimaryKeyColumns(), values);
        if(this.insert(txCtx, key, values)){
            return key;
        }
        return null;
    }

    @Override
    public boolean remove(TransactionContext txCtx, IKey key){
        return this.removeOpt(txCtx, key).isPresent();
    }

    public Optional<Object[]> removeOpt(TransactionContext txCtx, IKey key) {
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        if (operationSet != null && operationSet.lastWriteType != WriteType.DELETE){
            if(operationSet.peak().key > txCtx.tid) {
                throw new RuntimeException("An attempt to delete with a lower TID than the original record. Perhaps a misconfigured function annotation?");
            }
            Object[] lastRecord = operationSet.peak().val().record;
            TransactionWrite entry = TransactionWrite.delete(WriteType.DELETE);
            operationSet.put(txCtx.tid, entry);
            operationSet.lastWriteType = WriteType.DELETE;
            this.appendWrite(txCtx, key);
            return Optional.of( lastRecord );
            // does this key even exist? if not, don't even need to save it on transaction metadata
        }
        Object[] obj = this.rawIndex.lookupByKey(key);
        if(obj != null) {
            // that means we haven't had any previous transaction performing writes to this key
            operationSet = new OperationSetOfKey(WriteType.DELETE);
            this.updatesPerKeyMap.put( key, operationSet );
            TransactionWrite entry = TransactionWrite.delete(WriteType.DELETE);
            operationSet.put(txCtx.tid, entry);
            this.appendWrite(txCtx, key);
            return Optional.of( obj );
        }
        return Optional.empty();
    }

    @Override
    public int[] indexColumns() {
        return this.rawIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.rawIndex.containsColumn(columnPos);
    }

    /**
     * Called when a constraint is violated, leading to a transaction abort
     */
    public void undoTransactionWrites(TransactionContext txCtx){
        Set<IKey> writeSet = this.removeWriteSet(txCtx);
        if(writeSet == null) return;
        for(IKey key : writeSet) {
            // do we have a record written in the corresponding index? always yes. if no, it is a bug
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            operationSetOfKey.poll();
        }
        writeSet.clear();
        WRITE_SET_BUFFER.addLast(writeSet);
    }

    @Override
    public void reset(){
        this.writeSetMap.clear();
        if(this.rawIndex instanceof UniqueHashBufferIndex){
            this.rawIndex.reset();
        }
        this.updatesPerKeyMap.clear();
        this.keysToFlush.clear();
    }

    public int checkpoint(long maxTid){
        if(this.keysToFlush.isEmpty() || this.updatesPerKeyMap.isEmpty()) return 0;
        int numRecords = 0;
        this.rawIndex.lock();
        for (IKey key : this.keysToFlush) {
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            if (operationSetOfKey == null) {
                this.rawIndex.unlock();
                throw new RuntimeException("Error on retrieving operation set for key " + key);
            }
            Entry<Long, TransactionWrite> entry = operationSetOfKey.floorEntry(maxTid);
            if (entry == null) continue;
            // is the head?
            if (operationSetOfKey.peak() == entry) {
                this.keysToFlush.remove(key);
            }
            operationSetOfKey.removeChildren(entry);
            switch (operationSetOfKey.lastWriteType) {
                case UPDATE -> this.rawIndex.upsert(key, entry.val().record);
                case INSERT -> this.rawIndex.insert(key, entry.val().record);
                case DELETE -> this.rawIndex.delete(key);
            }
            numRecords++;
        }
        this.rawIndex.unlock();
        if(numRecords > 0) {
            this.rawIndex.flush();
        }
        return numRecords;
    }

    public void cleanup(long maxTid){
        for(IKey key : this.keysToFlush){
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            if(operationSetOfKey == null){
                throw new RuntimeException("Error on retrieving operation set for key "+key);
            }
            Entry<Long, TransactionWrite> entry = operationSetOfKey.floorEntry(maxTid);
            if (entry == null) continue;
            if(operationSetOfKey.peak() == entry) {
                this.keysToFlush.remove(key);
            }
            operationSetOfKey.removeChildren(entry);
        }
    }

    public void installWrites(TransactionContext txCtx){
        Set<IKey> writeSet = this.removeWriteSet(txCtx);
        if(writeSet == null) {
            LOGGER.log(WARNING, "Primary Index: Transaction ID "+txCtx.tid+" could not be found in write set. Perhaps concurrent threads are set to the same TID?");
            return;
        }
        this.keysToFlush.addAll(writeSet);
        writeSet.clear();
        WRITE_SET_BUFFER.addLast(writeSet);
    }

    public void appendWrite(TransactionContext txCtx, IKey key){
        this.writeSetMap.computeIfAbsent(txCtx.tid, _ -> Objects.requireNonNullElseGet(WRITE_SET_BUFFER.poll(), HashSet::new)).add(key);
    }

    public Set<IKey> removeWriteSet(TransactionContext txCtx){
        return this.writeSetMap.remove(txCtx.tid);
    }

    public ReadWriteIndex<IKey> underlyingIndex(){
        return this.rawIndex;
    }

    @Override
    public Iterator<Object[]> iterator(TransactionContext txCtx, IKey[] keys) {
        return new MultiVersionIterator(txCtx, keys);
    }

    /**
     * Used for scanning short number of keys
     * 1 - collect all records eligible in this.updatesPerKeyMap (use a hashmap)
     * 2 - build an iterator object that iterates over the records of the underling index
     * 3. for each key, check if it is present in the set (1)
     * 3a. if so, just return the fresh record as next. remember to remove it from the set
     * 3b. otherwise, return the object as it is
     */
    protected class MultiVersionIterator implements Iterator<Object[]>{

        private final TransactionContext txCtx;
        private final IKey[] keys;
        private int idx = 0;
        private Object[] next;

        public MultiVersionIterator(TransactionContext txCtx, IKey[] keys){
            this.txCtx = txCtx;
            this.keys = keys;
        }

        @Override
        public boolean hasNext() {
            while(this.idx < this.keys.length){
                OperationSetOfKey operation = updatesPerKeyMap.get(this.keys[this.idx]);
                if(operation == null) {
                    Object[] record = rawIndex.record(this.keys[this.idx]);
                    if(record != null){
                        this.next = record;
                        this.idx++;
                        return true;
                    }
                    this.idx++;
                    continue;
                }
                Entry<Long, TransactionWrite> obj = operation.floorEntry(this.txCtx.tid);
                if (obj != null && obj.val().type != WriteType.DELETE) {
                    this.next = obj.val().record;
                    this.idx++;
                    return true;
                }
                this.idx++;
            }
            return false;
        }

        @Override
        public Object[] next() {
            return this.next;
        }
    }

    public Object[] getRecord(long tid, IKey key){
        OperationSetOfKey operation = this.updatesPerKeyMap.get(key);
        if(operation != null){
            Entry<Long, TransactionWrite> entry = operation.floorEntry(tid);
            if(entry != null && entry.val().type != WriteType.DELETE) {
                return entry.val().record;
            }
        }
        return this.rawIndex.lookupByKey(key);
    }

    public Entry<Long, TransactionWrite> getFloorEntry(TransactionContext txCtx, IKey key){
        OperationSetOfKey operation = this.updatesPerKeyMap.get(key);
        return operation.floorEntry(txCtx.tid);
    }

    /**
     * Used in full scan
     */
    @Override
    public Iterator<Object[]> iterator(TransactionContext txCtx) {
        return new PrimaryIndexIterator(txCtx);
    }

    private final class PrimaryIndexIterator implements Iterator<Object[]> {

        private final TransactionContext txCtx;
        private final Iterator<Map.Entry<IKey, OperationSetOfKey>> iterator;
        private Object[] currRecord;

        public PrimaryIndexIterator(TransactionContext txCtx){
            this.txCtx = txCtx;
            this.iterator = updatesPerKeyMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            while(this.iterator.hasNext()){
                Map.Entry<IKey, OperationSetOfKey> next = this.iterator.next();
                Entry<Long, TransactionWrite> entry = next.getValue().getHigherEntryUpToKey(this.txCtx.tid);
                if(entry == null) {
                    this.currRecord = rawIndex.lookupByKey(next.getKey());
                    if(this.currRecord == null){
                        if(this.iterator.hasNext()) {
                            continue;
                        } else {
                            return false;
                        }
                    }
                    return true;
                }
                if(entry.val().type == WriteType.DELETE) {
                    if(this.iterator.hasNext()) {
                        continue;
                    } else {
                        return false;
                    }
                }
                /* is it returning a deleted entry???
                if(updatesPerKeyMap.get(next.getKey()).lastWriteType == WriteType.DELETE){
                    LOGGER.log(ERROR,"ATTENTION: "+this.txCtx.tid+" < "+updatesPerKeyMap.get(next.getKey()).peak().key);
                }
                */
                this.currRecord = entry.val().record;
                return true;
            }
            return false;
        }

        @Override
        public Object[] next() {
            return this.currRecord;
        }

    }

}
