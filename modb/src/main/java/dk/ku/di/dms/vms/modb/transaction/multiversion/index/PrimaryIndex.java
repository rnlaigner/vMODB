package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.modb.common.transaction.WriteType;
import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.pk.IPrimaryKeyGenerator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.OperationSetOfKey;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static dk.ku.di.dms.vms.modb.common.constraint.ConstraintConstants.*;

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

    private final UniqueHashIndex primaryKeyIndex;

    private final Map<IKey, OperationSetOfKey> updatesPerKeyMap;

    // for PK generation. for now, all strategies use this (auto, sequence, etc)
    private final IPrimaryKeyGenerator<?> primaryKeyGenerator;

    public PrimaryIndex(UniqueHashIndex primaryKeyIndex) {
        this.primaryKeyIndex = primaryKeyIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>();
        this.primaryKeyGenerator = null;
    }

    public PrimaryIndex(UniqueHashIndex primaryKeyIndex, IPrimaryKeyGenerator<?> primaryKeyGenerator) {
        this.primaryKeyIndex = primaryKeyIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>();
        this.primaryKeyGenerator = primaryKeyGenerator;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrimaryIndex && this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return this.primaryKeyIndex.key().hashCode();
    }

    @Override
    public IIndexKey key() {
        return this.primaryKeyIndex.key();
    }

    @Override
    public Schema schema() {
        return this.primaryKeyIndex.schema();
    }

    @Override
    public int[] columns() {
        return this.primaryKeyIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.primaryKeyIndex.containsColumn(columnPos);
    }

    @Override
    public IndexTypeEnum getType() {
        return this.primaryKeyIndex.getType();
    }

    @Override
    public int size() {
        return this.primaryKeyIndex.size();
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        // TODO return a non-unique index iterator if there is some. it would be faster since
        //  records are packed together in memory, connected through a linked list
        //  different from the sparse hash index
        return null;
        // return new UniqueKeySnapshotRecordIterator(this, this.primaryKeyIndex.iterator());
    }

    @Override
    public IRecordIterator<IKey> iterator(IKey[] keys) {
        return this.iterator.init(keys);
    }

    private final PrimaryIndexKeyIterator iterator = new PrimaryIndexKeyIterator();

    private static class PrimaryIndexKeyIterator implements IRecordIterator<IKey> {

        private IKey[] keys;
        private int pos;

        public PrimaryIndexKeyIterator init(IKey[] keys){
            this.keys = keys;
            this.pos = 0;
            return this;
        }

        @Override
        public IKey get() {
            return this.keys[this.pos];
        }

        @Override
        public void next() {
            this.pos++;
        }

        @Override
        public boolean hasElement() {
            return pos < keys.length;
        }

    }

    @Override
    public Object[] record(IKey key) {
        return this.lookupByKey(key);
    }

    /**
     * Same logic as {@link #lookupByKey}
     * @param key record key
     * @return whether the record exists for this transaction, respecting atomic visibility
     */
    @Override
    public boolean exists(IKey key) {

        // O(1)
        OperationSetOfKey opSet = this.updatesPerKeyMap.get(key);

        if(opSet != null){

            // why checking first if I am a WRITE. because by checking if I am right, I don't need to pay O(log n)
            // 1 write thread at a time. if that is a writer thread, does not matter my lastTid. I can just check the last write for this entry
            if( !TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly ){
                return opSet.lastWriteType != WriteType.DELETE;
            }

            // O(log n)
            var floorEntry = opSet.updateHistoryMap.floorEntry(TransactionMetadata.TRANSACTION_CONTEXT.get().lastTid);
            if(floorEntry == null) return this.primaryKeyIndex.exists(key);
            return floorEntry.val().type != WriteType.DELETE;
        }

        return this.primaryKeyIndex.exists(key);

    }

    /**
     */
    @Override
    public boolean exists(long address) {
        int pk = UNSAFE.getInt( address + Header.SIZE );
        if(pk == 0) return false;
        var key = SimpleKey.of(pk);
        return this.exists( key );
    }

//    @Override
//    public long address(IKey key) {
//        return this.primaryKeyIndex.address(key);
//    }


    // every condition checked is proceeded by a completion handler
    // can maintain the boolean return but all operators (for read queries)
    // necessarily require appending to a memory result space

    @Override
    public boolean checkCondition(IRecordIterator<IKey> iterator, FilterContext filterContext) {
        IKey key = iterator.get();
        return this.checkCondition(key, filterContext);
    }

    @Override
    public boolean checkCondition(IKey key, FilterContext filterContext) {
        Object[] record = this.lookupByKey(key);
        if(record == null) return false;
        return this.checkConditionVersioned(filterContext, record);
    }

    /**
     * This is the basic check condition. Does not take into consideration the
     * versioned values.
     * @param record record values
     * @param filterContext the filter to be applied
     * @return the correct data item version
     */
    @SuppressWarnings("unchecked")
    public boolean checkConditionVersioned(FilterContext filterContext, Object[] record){

        if(filterContext == null) return true;

        boolean conditionHolds = true;

        // the number of filters to apply
        int filterIdx = 0;

        // the filter index on which a given param (e.g., literals, zero, 1, 'SURNAME', etc) should apply
        int biPredIdx = 0;

        // simple predicates, do not involve input params (i.e, NULL, NOT NULL, EXISTS?, etc)
        int predIdx = 0;

        while( conditionHolds && filterIdx < filterContext.filterTypes.size() ){

            int columnIndex = filterContext.filterColumns.get(filterIdx);

            Object val = record[columnIndex];

            // it is a literal passed to the query
            if(filterContext.filterTypes.get(filterIdx) == FilterType.BP) {
                conditionHolds = filterContext.biPredicates.get(biPredIdx).
                        apply(val, filterContext.biPredicateParams.get(biPredIdx));
                biPredIdx++;
            } else {
                conditionHolds = filterContext.predicates.get(predIdx).test( val );
                predIdx++;
            }

            filterIdx++;

        }

        return conditionHolds;

    }

    /**
     * Methods for insert/update operations.
     */
    private boolean nonPkConstraintViolation(Object[] values) {

        Map<Integer, ConstraintReference> constraints = schema().constraints();

        boolean violation = false;

        for(Map.Entry<Integer, ConstraintReference> c : constraints.entrySet()) {

            switch (c.getValue().constraint.type){

                case NUMBER -> {
                    switch (schema().columnDataType(c.getKey())) {
                        case INT -> violation = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , 0, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> violation = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , 0L, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> violation = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , 0f, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> violation = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , 0d, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }

                case NUMBER_WITH_VALUE -> {
                    Object valToCompare = c.getValue().asValueConstraint().value;
                    switch (schema().columnDataType(c.getKey())) {
                        case INT -> violation = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , (int)valToCompare, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> violation = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , (long)valToCompare, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> violation = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , (float)valToCompare, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> violation = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , (double)valToCompare, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }

                case NULLABLE ->
                        violation = NullableTypeConstraintHelper.eval(values[c.getKey()], c.getValue().constraint);

                case CHARACTER ->
                        violation = CharOrStringTypeConstraintHelper.eval((String) values[c.getKey()], c.getValue().constraint );

            }

            if(violation) return true;

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
                default ->
                        throw new IllegalStateException("Cannot compare the constraint "+constraint+" for number type.");
            }

        }

    }

    public Object[] lookupByKey(IKey key){
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        if ( operationSet != null ){
            if(TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly) {
                var entry = operationSet.updateHistoryMap.floorEntry(TransactionMetadata.TRANSACTION_CONTEXT.get().lastTid);
                if (entry != null){
                    return (entry.val().type != WriteType.DELETE ? entry.val().record : null);
                }
            } else
                return operationSet.lastWriteType != WriteType.DELETE ? operationSet.lastVersion : null;
        }
        // it is a readonly
        if(this.primaryKeyIndex.exists(key)) {
            return this.primaryKeyIndex.record(key);
        }
        return null;
    }

    /**
     * TODO if cached value is not null, then extract the updated columns to make constraint violation check faster
     */
    @Override
    public boolean insert(IKey key, Object[] values) {

        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );

        boolean pkConstraintViolation;
        // if not delete, violation (it means some tid has written to this PK before)
        if(operationSet != null){
            pkConstraintViolation = operationSet.lastWriteType != WriteType.DELETE;
        } else {
            // let's check now the index itself. it exists, it is a violation
            pkConstraintViolation = this.primaryKeyIndex.exists(key);
        }

        if(pkConstraintViolation || this.nonPkConstraintViolation(values)) {
            return false;
        }

        // create a new insert
        TransactionWrite entry = new TransactionWrite(WriteType.INSERT, values);

        if(operationSet == null){
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put( key, operationSet );
        }

        operationSet.updateHistoryMap.put( TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
        operationSet.lastWriteType = WriteType.INSERT;
        operationSet.lastVersion = values;

        return true;

    }

    /**
     * Pending: On abort, must return the primary key generator to a value
     * previous to the start of the batch
     */
    public Tuple<Object, IKey> insertAndGet(Object[] values){
        if(this.primaryKeyGenerator != null){
            Object key_ = this.primaryKeyGenerator.next();
            // when generating, it is always the first-and-only column
            values[this.primaryKeyIndex.columns()[0]] = key_;
            IKey key = KeyUtils.buildKey( key_ );
            if(this.insert( key, values )){
                return new Tuple<>(key_, key);
            }
        } else {
            int[] pkColumns = this.schema().getPrimaryKeyColumns();
            IKey key = KeyUtils.buildRecordKey(pkColumns, values);
            if(this.insert( key, values )){
                Object[] pkValues = new Object[pkColumns.length];
                // build pk values array
                for(int i = 0; i < pkColumns.length; i++){
                    pkValues[i] = values[pkColumns[i]];
                }
                return new Tuple<>(pkValues, key);
            }
        }
        return null;
    }

    @Override
    public boolean update(IKey key, Object[] values) {

        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );

        boolean pkConstraintViolation;
        if ( operationSet != null ){
            pkConstraintViolation = operationSet.lastWriteType == WriteType.DELETE;
        } else {
            pkConstraintViolation = !this.primaryKeyIndex.exists(key);
        }

        if(pkConstraintViolation || nonPkConstraintViolation(values)) {
            return false;
        }

        // create a new update
        TransactionWrite entry = new TransactionWrite(WriteType.UPDATE, values);

        if(operationSet == null){
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put( key, operationSet );
        }

        operationSet.updateHistoryMap.put( TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
        operationSet.lastWriteType = WriteType.UPDATE;
        operationSet.lastVersion = values;

        return true;

    }

    @Override
    public boolean delete(IKey key) {

        // O(1)
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );

        if ( operationSet != null ){

            if(operationSet.lastWriteType != WriteType.DELETE){

                TransactionWrite entry = new TransactionWrite(WriteType.DELETE, null);
                // amortized O(1)
                operationSet.updateHistoryMap.put(TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
                operationSet.lastWriteType = WriteType.DELETE;

                return true;
            }
            // does this key even exist? if not, don't even need to save it on transaction metadata

        } else if(this.primaryKeyIndex.exists(key)) {

            // that means we haven't had any previous transaction performing writes to this key
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put( key, operationSet );

            TransactionWrite entry = new TransactionWrite(WriteType.DELETE, null);

            operationSet.updateHistoryMap.put(TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
            operationSet.lastWriteType = WriteType.DELETE;

            return true;
        }

        return false;
    }

    /**
     * Called when a constraint is violated, leading to a transaction abort
     */
    @Override
    public void undoTransactionWrite(IKey key){
        // do we have a record written in the corresponding index? always yes. if no, it is a bug
        OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
        operationSetOfKey.updateHistoryMap.poll();
    }

    @Override
    public void installWrites(){
        for(Map.Entry<IKey, OperationSetOfKey> entry : this.updatesPerKeyMap.entrySet()) {
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(entry.getKey());
            switch (operationSetOfKey.lastWriteType){
                case UPDATE -> this.primaryKeyIndex.update(entry.getKey(), operationSetOfKey.lastVersion);
                case INSERT -> this.primaryKeyIndex.insert(entry.getKey(), operationSetOfKey.lastVersion);
                case DELETE -> this.primaryKeyIndex.delete(entry.getKey());
            }
            operationSetOfKey.updateHistoryMap.clear();
        }
    }

    public ReadWriteIndex<IKey> underlyingIndex(){
        return this.primaryKeyIndex;
    }

}
