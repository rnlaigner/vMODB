package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.transaction.multiversion.OperationSet;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.DataItemVersion;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.InsertOp;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A transaction management facade
 * Responsibilities:
 * - Keep track of modifications
 * - Commit (write to the actual corresponding regions of memories)
 *
 * AbstractIndex must be modified so reads can return the correct (versioned/consistent) value
 *
 * Repository facade parses the request. Transaction facade deals with low-level operations
 *
 * Batch-commit aware. That means when a batch comes, must make data durable.
 *
 * TODO in order to accommodate two or more VMSs in the same resource,
 *  need to make this class an instance and put it into modb modules
 */
public class TransactionFacade {

    private TransactionFacade(){}

    // key: tid. always ordered by default (insertion order)
    // single-thread, no need to synchronize
    private static final Map<Long, List<DataItemVersion>> writesPerTransaction;

    // key: PK accessed by many read-only transactions
    private static final Map<IIndexKey, Map<IKey, OperationSet>> writesPerIndexAndKey;

    static {
        // writesPerTransaction = Collections.synchronizedMap( new LinkedHashMap<>() );
        writesPerTransaction = new ConcurrentHashMap<>();
        writesPerIndexAndKey = new ConcurrentHashMap<>();
    }

    /****** ENTITY *******/

    public static void insert(Table table, Object[] values){

        IKey pk = KeyUtils.buildRecordKey(table.getSchema(), table.getSchema().getPrimaryKeyColumns(), values);
        Map<IKey, OperationSet> keyMap = writesPerIndexAndKey.get( table.primaryKeyIndex().key() );

        boolean a = pkConstraintViolation(table, pk, keyMap);
        boolean b = constraintViolation(table, values);
        if(a || b){

            long threadId = Thread.currentThread().getId();
            long tid = TransactionMetadata.tid(threadId);
            List<DataItemVersion> writesTid = writesPerTransaction.get(tid);

            if(writesTid == null){
                // throw right away
                throw new IllegalStateException("Primary key"+pk+"already exists for this table: "+table.getName());
            }

            // clean writes from this transaction

            for(DataItemVersion v : writesTid){

                // do we have a write in the corresponding index? always yes. if no, it is a bug
                Map<IKey, OperationSet> operations = writesPerIndexAndKey.get( v.indexKey() );
                if(operations != null){
                    operations.remove(v.pk());
                }

            }

            throw new IllegalStateException("Primary key"+pk+"already exists for this table: "+table.getName());
        }

        int recordSize = table.getSchema().getRecordSizeWithoutHeader();

        MemoryRefNode memRef = MemoryManager.getTemporaryDirectMemory(recordSize);

        long threadId = Thread.currentThread().getId();
        long tid = TransactionMetadata.tid(threadId);

        long addressToWriteTo = memRef.address();

        int maxColumns = table.getSchema().columnOffset().length;
        int index;

        // TODO For embed and default?, can be directly put in a buffer instead of saving an object.
        for(index = 0; index < maxColumns; index++) {

            DataType dt = table.getSchema().getColumnDataType(index);

            DataTypeUtils.callWriteFunction( addressToWriteTo,
                    dt,
                    values[index] );

            addressToWriteTo += dt.value;

        }

        IIndexKey indexKey = table.primaryKeyIndex().key();

        InsertOp dataItemVersion = InsertOp.insert( tid, addressToWriteTo, indexKey, pk );

        writesPerTransaction.putIfAbsent( tid, new ArrayList<>(5) );
        writesPerTransaction.get( tid ).add( dataItemVersion );

        if(keyMap == null){
            keyMap = new HashMap<>();
            writesPerIndexAndKey.putIfAbsent(indexKey, keyMap);
        }

        OperationSet operationSet = new OperationSet();
        operationSet.lastWriteType = OperationSet.Type.INSERT;
        operationSet.insertOp = dataItemVersion;

        keyMap.put(pk, operationSet);

    }

    private static boolean pkConstraintViolation(Table table, IKey pk, Map<IKey, OperationSet> keyMap){
        if(keyMap != null && keyMap.get(pk) != null){
            //  writes in this batch for this pk
            return true;
        }
        // lets check now the index itself
        return table.primaryKeyIndex().exists(pk);
    }

    private static boolean constraintViolation(Table table, Object[] values) {

        Map<Integer, ConstraintReference> constraints = table.getSchema().constraints();

        boolean violation = false;

        for(Map.Entry<Integer, ConstraintReference> c : constraints.entrySet()) {

            if(c.getValue().constraint == ConstraintEnum.NOT_NULL){
                violation = values[c.getKey()] == null;
            } else {

                switch (table.getSchema().getColumnDataType(c.getKey())) {

                    case INT -> violation = ConstraintHelper.eval((int)values[c.getKey()] , 0, Integer::compareTo, c.getValue().constraint);
                    case LONG, DATE -> violation = ConstraintHelper.eval((long)values[c.getKey()] , 0L, Long::compareTo, c.getValue().constraint);
                    case FLOAT -> violation = ConstraintHelper.eval((float)values[c.getKey()] , 0f, Float::compareTo, c.getValue().constraint);
                    case DOUBLE -> violation = ConstraintHelper.eval((double)values[c.getKey()] , 0d, Double::compareTo, c.getValue().constraint);
                    case CHAR -> {
                        //
                    }
                    case BOOL -> {
                        //
                    }
                    default -> throw new IllegalStateException("Data type not recognized!");
                }
            }

            if(violation) return true;

        }

        return false;

    }

    private static class ConstraintHelper {

        public static <T> boolean eval(T v1, T v2, Comparator<T> comparator, ConstraintEnum constraint){

            if(constraint == ConstraintEnum.POSITIVE_OR_ZERO){
                return comparator.compare(v1, v2) >= 0;
            }
            if(constraint == ConstraintEnum.POSITIVE){
                return comparator.compare(v1, v2) > 0;
            }
            return false;
        }

    }

    /****** SCAN *******/

    public static MemoryRefNode run(List<WherePredicate> wherePredicates,
                                    IndexScanWithProjection operator){

        long threadId = Thread.currentThread().getId();
        long tid = TransactionMetadata.tid(threadId);

        List<Object> keyList = new ArrayList<>(operator.index.columns().length);
        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then build filter
            if(operator.index.columnsHash().contains( wherePredicate.columnReference.columnPosition )){
                keyList.add( wherePredicate.value );
            } else {
                wherePredicatesNoIndex.add(wherePredicate);
            }
        }

        FilterContext filterContext = FilterContextBuilder.build(wherePredicatesNoIndex);

        // build input
        IKey inputKey = KeyUtils.buildInputKey(keyList.toArray());

        Map<IKey, OperationSet> operationSetMap = writesPerIndexAndKey.get(operator.index.key());

        ConsistentView consistentView = new ConsistentView(operator.index, operationSetMap, tid);

        return operator.run( consistentView, filterContext, inputKey );
    }

    public static MemoryRefNode run(List<WherePredicate> wherePredicates,
                                    FullScanWithProjection operator){

        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);

        Map<IKey, OperationSet> operationSetMap = writesPerIndexAndKey.get(operator.index.key());

        long threadId = Thread.currentThread().getId();
        long tid = TransactionMetadata.tid(threadId);

        ConsistentView consistentView = new ConsistentView(operator.index, operationSetMap, tid);

        return operator.run( consistentView, filterContext );

    }

    /* COMMIT *******/

    /**
     * Only log those data versions until the corresponding batch.
     * TIDs are not necessarily a sequence.
     * TODO merge the last values of each data item to avoid multiple writes
     *      use virtual threads to speed up
     *
     * @param lastTid the last tid of the batch
     */
    public static void log(long lastTid, Catalog catalog){

        // make state durable
        // get buffered writes in transaction facade and merge in memory

        for(var entry : writesPerIndexAndKey.entrySet()){

            // for each index, get the last update
            ReadWriteIndex<IKey> index = catalog.getIndexByKey(entry.getKey());

            for(var keyEntry : entry.getValue().entrySet()){



                switch (keyEntry.getValue().lastWriteType){

                    case INSERT -> {

                        if(keyEntry.getValue().insertOp.tid() <= lastTid) {
                            // memcopy
                            index.insert(keyEntry.getKey(), keyEntry.getValue().insertOp.bufferAddress);
                        }

                    }

                    case DELETE -> {
                        // put bit active as 0
                    }

                    case UPDATE -> {
                        // memcopy

                    }

                }

            }



            // log index since all updates are made
            index.asUniqueHashIndex().buffer().log();

            writesPerIndexAndKey.remove(entry.getKey());

        }


        for(var tx : writesPerTransaction.entrySet()){
            if(tx.getKey() > lastTid) break; // can stop
            writesPerTransaction.remove( tx.getKey() );
        }

        // TODO must modify corresponding secondary indexes too

    }

}
