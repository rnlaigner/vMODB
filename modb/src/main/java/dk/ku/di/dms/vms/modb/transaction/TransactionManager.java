package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionContext;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.execution.operators.minmax.IndexAggregateScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.IndexScan;
import dk.ku.di.dms.vms.modb.query.planner.SimplePlanner;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.UniqueSecondaryIndex;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

/**
 * A transaction management facade
 * Responsibilities:
 * - Keep track of modifications
 * - Commit (write to the actual corresponding regions of memories)
 * AbstractIndex must be modified so reads can return the correct (versioned/consistent) value
 * Repository facade parses the request. Transaction facade deals with low-level operations
 * Batch-commit aware. That means when a batch comes, must make data durable.
 * in order to accommodate two or more VMSs in the same resource,
 *  it would need to make this class an instance (no static methods) and put it into modb modules
 */
public final class TransactionManager implements OperationalAPI, ITransactionManager {

    private static final System.Logger LOGGER = System.getLogger(TransactionManager.class.getName());

    private final Map<Long, TransactionContext> txCtxMap;

    private final Analyzer analyzer;

    private final SimplePlanner planner;

    /**
     * Operators output results
     * They are read-only operations, do not modify data
     */
    private final Map<String, AbstractSimpleOperator> queryPlanCacheMap;

    private final Map<String, Table> catalog;

    public TransactionManager(Map<String, Table> catalog){
        this.planner = new SimplePlanner();
        this.analyzer = new Analyzer(catalog);
        this.catalog = catalog;
        this.queryPlanCacheMap = new ConcurrentHashMap<>();
        this.txCtxMap = new ConcurrentHashMap<>(2048*10);
    }

    private boolean fkConstraintViolation(TransactionContext txCtx, Table table, Object[] values){
        for(Map.Entry<PrimaryIndex, int[]> entry : table.foreignKeys().entrySet()){
            IKey fk = KeyUtils.buildRecordKey( entry.getValue(), values );
            // have some previous TID deleted it? or simply not exists
            return !entry.getKey().exists(txCtx, fk);
        }
        return false;
    }

    @Override
    public List<Object[]> fetch(final Table table, final SelectStatement selectStatement) {
        return this.fetch(table, selectStatement, selectStatement.whereClause);
    }

    /**
     * Would be nice to store the decision of which method to call or the method call itself
     */
    @Override
    public List<Object[]> fetch(final Table table, final SelectStatement selectStatement, List<WhereClauseElement> whereClauseElements) {
        String sqlAsKey = selectStatement.SQL.toString();
        AbstractSimpleOperator scanOperator = this.queryPlanCacheMap.computeIfAbsent(sqlAsKey,
                (_) -> {
                    QueryTree queryTree = this.analyzer.analyze(selectStatement);
                    return this.planner.plan(queryTree);
                });
        List<WherePredicate> wherePredicates;
        if(whereClauseElements.isEmpty()) {
            wherePredicates = Collections.emptyList();
        } else {
            wherePredicates = this.analyzer.analyzeWhere(table, whereClauseElements);
        }
        if(scanOperator.isIndexScan()) {
            IndexScan indexScan = scanOperator.asIndexScan();
            if(wherePredicates.get(0).expression == ExpressionTypeEnum.EQUALS) {
                IKey key = this.getIndexedKeysFromWhereClause(wherePredicates, indexScan.index());
                if(containsNonIndexColumns(wherePredicates, indexScan.index())) {
                    List<WherePredicate> nonIdxClause = this.getNonIndexedColumnsWhereClause(wherePredicates, indexScan.index());
                    FilterContext filterContext = FilterContextBuilder.build(nonIdxClause);
                    return scanOperator.asIndexScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key, filterContext, selectStatement.limit);
                } else {
                    return indexScan.runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key, selectStatement.limit);
                }
            } else {
                // can only be IN
                IKey[] keys = this.getMultiKeysFromWhereClause(wherePredicates, indexScan.index());
                if(containsNonIndexColumns(wherePredicates, indexScan.index())) {
                    List<WherePredicate> nonIdxClause = this.getNonIndexedColumnsWhereClause(wherePredicates, indexScan.index());
                    FilterContext filterContext = FilterContextBuilder.build(nonIdxClause);
                    return indexScan.runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), keys, selectStatement.distinct, filterContext);
                }
                return indexScan.runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), keys, selectStatement.distinct);
            }
        } else if(scanOperator.isIndexScanWithOrder()) {
            var indexScanWithOrder = scanOperator.asIndexScanWithOrder();
            IKey key = this.getIndexedKeysFromWhereClause(wherePredicates, indexScanWithOrder.index());
            if(containsNonIndexColumns(wherePredicates, indexScanWithOrder.index())) {
                List<WherePredicate> nonIdxClause = this.getNonIndexedColumnsWhereClause(wherePredicates, indexScanWithOrder.index());
                FilterContext filterContext = FilterContextBuilder.build(nonIdxClause);
                return indexScanWithOrder.runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key, filterContext, selectStatement.limit);
            } else {
                return indexScanWithOrder.runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key, selectStatement.limit);
            }
        } else if(scanOperator.isFullScanWithOrder()) {
            FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
            return scanOperator.asFullScanWithOrder().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), filterContext, selectStatement.limit);
        } else if(scanOperator.isIndexAggregationScan()) {
            return scanOperator.asIndexAggregationScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()));
        } else if(scanOperator.isIndexMultiAggregationScan()) {
            IKey key = this.getIndexedKeysFromWhereClause(wherePredicates, scanOperator.asIndexMultiAggregationScan().index());
            return scanOperator.asIndexMultiAggregationScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key);
        } else {
            // future optimization is filter not including the columns of partial or non-unique index
            if(wherePredicates.isEmpty()){
                return scanOperator.asFullScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), selectStatement.limit);
            }
            FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
            return scanOperator.asFullScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), filterContext, selectStatement.limit);
        }
    }

    /**
     * Best guess return type. Differently from the parameter type received.
     * @param selectStatement a select statement
     * @return the query result in a memory space
     */
    @Override
    public MemoryRefNode fetchMemoryReference(Table table, SelectStatement selectStatement) {
        String sqlAsKey = selectStatement.SQL.toString();
        AbstractSimpleOperator scanOperator = this.queryPlanCacheMap.getOrDefault( sqlAsKey, null );
        List<WherePredicate> wherePredicates;
        if(scanOperator == null){
            QueryTree queryTree = this.analyzer.analyze(selectStatement);
            wherePredicates = queryTree.wherePredicates;
            scanOperator = this.planner.plan(queryTree);
            this.queryPlanCacheMap.put(sqlAsKey, scanOperator);
        } else {
            // get only the where clause params
            wherePredicates = this.analyzer.analyzeWhere(table, selectStatement.whereClause);
        }
        MemoryRefNode memRes;
        // complete for all types or migrate the choice to transaction facade
        // make an enum, it is easier
        if(scanOperator.isIndexScan()){
            // build keys and filters
            memRes = this.run(wherePredicates, scanOperator.asIndexScan());
        } else if(scanOperator.isIndexAggregationScan()){
            memRes = this.run(wherePredicates, scanOperator.asIndexAggregationScan());
        } else {
            // build only filters
            memRes = this.run(table, wherePredicates, scanOperator.asFullScan());
        }
        return memRes;
    }

    /**
     * finish at some point. can we extract the column values and make a special api for the facade? only if it is a single key
     */
    public void issue(Table table, IStatement statement) throws AnalyzerException {
        switch (statement.getType()){
            case UPDATE -> {
                List<WherePredicate> wherePredicates = this.analyzer.analyzeWhere(
                        table, statement.asUpdateStatement().whereClause);
                // this.planner.getOptimalIndex(table, wherePredicates);
                // TODO plan update and delete in planner. only need to send where predicates and not a query tree like a select
                // UpdateOperator.run(statement.asUpdateStatement(), table.primaryKeyIndex() );
            }
            case INSERT -> {
                // TODO get columns, put object array in order and submit to entity api
            }
            case DELETE -> { }
            default -> throw new IllegalStateException("Statement type cannot be identified.");
        }
    }

    /****** ENTITY *******/

    @Override
    public List<Object[]> getAll(Table table){
        List<Object[]> res = new ArrayList<>();
        Iterator<Object[]> iterator = table.primaryKeyIndex().iterator(this.txCtxMap.get(Thread.currentThread().threadId()));
        while(iterator.hasNext()){
            res.add(iterator.next());
        }
        return res;
    }

    @Override
    public void insertAll(Table table, List<Object[]> objects){
        // get tid, do all the checks, etc
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        for(Object[] entry : objects) {
            this.doInsert(txCtx, table, entry);
        }
    }

    @Override
    public void deleteAll(Table table, List<Object[]> objects) {
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        for(Object[] entry : objects) {
            IKey pk = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), entry);
            this.deleteByKey(txCtx, table, pk);
        }
    }

    @Override
    public void updateAll(Table table, List<Object[]> objects) {
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        for(Object[] entry : objects) {
            this.update(txCtx, table, entry);
        }
    }

    /**
     * Not yet considering this record can serve as FK to a record in another table.
     */
    @Override
    public void delete(Table table, Object[] values) {
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        IKey pk = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), values);
        this.deleteByKey(txCtx, table, pk);
    }

    @Override
    public void deleteByKey(Table table, Object[] keyValues) {
        IKey pk = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), keyValues);
        this.deleteByKey(this.txCtxMap.get(Thread.currentThread().threadId()), table, pk);
    }

    /**
     * @param table The corresponding table
     * @param pk The primary key
     */
    private void deleteByKey(TransactionContext txCtx, Table table, IKey pk){
        Optional<Object[]> opt = table.primaryKeyIndex().removeOpt(txCtx, pk);
        if(opt.isPresent()){
            txCtx.indexes.add(table.primaryKeyIndex());
            for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
                txCtx.indexes.add(secIndex);
                secIndex.remove(txCtx, pk, opt.get());
            }
            for(var entry : table.partialIndexMap.entrySet()){
                // does the record "fits" the partial index?
                Tuple<Integer, Object> check = table.partialIndexMetaMap.get( entry.getKey() );
                if (table.primaryKeyIndex().meetPartialIndex(opt.get(), check.t1(), check.t2() )){
                    txCtx.indexes.add(entry.getValue());
                    entry.getValue().remove(txCtx, pk);
                }
            }
        }
    }

    @Override
    public boolean exists(PrimaryIndex index, Object[] valuesOfKey){
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), valuesOfKey);
        return index.exists(this.txCtxMap.get(Thread.currentThread().threadId()), pk);
    }

    @Override
    public Object[] lookupByKey(PrimaryIndex index, Object[] valuesOfKey){
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), valuesOfKey);
        return index.lookupByKey(this.txCtxMap.get(Thread.currentThread().threadId()), pk);
    }

    /**
     * @param table The corresponding database table
     * @param values The fields extracted from the entity
     */
    @Override
    public void insert(Table table, Object[] values){
        this.doInsert(this.txCtxMap.get(Thread.currentThread().threadId()), table, values);
    }

    private Object[] doInsert(TransactionContext txCtx, Table table, Object[] values) {
        PrimaryIndex primaryIndex = table.primaryKeyIndex();
        if(this.fkConstraintViolation(txCtx, table, values)){
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Foreign key constraint violation in table " + table.getName());
        }
        IKey pk = primaryIndex.insertAndGetKey(txCtx, values);
        if(pk == null) {
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Constraint violation in table " + table.getName() + ". Record:\n" + Arrays.stream(values).toList());
        }
        trackIndexes(txCtx, table, values, primaryIndex, pk);
        return values;
    }

    private static void trackIndexes(TransactionContext txCtx, Table table, Object[] values, PrimaryIndex primaryIndex, IKey pk) {
        txCtx.indexes.add(primaryIndex);
        // iterate over secondary indexes to insert the new write
        // this is the delta. records that the underlying index does not know yet
        for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
            txCtx.indexes.add(secIndex);
            secIndex.insert(txCtx, pk, values);
        }
        if(table.partialIndexMap.isEmpty()) {
            return;
        }
        for (var entry : table.partialIndexMap.entrySet()) {
            // does the record "fits" the partial index?
            Tuple<Integer, Object> check = table.partialIndexMetaMap.get(entry.getKey());
            if (primaryIndex.meetPartialIndex(values, check.t1(), check.t2())) {
                txCtx.indexes.add(entry.getValue());
                entry.getValue().insert(txCtx, pk, values);
            }
        }
    }

    @Override
    public Object[] insertAndGet(Table table, Object[] values){
        return this.doInsert(this.txCtxMap.get(Thread.currentThread().threadId()), table, values);
    }

    @Override
    public void upsert(Table table, Object[] values){
        PrimaryIndex primaryIndex = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(primaryIndex.underlyingIndex().schema().getPrimaryKeyColumns(), values);
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        if(primaryIndex.upsert(txCtx, pk, values)) {
            // FIXME must check if it is insert in order to insert in the secondary indexes
            //  update may also lead to changes in the secondary index (e.g., a column that requires readdressing)
            trackIndexes(txCtx, table, values, primaryIndex, pk);
            return;
        }
        this.undoTransactionWrites(txCtx);
        throw new RuntimeException("Constraint violation.");
    }

    @Override
    public void update(Table table, Object[] values) {
        this.update(this.txCtxMap.get(Thread.currentThread().threadId()), table, values);
    }

    /**
     * Iterate over all indexes, get the corresponding writes of this tid and remove them
     * This method can be called in parallel by transaction facade without any risk
     */
    private void update(TransactionContext txCtx, Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), values);
        if(!index.update(txCtx, pk, values)){
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Primary key constraint violation. Table: "+table.getName()+" Key: "+pk);
        }
        if(this.fkConstraintViolation(txCtx, table, values)){
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Foreign key constraint violation. Table: "+table.getName()+" Key: "+pk);
        }
        txCtx.indexes.add(index);
    }

    /**
     * how can I do that more optimized? creating another interface so secondary indexes also have the #undoTransactionWrites ?
     * INDEX_WRITES can have primary indexes and secondary indexes...
     */
    private void undoTransactionWrites(TransactionContext txCtx){
        for(IMultiVersionIndex index : txCtx.indexes) {
            index.undoTransactionWrites(txCtx);
        }
    }

    /****** SCAN OPERATORS *******/

    /*
     * Simple implementation to make package query work
     * disaggregate the index choice, limit, aka query details, from the operator
     */
    public MemoryRefNode run(List<WherePredicate> wherePredicates,
                             IndexAggregateScan operator){
        return null; // operator.run();
    }

    /**
     * Assumes IN always refers to the first column
     */
    private IKey[] getMultiKeysFromWhereClause(List<WherePredicate> wherePredicates, IMultiVersionIndex index){
        if(!(wherePredicates.get(0).value instanceof int[] intArray)){
            throw new RuntimeException("Do not support IN clause of types other than INT");
        }
        if(wherePredicates.size() == 1){
            SimpleKey[] keysToRet = new SimpleKey[intArray.length];
            int idx = 0;
            for(var key : intArray){
                keysToRet[idx] = SimpleKey.of(key);
                idx++;
            }
            return keysToRet;
        }
        IKey[] keys = new IKey[intArray.length];
        for(int i = 0; i < intArray.length; i++){
            Object[] keyList = new Object[index.indexColumns().length];
            int j = 0;
            for (WherePredicate wherePredicate : wherePredicates) {
                if(j == 0){
                    keyList[j] = intArray[i];
                    j++;
                } else {
                    if (index.containsColumn(wherePredicate.columnReference.columnPosition)) {
                        keyList[j] = wherePredicate.value;
                        j++;
                    }
                }
            }
            keys[i] = KeyUtils.buildRecordKey(keyList);
        }
        return keys;
    }

    private IKey getIndexedKeysFromWhereClause(List<WherePredicate> wherePredicates, IMultiVersionIndex index){
        int i = 0;
        Object[] keyList = new Object[index.indexColumns().length];
        // build index key for only those columns in the selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            if (index.containsColumn(wherePredicate.columnReference.columnPosition)) {
                keyList[i] = wherePredicate.value;
                i++;
            }
        }
        return KeyUtils.buildRecordKey(keyList);
    }

    private List<WherePredicate> getNonIndexedColumnsWhereClause(List<WherePredicate> wherePredicates, IMultiVersionIndex index){
        List<WherePredicate> nonIdxWhereClause = new ArrayList<>();
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then include in the filter
            if (index.containsColumn(wherePredicate.columnReference.columnPosition)) {
                continue;
            }
            nonIdxWhereClause.add(wherePredicate);
        }
        return nonIdxWhereClause;
    }

    private boolean containsNonIndexColumns(List<WherePredicate> wherePredicates, IMultiVersionIndex index){
        for(WherePredicate wherePredicate : wherePredicates){
            if (!index.containsColumn(wherePredicate.columnReference.columnPosition)) { return true; }
        }
        return false;
    }

    public MemoryRefNode run(List<WherePredicate> wherePredicates,
                             IndexScan operator){
        /* COMMENTED FOR NOW
        int i = 0;
        Object[] keyList = new Object[operator.index.columns().length];
        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then build filter
            if(operator.index.containsColumn( wherePredicate.columnReference.columnPosition )){
                keyList[i] = wherePredicate.value;
                i++;
            } else {
                wherePredicatesNoIndex.add(wherePredicate);
            }
        }

         build input
        IKey inputKey = KeyUtils.buildKey( keyList );

        FilterContext filterContext;
        if(!wherePredicatesNoIndex.isEmpty()) {
            filterContext = FilterContextBuilder.build(wherePredicatesNoIndex);
//            return operator.run( table.underlyingPrimaryKeyIndex(), filterContext, inputKey );
            return operator.run( filterContext, inputKey );
        }
        return operator.run(inputKey);
         */
        return null;
    }

    public MemoryRefNode run(Table table,
                             List<WherePredicate> wherePredicates,
                             FullScan operator){
        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
        return null; //operator.run( table.underlyingPrimaryKeyIndex(), filterContext );
    }

    @Override
    public void checkpoint(long maxTid){
        LOGGER.log(DEBUG, "Checkpoint for max TID "+maxTid+" started at "+System.currentTimeMillis());
        for (Table table : this.catalog.values()) {
            // LOGGER.log(DEBUG, "Checkpointing table "+table.getName());
            int numRecords = table.primaryKeyIndex().checkpoint(maxTid);
            if(numRecords > 0) {
                LOGGER.log(DEBUG, numRecords+" record(s) persisted to table "+table.getName());
            } else {
                LOGGER.log(DEBUG, "No records have been flushed to table "+table.getName());
            }
        }
        LOGGER.log(DEBUG, "Checkpoint for max TID "+maxTid+" finished at "+System.currentTimeMillis());
    }

    @Override
    public void cleanup(long maxTid){
        LOGGER.log(DEBUG, "Garbage collection for max TID "+maxTid+" started at "+System.currentTimeMillis());
        for (Table table : this.catalog.values()) {
            table.primaryKeyIndex().cleanup(maxTid);
        }
        LOGGER.log(DEBUG, "Garbage collection for max TID "+maxTid+" finished at "+System.currentTimeMillis());
    }

    /**
     * The idea of commit is to make the effects of the transaction (i.e., operations)
     * materialized in the underlying indexes. The primary index does not need such because
     * it already tracks individual operations on keys through its own cache.
     */
    @Override
    public void commit() {
        TransactionContext txCtx = this.txCtxMap.remove(Thread.currentThread().threadId());
        for(IMultiVersionIndex index : txCtx.indexes){
            index.installWrites(txCtx);
        }
    }

    @Override
    public ITransactionContext beginTransaction(long tid, int identifier, long lastTid, boolean readOnly) {
        return this.txCtxMap.compute(Thread.currentThread().threadId(),
                (_,v) -> {
                    if (v != null && v.tid == 0 && tid == 0)
                        return v;
                    return new TransactionContext(tid, lastTid, readOnly);
                });
    }

    @Override
    public void reset() {
        LOGGER.log(DEBUG, "Reset triggered at "+System.currentTimeMillis());
        for (Table table : this.catalog.values()) {
            LOGGER.log(DEBUG, "Resetting "+table.name);
            table.primaryKeyIndex().reset();
            for(NonUniqueSecondaryIndex secIdx : table.secondaryIndexMap.values()){
                secIdx.reset();
            }
            for(UniqueSecondaryIndex uniqueIdx : table.partialIndexMap.values()){
                uniqueIdx.reset();
            }
        }
        LOGGER.log(DEBUG, "Reset finished at "+System.currentTimeMillis());
    }

    @Override
    public void rebuildIndexes() {
        TransactionContext txCtx = (TransactionContext) beginTransaction(0, 0, 0, false);
        for (Table table : this.catalog.values()) {
            int count = 0;
            Iterator<Object[]> it = table.primaryKeyIndex().iterator(txCtx);
            while ((it.hasNext())) {
                Object[] record = it.next();
                IKey key = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), record);
                table.primaryKeyIndex().doInsert(txCtx, key, record, null);
                for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
                    secIndex.insert(txCtx, key, record);
                }
                count++;
            }
            LOGGER.log(INFO, "Table "+table.getName()+" with "+count+" entries scanned for index rebuilding.");
        }
    }

}
