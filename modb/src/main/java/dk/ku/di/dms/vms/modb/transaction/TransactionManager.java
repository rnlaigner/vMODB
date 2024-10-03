package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionContext;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.execution.operators.min.IndexGroupByMinWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.SimplePlanner;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    private final boolean checkpointing;

    public TransactionManager(Map<String, Table> catalog, boolean checkpointing){
        this.planner = new SimplePlanner();
        this.analyzer = new Analyzer(catalog);
        this.catalog = catalog;
        this.queryPlanCacheMap = new ConcurrentHashMap<>();
        this.checkpointing = checkpointing;
        this.txCtxMap = new ConcurrentHashMap<>();
    }

    private boolean fkConstraintViolation(TransactionContext txCtx, Table table, Object[] values){
        for(Map.Entry<PrimaryIndex, int[]> entry : table.foreignKeys().entrySet()){
            IKey fk = KeyUtils.buildRecordKey( entry.getValue(), values );
            // have some previous TID deleted it? or simply not exists
            if (!entry.getKey().exists(txCtx, fk))
                return true;
        }
        return false;
    }

    @Override
    public List<Object[]> fetch(final Table table, final SelectStatement selectStatement){
        String sqlAsKey = selectStatement.SQL.toString();
        AbstractSimpleOperator scanOperator = this.queryPlanCacheMap.computeIfAbsent(sqlAsKey,
                (ignored) -> {
                    QueryTree queryTree = this.analyzer.analyze(selectStatement);
                    return this.planner.plan(queryTree);
                });
        List<WherePredicate> wherePredicates = this.analyzer.analyzeWhere(table, selectStatement.whereClause);
        if(scanOperator.isIndexScan()){
            IKey key = this.getIndexKeysFromWhereClause(wherePredicates, scanOperator.asIndexScan().index());
            return scanOperator.asIndexScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key);
        } else if(scanOperator.isIndexAggregationScan()){
            return scanOperator.asIndexAggregationScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()));
        } else if(scanOperator.isIndexMultiAggregationScan()){
            IKey key = this.getIndexKeysFromWhereClause(wherePredicates, scanOperator.asIndexMultiAggregationScan().index());
            return scanOperator.asIndexMultiAggregationScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key);
        } else {
            // future optimization is filter not including the columns of partial or non-unique index
            FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
            return scanOperator.asFullScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), filterContext);
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
        txCtx.indexes.add(primaryIndex);
        // iterate over secondary indexes to insert the new write
        // this is the delta. records that the underlying index does not know yet
        for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
            txCtx.indexes.add(secIndex);
            secIndex.insert(txCtx, pk, values);
        }
        for(var entry : table.partialIndexMap.entrySet()){
            // does the record "fits" the partial index?
            Tuple<Integer, Object> check = table.partialIndexMetaMap.get( entry.getKey() );
            if (primaryIndex.meetPartialIndex(values, check.t1(), check.t2() )){
                txCtx.indexes.add(entry.getValue());
                entry.getValue().insert(txCtx, pk, values);
            }
        }
        return values;
    }

    @Override
    public Object[] insertAndGet(Table table, Object[] values){
        return this.doInsert(this.txCtxMap.get(Thread.currentThread().threadId()), table, values);
    }

    @Override
    public void upsert(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), values);
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        if(index.upsert(txCtx, pk, values)) {
            txCtx.indexes.add(index);
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
                             IndexGroupByMinWithProjection operator){
        return null; // operator.run();
    }

    private IKey getIndexKeysFromWhereClause(List<WherePredicate> wherePredicates, IMultiVersionIndex index){
        int i = 0;
        Object[] keyList = new Object[index.indexColumns().length];
        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then build filter
            if (index.containsColumn(wherePredicate.columnReference.columnPosition)) {
                keyList[i] = wherePredicate.value;
                i++;
            }
        }
        return KeyUtils.buildIndexKey(keyList);
    }

    public MemoryRefNode run(List<WherePredicate> wherePredicates,
                             IndexScanWithProjection operator){
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
                             FullScanWithProjection operator){
        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
        return null; //operator.run( table.underlyingPrimaryKeyIndex(), filterContext );
    }

    /**
     * Must log the updates in a separate file. no need for WAL, no need to store before and after
     * Only log those data versions until the corresponding batch.
     * TIDs are not necessarily a sequence.
     */
    @Override
    public void checkpoint(long maxTid){
        LOGGER.log(INFO, "Checkpoint for max TID "+maxTid+" started at "+System.currentTimeMillis());
        if(this.checkpointing) {
            for (Table table : this.catalog.values()) {
                LOGGER.log(INFO, "Checkpointing table "+table.getName());
                table.primaryKeyIndex().checkpoint(maxTid);
            }
        } else {
            LOGGER.log(INFO, "Checkpoint disabled. Starting only garbage collection for max TID "+maxTid);
            for (Table table : this.catalog.values()) {
                table.primaryKeyIndex().garbageCollection(maxTid);
            }
        }
        LOGGER.log(INFO, "Checkpoint for max TID "+maxTid+" finished at "+System.currentTimeMillis());
    }

    /**
     * The idea of commit is to make the effects of the transaction (i.e., operations)
     * materialized in the underlying indexes. The primary index does not need such because
     * it already tracks individual operations on keys through its own cache.
     */
    @Override
    public void commit(){
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        for(var index : txCtx.indexes){
            index.installWrites(txCtx);
        }
    }

    @Override
    public ITransactionContext beginTransaction(long tid, int identifier, long lastTid, boolean readOnly) {
        return this.txCtxMap.compute(Thread.currentThread().threadId(),
                (ignored, v) -> {
                    if (v != null && tid == 0 && v.tid == 0)
                        return v;
                    return new TransactionContext(tid, lastTid, readOnly);
                });
    }

    @Override
    public void reset() {
        LOGGER.log(INFO, "Reset triggered at "+System.currentTimeMillis());
        for (Table table : this.catalog.values()) {
            LOGGER.log(INFO, "Resetting "+table.name);
            table.primaryKeyIndex().reset();
            for(var secIdx : table.secondaryIndexMap.values()){
                secIdx.reset();
            }
            for(var partialIdx : table.partialIndexMap.values()){
                partialIdx.reset();
            }
        }
        LOGGER.log(INFO, "Reset finished at "+System.currentTimeMillis());
        LOGGER.log(INFO, "GC triggered.");
        System.gc();
        LOGGER.log(INFO, "GC finished.");
    }

}
