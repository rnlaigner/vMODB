package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionalHandler;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
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
public final class TransactionManager implements OperationalAPI, ITransactionalHandler {

    private static final ThreadLocal<Set<IMultiVersionIndex>> INDEX_WRITES = ThreadLocal.withInitial( () -> {
        if(!TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly) {
            // TODO buffer of hashsets...
            return new HashSet<>(4); // 4 maximum indexes used per task
        }
        return Collections.emptySet();
    });

    private final Analyzer analyzer;

    private final SimplePlanner planner;

    /**
     * Operators output results
     * They are read-only operations, do not modify data
     */
    private final Map<String, AbstractSimpleOperator> queryPlanCacheMap;

    public TransactionManager(Map<String, Table> catalog){
        this.planner = new SimplePlanner();
        this.analyzer = new Analyzer(catalog);
        this.queryPlanCacheMap = new ConcurrentHashMap<>();
    }

    private boolean fkConstraintViolationFree(Table table, Object[] values){
        for(var entry : table.foreignKeys().entrySet()){
            IKey fk = KeyUtils.buildRecordKey( entry.getValue(), values );
            // have some previous TID deleted it? or simply not exists
            if (!entry.getKey().exists(fk)) return false;
        }
        return true;
    }

    @Override
    public List<Object[]> fetch(Table table, SelectStatement selectStatement){
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
            wherePredicates = this.analyzer.analyzeWhere(
                    table, selectStatement.whereClause);
        }

        if(scanOperator.isIndexScan()){
            IKey key = this.getIndexKeysFromWhereClause(wherePredicates, scanOperator.asIndexScan().index());
            return scanOperator.asIndexScan().runAsEmbedded(new IKey[]{ key });
        } else if(scanOperator.isIndexAggregationScan()){
            return scanOperator.asIndexAggregationScan().runAsEmbedded();
        } else {
            return scanOperator.asFullScan().runAsEmbedded();
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
            wherePredicates = this.analyzer.analyzeWhere(
                    table, selectStatement.whereClause);
        }

        MemoryRefNode memRes;

        // TODO complete for all types or migrate the choice to transaction facade
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
     * TODO finish. can we extract the column values and make a special api for the facade? only if it is a single key
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
            case DELETE -> {

            }
            default -> throw new IllegalStateException("Statement type cannot be identified.");
        }

    }

    /****** ENTITY *******/

    public void insertAll(Table table, List<Object[]> objects){
        // get tid, do all the checks, etc
        for(Object[] entry : objects) {
            this.insert(table, entry);
        }
    }

    public void deleteAll(Table table, List<Object[]> objects) {
        for(Object[] entry : objects) {
            this.delete(table, entry);
        }
    }

    public void updateAll(Table table, List<Object[]> objects) {
        for(Object[] entry : objects) {
            this.update(table, entry);
        }
    }

    /**
     * Not yet considering this record can serve as FK to a record in another table.
     */
    public void delete(Table table, Object[] values) {
        IKey pk = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), values);
        this.deleteByKey(table, pk);
    }

    public void deleteByKey(Table table, Object[] keyValues) {
        IKey pk = KeyUtils.buildIndexKey(keyValues);
        this.deleteByKey(table, pk);
    }

    /**
     * @param table The corresponding table
     * @param pk The primary key
     */
    private void deleteByKey(Table table, IKey pk){
        var opt = table.primaryKeyIndex().removeOpt(pk);
        if(opt.isPresent()){
            INDEX_WRITES.get().add(table.primaryKeyIndex());

            for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
                INDEX_WRITES.get().add(secIndex);
                secIndex.remove(pk, opt.get());
            }

            for(var entry : table.partialIndexMap.entrySet()){
                // does the record "fits" the partial index?
                Tuple<Integer, Object> check = table.partialIndexMetaMap.get( entry.getKey() );
                if (table.primaryKeyIndex().meetPartialIndex(opt.get(), check.t1(), check.t2() )){
                    INDEX_WRITES.get().add(entry.getValue());
                    entry.getValue().remove( pk );
                }
            }

        }
    }

    public boolean exists(PrimaryIndex primaryKeyIndex, Object[] valuesOfKey){
        IKey pk = KeyUtils.buildIndexKey(valuesOfKey);
        return primaryKeyIndex.exists(pk);
    }

    public Object[] lookupByKey(PrimaryIndex index, Object[] valuesOfKey){
        IKey pk = KeyUtils.buildIndexKey(valuesOfKey);
        return index.lookupByKey(pk);
    }

    /**
     * @param table The corresponding database table
     * @param values The fields extracted from the entity
     */
    public void insert(Table table, Object[] values){
        this.doInsert(table, values);
    }

    private Object[] doInsert(Table table, Object[] values) {
        PrimaryIndex primaryIndex = table.primaryKeyIndex();
        if(this.fkConstraintViolationFree(table, values)){
            IKey pk = primaryIndex.insertAndGetKey(values);
            if(pk != null) {
                INDEX_WRITES.get().add(primaryIndex);
                // iterate over secondary indexes to insert the new write
                // this is the delta. records that the underlying index does not know yet
                for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
                    INDEX_WRITES.get().add(secIndex);
                    secIndex.insert(pk, values);
                }
                for(var entry : table.partialIndexMap.entrySet()){
                    // does the record "fits" the partial index?
                    Tuple<Integer, Object> check = table.partialIndexMetaMap.get( entry.getKey() );
                    if (primaryIndex.meetPartialIndex(values, check.t1(), check.t2() )){
                        INDEX_WRITES.get().add(entry.getValue());
                        entry.getValue().insert( pk, values);
                    }
                }
                return values;
            }
        }
        this.undoTransactionWrites();
        throw new RuntimeException("Constraint violation in table "+table.getName());
    }

    public Object[] insertAndGet(Table table, Object[] values){
        return this.doInsert(table, values);
    }

    /**
     * Iterate over all indexes, get the corresponding writes of this tid and remove them
     *      this method can be called in parallel by transaction facade without risk
     */
    public void update(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), values);
        if(this.fkConstraintViolationFree(table, values) && index.update(pk, values)){
            INDEX_WRITES.get().add(index);
            return;
        }
        this.undoTransactionWrites();
        throw new RuntimeException("Constraint violation.");
    }

    /**
     * how can I do that more optimized? creating another interface so secondary indexes also have the #undoTransactionWrites ?
     * INDEX_WRITES can have primary indexes and secondary indexes...
     */
    private void undoTransactionWrites(){
        for(var index : INDEX_WRITES.get()) {
            index.undoTransactionWrites();
        }
    }

    /****** SCAN OPERATORS *******/

    /*
     * Simple implementation to make package query work
     * TODO disaggregate the index choice, limit, aka query details, from the operator
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
//        int i = 0;
//        Object[] keyList = new Object[operator.index.columns().length];
//        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
//        // build filters for only those columns not in selected index
//        for (WherePredicate wherePredicate : wherePredicates) {
//            // not found, then build filter
//            if(operator.index.containsColumn( wherePredicate.columnReference.columnPosition )){
//                keyList[i] = wherePredicate.value;
//                i++;
//            } else {
//                wherePredicatesNoIndex.add(wherePredicate);
//            }
//        }

        // build input
//        IKey inputKey = KeyUtils.buildKey( keyList );
//
//        FilterContext filterContext;
//        if(!wherePredicatesNoIndex.isEmpty()) {
//            filterContext = FilterContextBuilder.build(wherePredicatesNoIndex);
////            return operator.run( table.underlyingPrimaryKeyIndex(), filterContext, inputKey );
//            return operator.run( filterContext, inputKey );
//        }
//        return operator.run(inputKey);
        return null;
    }

    public MemoryRefNode run(Table table,
                             List<WherePredicate> wherePredicates,
                             FullScanWithProjection operator){
        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
        return null; //operator.run( table.underlyingPrimaryKeyIndex(), filterContext );
    }

    /**
     * TODO checkpoint
     * Must log the updates in a separate file. no need for WAL, no need to store before and after
     * Only log those data versions until the corresponding batch.
     * TIDs are not necessarily a sequence.
     */
    @Override
    public void checkpoint(){
        // make state durable
        // get buffered writes in transaction facade and merge in memory
    }

    @Override
    public void commit(){
        var indexes = INDEX_WRITES.get();
        for(var index : indexes){
            index.installWrites();
        }
    }

    @Override
    public void beginTransaction(long tid, int identifier, long lastTid, boolean readOnly) {
        TransactionMetadata.registerTransactionStart(tid, identifier, lastTid, readOnly);
    }

}
