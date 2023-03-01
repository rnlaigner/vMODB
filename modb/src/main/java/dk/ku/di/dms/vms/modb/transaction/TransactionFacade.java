package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.modb.common.transaction.WriteType;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.SimplePlanner;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.transaction.api.TransactionManagerAPI;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A transaction management facade, i.e., a transaction manager.
 * Encapsulates a set of concurrency-control features through a set of interfaces,
 * in the direction of the <a href="https://en.wikipedia.org/wiki/Facade_pattern">facade</a> pattern.
 * Responsibilities:
 * - Keep track of modifications
 * - Commit (write to the actual corresponding regions of memories)
 * AbstractIndex must be modified so reads can return the correct (versioned/consistent) value
 * Repository facade parses the request. Transaction facade deals with low-level operations
 * Batch-commit aware. That means when a batch comes, must make data durable.
 * In order to accommodate two or more VMSs in the same JVM instance,
 *  it would need to make this class non-static and put it into modb modules.
 */
public final class TransactionFacade implements TransactionManagerAPI {

    private final Analyzer analyzer;

    private final SimplePlanner planner;

    /**
     * Operators output results
     * They are read-only operations, do not modify data
     */
    private final Map<String, AbstractSimpleOperator> readQueryPlans;

    private final Map<String, Table> schema;

    // in the future, specialize this for only foreign key (tracking only the PK) or the whole record
    private final Set<String> subscriptedTables;

    private final Map<String, PrimaryIndex> replicatedTables;

    private TransactionFacade(Map<String, Table> schema, Map<String, PrimaryIndex> replicatedTables, SimplePlanner planner, Analyzer analyzer){
        this.schema = schema;
        this.replicatedTables = replicatedTables;
        this.subscriptedTables = new CopyOnWriteArraySet<>();
        this.planner = planner;
        this.analyzer = analyzer;
        // read-only transactions may put items here
        this.readQueryPlans = new ConcurrentHashMap<>();
    }

    /**
     * TransactionFacade should not create any new object
     */
    public static TransactionFacade build(Map<String, Table> schema, Map<String, PrimaryIndex> replicatedTables, SimplePlanner planner, Analyzer analyzer){
        return new TransactionFacade(schema, replicatedTables, planner, analyzer);
    }

    private boolean fkConstraintHolds(Table table, Object[] values){
        boolean a = table.foreignKeys().isEmpty();
        boolean b = table.externalForeignKeys().isEmpty();
        if(a && b) return true;
        IKey fk;
        if(a) {
            for (var entry : table.foreignKeys().entrySet()) {
                fk = KeyUtils.buildRecordKey(entry.getValue(), values);
                // have some previous TID deleted it? or simply not exists
                if (!entry.getKey().exists(fk)) return false;
            }
        }
        if(b) {
            // now check the external tables
            for (var entry : table.externalForeignKeys().entrySet()) {
                fk = KeyUtils.buildRecordKey(entry.getValue(), values);
                if (!entry.getKey().exists(fk)) return false;
            }
        }
        return true;
    }

    /**
     * Best guess return type. Differently from the parameter type received.
     * @param selectStatement a select statement
     * @return the query result in a memory space
     */
    public MemoryRefNode fetch(PrimaryIndex consistentIndex, SelectStatement selectStatement) {

        String sqlAsKey = selectStatement.SQL.toString();

        AbstractSimpleOperator scanOperator = this.readQueryPlans.get( sqlAsKey );

        List<WherePredicate> wherePredicates;

        if(scanOperator == null){
            QueryTree queryTree;
            try {
                queryTree = this.analyzer.analyze(selectStatement);
                wherePredicates = queryTree.wherePredicates;
                scanOperator = this.planner.plan(queryTree);
                this.readQueryPlans.put(sqlAsKey, scanOperator );
            } catch (AnalyzerException ignored) { return null; }

        } else {
            // get only the where clause params
            try {
                wherePredicates = this.analyzer.analyzeWhere(
                        scanOperator.asScan().table, selectStatement.whereClause);
            } catch (AnalyzerException ignored) { return null; }
        }

        MemoryRefNode memRes = null;

        // TODO complete for all types or migrate the choice to transaction facade
        //  make an enum, it is easier
        if(scanOperator.isIndexScan()){
            // build keys and filters
            //memRes = OperatorExecution.run( wherePredicates, scanOperator.asIndexScan() );
            memRes = this.run(consistentIndex, wherePredicates, scanOperator.asIndexScan());
        } else {
            // build only filters
            memRes = this.run( consistentIndex, wherePredicates, scanOperator.asFullScan() );
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

                this.planner.getOptimalIndex(table, wherePredicates);

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

    /**
     * It installs the writes without taking into consideration concurrency control.
     * Used when the buffer received is aligned with how the data is stored in memory
     */
    public void bulkInsert(Table table, ByteBuffer buffer, int numberOfRecords){
        // if the memory address is occupied, must log warning,
        // so we can increase the table size
        long address = MemoryUtils.getByteBufferAddress(buffer);

        // if the memory address is occupied, must log warning,
        // so we can increase the table size
        ReadWriteIndex<IKey> index = table.primaryKeyIndex().underlyingIndex();
        int sizeWithoutHeader = table.schema.getRecordSizeWithoutHeader();
        long currAddress = address;

        IKey key;
        for (int i = 0; i < numberOfRecords; i++) {
            key = KeyUtils.buildPrimaryKey(table.schema, currAddress);
            index.insert( key, currAddress );
            currAddress += sizeWithoutHeader;
        }

    }

    /****** ENTITY *******/

    // possible optimization is checking the foreign
    // key of all records first and then later insert
    public void insertAll(Table table, List<Object[]> objects){
        if(this.subscriptedTables.contains(table.getName())){
            for(Object[] entry : objects) {
                this.insert(table, entry);
                this.trackUpdate(table.getName(), entry, WriteType.INSERT);
            }
            return;
        }
        // get tid, do all the checks, etc
        for(Object[] entry : objects) {
            this.insert(table, entry);
        }
    }

    public void deleteAll(Table table, List<Object[]> objects) {
        if(this.subscriptedTables.contains(table.getName())){
            for(Object[] entry : objects) {
                this.delete(table, entry);
                trackUpdate(table.getName(), entry, WriteType.DELETE);
            }
            return;
        }
        for(Object[] entry : objects) {
            this.delete(table, entry);
        }
    }

    public void updateAll(Table table, List<Object[]> objects) {
        if(this.subscriptedTables.contains(table.getName())){
            for(Object[] entry : objects) {
                this.update(table, entry);
                trackUpdate(table.getName(), entry, WriteType.UPDATE);
            }
            return;
        }
        for(Object[] entry : objects) {
            this.update(table, entry);
        }
    }

    /**
     * Not yet considering this record can serve as FK to a record in another table.
     */
    public void delete(Table table, Object[] record) {
        if(this.subscriptedTables.contains(table.getName())){
            Object[] keyValues = new Object[record.length];
            int[] pkColumns = table.schema.getPrimaryKeyColumns();
            for(int i = 0; i < pkColumns.length; i++){
                keyValues[i] = record[pkColumns[i]];
            }
            IKey pk = KeyUtils.buildKey(keyValues);
            if(table.primaryKeyIndex().delete(pk)){
                trackUpdate(table.getName(), keyValues, WriteType.DELETE);
            }
            return;
        }
        IKey pk = KeyUtils.buildPrimaryKey(table.schema, record);
        table.primaryKeyIndex().delete(pk);
    }

    public void deleteByKey(Table table, Object[] keyValues) {
        IKey pk = KeyUtils.buildKey(keyValues);
        boolean res = table.primaryKeyIndex().delete(pk);
        if(res && this.subscriptedTables.contains(table.getName())){
            this.trackUpdate(table.getName(), keyValues, WriteType.DELETE);
        }
    }

    private void trackUpdate(String tableName, Object[] values, WriteType writeType) {
        TransactionMetadata.TRANSACTION_CONTEXT.get().writes
                .computeIfAbsent(tableName, (k) -> new ArrayList<>() )
                .add(new TransactionWrite(writeType, values));
    }

    public Object[] lookupByKey(PrimaryIndex index, Object... valuesOfKey){
        IKey pk = KeyUtils.buildKey(valuesOfKey);
        return index.lookupByKey(pk);
    }

    /**
     * @param table The corresponding database table
     * @param values The fields extracted from the entity
     */
    public void insert(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.schema().getPrimaryKeyColumns(), values);
        if(this.fkConstraintHolds(table, values) && index.insert(pk, values)){
            // iterate over secondary indexes to insert the new write
            // this is the delta. records that the underlying index does not know yet
            for(NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()){
                secIndex.appendDelta( pk, values );
            }
            if(this.subscriptedTables.contains(table.getName()))
                this.trackUpdate(table.getName(), values, WriteType.INSERT);
            return;
        }
        this.undoTransactionWrites();
        throw new RuntimeException("Constraint violation.");
    }

    public Object insertAndGet(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        if(this.fkConstraintHolds(table, values)){
            Tuple<Object, IKey> res = index.insertAndGet(values);
            if(res != null) {
                for(NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()){
                    secIndex.appendDelta( res.t2(), values );
                }
                if(this.subscriptedTables.contains(table.getName()))
                    this.trackUpdate(table.getName(), values, WriteType.INSERT);
                return res.t1();
            }
        }
        this.undoTransactionWrites();
        throw new RuntimeException("Constraint violation.");
    }

    /**
     * Iterate over all indexes, get the corresponding writes of this tid and remove them
     *      this method can be called in parallel by transaction facade without risk
     */
    public void update(Table table, Object[] values){
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.schema().getPrimaryKeyColumns(), values);
        if(this.fkConstraintHolds(table, values) && index.update(pk, values)){
            if(this.subscriptedTables.contains(table.getName()))
                this.trackUpdate(table.getName(), values, WriteType.UPDATE);
            return;
        }
        this.undoTransactionWrites();
        throw new RuntimeException("Constraint violation.");
    }

    /**
     * As single-threaded model, assuming this will almost never happen so overhead of building gain the pk is minor
     * Can also perform this in parallel without compromising safety
     * TODO Must unmark secondary index records as deleted...
     * how can I do that more optimized? creating another interface so secondary indexes also have the #undoTransactionWrites ?
     * INDEX_WRITES can have primary indexes and secondary indexes...
     */
    private void undoTransactionWrites(){
        Map<String, List<TransactionWrite>> writes = TransactionMetadata.TRANSACTION_CONTEXT.get().writes;
        Table table;
        IKey key;
        for(var entry : writes.entrySet()){
            table = this.schema.get(entry.getKey());
            for(var write : entry.getValue()){
                if (write.type == WriteType.DELETE) {
                    key = KeyUtils.buildKey(write.record);
                } else {
                    key = KeyUtils.buildPrimaryKey(table.getSchema(), write.record);
                }
                table.primaryKeyIndex().undoTransactionWrite(key);
            }
        }
    }

    /****** SCAN OPERATORS *******/

    public MemoryRefNode run(PrimaryIndex consistentIndex,
                             List<WherePredicate> wherePredicates,
                             IndexScanWithProjection operator){

        List<Object> keyList = new ArrayList<>(operator.index.columns().length);
        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then build filter
            if(operator.index.containsColumn( wherePredicate.columnReference.columnPosition )){
                keyList.add( wherePredicate.value );
            } else {
                wherePredicatesNoIndex.add(wherePredicate);
            }
        }

        FilterContext filterContext = FilterContextBuilder.build(wherePredicatesNoIndex);
        // build input
        IKey inputKey = KeyUtils.buildKey(keyList.toArray());
        return operator.run( consistentIndex, filterContext, inputKey );
    }

    public MemoryRefNode run(PrimaryIndex consistentIndex,
                             List<WherePredicate> wherePredicates,
                             FullScanWithProjection operator){
        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
        return operator.run( consistentIndex, filterContext );
    }

    /****** WRITE OPERATORS *******/



    /**
     * CHECKPOINTING
     * Only log those data versions until the corresponding batch.
     * TIDs are not necessarily a sequence.
     */
    public void checkpoint(){

        // make state durable
        // get buffered writes in transaction facade and merge in memory

        for(Table table : this.schema.values()){
            table.primaryKeyIndex().installWrites();
            // log index since all updates are made
            // index.asUniqueHashIndex().buffer().log();
        }

        // TODO must modify corresponding secondary indexes too
        //  must log the updates in a separate file. no need for WAL, no need to store before and after

    }

    /**
     * REPLICATION
     * ==================================
     * A subscription contains the information
     * necessary to forward updates to other nodes.
     * Subscriptions can be dynamically modified over
     * the runtime.
     */
    @Override
    public boolean addSubscription(String table) {
        return this.subscriptedTables.add(table);
    }

    @Override
    public boolean removeSubscription(String table) {
        return this.subscriptedTables.remove(table);
    }

    @Override
    public void applyUpdates(Map<String, List<TransactionWrite>> updates){

    }

}
