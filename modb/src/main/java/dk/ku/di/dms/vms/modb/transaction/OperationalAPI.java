package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;

import java.util.List;

/**
 * Set of internal operations performed in the private state
 * It abstracts the possible calls that {@link dk.ku.di.dms.vms.modb.api.interfaces.IRepository}
 * classes make to the {@link TransactionManager}.
 */
public sealed interface OperationalAPI permits TransactionManager {

    void deleteByKey(Table table, Object[] valuesOfKey);

    boolean exists(PrimaryIndex primaryKeyIndex, Object[] valuesOfKey);

    Object[] lookupByKey(PrimaryIndex primaryKeyIndex, Object[] valuesOfKey);

    void delete(Table table, Object[] values);

    void upsert(Table table, Object[] values);

    void update(Table table, Object[] values);

    void insert(Table table, Object[] values);

    Object[] insertAndGet(Table table, Object[] values);

    void issue(Table table, IStatement arg) throws AnalyzerException;

    MemoryRefNode fetchMemoryReference(Table table, SelectStatement selectStatement);

    List<Object[]> fetch(Table table, SelectStatement selectStatement);

    List<Object[]> fetch(Table table, SelectStatement selectStatement, List<WhereClauseElement> whereClauseElements);

    void updateAll(Table table, List<Object[]> parsedEntities);

    void deleteAll(Table table, List<Object[]> parsedEntities);

    void insertAll(Table table, List<Object[]> parsedEntities);

    List<Object[]> getAll(Table table);

}
