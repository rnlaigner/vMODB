package dk.ku.di.dms.vms.modb.transaction.api;

import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;

import java.util.List;

/**
 * Set of internal operations performed in the private state
 * It abstracts the possible calls {@link dk.ku.di.dms.vms.modb.api.interfaces.IRepository}
 * classes perform to the {@link TransactionFacade}.
 */
public interface OperationAPI {

    void deleteByKey(Table table, Object[] valuesOfKey);

    Object[] lookupByKey(PrimaryIndex primaryKeyIndex, Object[] valuesOfKey);

    void delete(Table table, Object[] values);

    void update(Table table, Object[] values);

    void insert(Table table, Object[] values);

    Object insertAndGet(Table table, Object[] values);

    void issue(Table table, IStatement arg) throws AnalyzerException;

    MemoryRefNode fetch(PrimaryIndex primaryKeyIndex, SelectStatement selectStatement);

    void updateAll(Table table, List<Object[]> parsedEntities);

    void deleteAll(Table table, List<Object[]> parsedEntities);

    void insertAll(Table table, List<Object[]> parsedEntities);

}
