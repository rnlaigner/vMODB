package dk.ku.di.dms.vms.modb.common.interfaces;

import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;

import java.io.Serializable;
import java.util.List;

/**
 * User-facing database operations
 * @param <PK> The primary key of the entity
 * @param <T> The entity type
 */
public interface IRepository<PK extends Serializable,T extends IEntity<PK>> {

    void insert(T object);
    T insertAndGet(T object);
    void insertAll(List<T> entities);
    List<T> insertAllAndGet(List<T> entities);

    T update(T object);
    T updateAll(List<T> entities);

    T delete(T object);
    T deleteAll(List<T> entities);
    T deleteByKey(PK key);
    T deleteAllByKey(List<PK> keys);

    T lookupByKey(PK key);

    /**
     * Used for issuing update, insert, and delete statements.
     * It does not return any value because
     * any error is handled by the system itself
      */
    void issue(IStatement statement);

    /**
     *  Used for retrieving a single row.
     *  Can't be tied to {@link Record} because the return can be a primitive type.
     */
    <DTO> DTO fetchOne(IStatement statement, Class<DTO> clazz);

    <DTO> IVmsFuture<DTO> fetchOnePromise(IStatement statement, Class<DTO> clazz);

    /**
     * Used for retrieving multiple rows.
     * Different method given strong typed system
     * Return type no concept of rows
     */
    <DTO> List<DTO> fetchMany(IStatement statement, Class<DTO> clazz);

    // maybe callbacks can be a good alternative for embedding statements inside repository
    // <DTO extends Record> List<DTO>  create(Consumer<UpdateStatementBuilder.SetClause> callback);

}