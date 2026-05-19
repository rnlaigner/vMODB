package dk.ku.di.dms.vms.sdk.embed.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.transaction.OperationalAPI;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The interface between the application and internal database operations
 * Responsible to parse objects and convert database results to application objects.
 * A repository class do not need direct access to {@link ITransactionManager}
 */
public abstract class AbstractProxyRepository<PK extends Serializable, T extends IEntity<PK>> extends EntityHandler<PK,T> implements IRepository<PK, T> {

    /**
     * Respective table of the entity
     */
    private final Table table;

    /**
     * Attribute set after database is loaded
     * Not when the metadata is loaded
     * The transaction facade requires DBMS modules (planner, analyzer)
     * along with the catalog. These are not ready on metadata loading time
     */
    private final OperationalAPI operationalAPI;

    private final Map<String, SelectStatement> repositoryQueriesMap;

    public AbstractProxyRepository(Class<PK> pkClazz,
                                  Class<T> entityClazz,
                                  Table table,
                                  OperationalAPI operationalAPI,
                                  // key: method name, value: select stmt
                                  Map<String, SelectStatement> repositoryQueriesMap)
            throws NoSuchFieldException, IllegalAccessException {
        super(pkClazz, entityClazz, table.schema());
        this.table = table;
        this.operationalAPI = operationalAPI;
        this.repositoryQueriesMap = repositoryQueriesMap;
    }

    /**
     * <a href="http://mydailyjava.blogspot.com/2022/02/using-byte-buddy-for-proxy-creation.html">Proxy creation in ByteBuddy</a>
     */
    @SuppressWarnings({"rawtypes, unused"})
    public static final class Interceptor {
        @RuntimeType
        public static Object intercept(
                @This AbstractProxyRepository self,
                @Origin Method method,
                @AllArguments Object[] args) {
            return self.intercept(method, args);
        }
    }

    public final Object intercept(Method method, Object[] args) {
        // retrieve statically defined query
        SelectStatement selectStatement = this.repositoryQueriesMap.get(method.getName());

        // set query arguments
        List<WhereClauseElement> whereClauseElements = new ArrayList<>(args.length);
        for(int i = 0; i < args.length; i++) {
            whereClauseElements.add(selectStatement.whereClause.get(i).overwriteValue( args[i] ));
        }
        // optimization is avoiding copying class, just passing the built where clause as parameter to method "fetch" below
        // submit for execution. could also see if it is possible to project only what is in the select clause
        List<Object[]> records = this.operationalAPI.fetch(this.table, selectStatement, whereClauseElements);

        if (List.class.isAssignableFrom(method.getReturnType())) {
            if(records.isEmpty()) {
                return List.of();
            }
            // this requires a fix. need to know whether the return type specified in the list is an entity
            if(selectStatement.selectClause.get(0).equals("*")) {
                List<T> result = new ArrayList<>(records.size());
                for (Object[] record : records) {
                    result.add(this.parseObjectIntoEntity(record));
                }
                return result;
            } else {
                ParameterizedType paramType = (ParameterizedType) method.getGenericReturnType();
                Class<?> clazz = (Class<?>) paramType.getActualTypeArguments()[0];
                Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
                Field[] fields = clazz.getFields();
                List<?> result = new ArrayList<>(records.size());
                for (Object[] record : records) {
                    result.add(this.buildDtoInstanceFromResult(record, constructor, fields));
                }
                return result;
            }
        }

        if(this.entityConstructor.getDeclaringClass().equals(method.getReturnType())) {
            if(records.isEmpty()) {
                return null;
            }
            if(records.size() == 1) {
                return this.parseObjectIntoEntity(records.get(0));
            }
            throw new IllegalStateException("Should not return more than a single record!");
        }

        if(method.getReturnType().isPrimitive() || Number.class.isAssignableFrom(method.getReturnType())) {
            // String column = selectStatement.selectClause.get(0);
            // this.table.schema().columnPosition(column)
            if(records.isEmpty()) {
                throw new IllegalStateException("Should not return empty record!");
            }
            return records.get(0)[0];
        }

        if(method.getReturnType().isArray()){
            Class<?> componentType = method.getReturnType().getComponentType();
            Object array = Array.newInstance(componentType, records.size());
            if(componentType.isPrimitive()){
                for (int i = 0; i < records.size(); i++) {
                    Array.set(array, i, DataTypeUtils.parseToPrimitive(componentType, records.get(i)[0]));
                }
                return array;
            } else {
                for (int i = 0; i < records.size(); i++) {
                    Array.set(array, i, this.parseObjectIntoEntity(records.get(i)));
                }
            }
        }

        if(!records.isEmpty()) {
            Constructor<?> constructor = method.getReturnType().getDeclaredConstructors()[0];
            Field[] fields = method.getReturnType().getFields();
            return this.buildDtoInstanceFromResult(records.getFirst(), constructor, fields);
        }
        return null;
    }

    @Override
    public final List<T> getAll(){
        List<Object[]> records = this.operationalAPI.getAll(this.table);
        List<T> resultList = new ArrayList<>(records.size());
        for (Object[] record : records){
            resultList.add(this.parseObjectIntoEntity(record));
        }
        return resultList;
    }

    @Override
    public final boolean exists(PK key){
        Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(key);
        return this.operationalAPI.exists(this.table.primaryKeyIndex(), valuesOfKey);
    }

    @Override
    public final T lookupByKey(PK key){
        // this repository is oblivious to multi-versioning
        // given the single-thread model, we can work with writes easily
        // but reads are another story. multiple threads may be calling the repository facade
        // and requiring different data item versions
        // we always need to offload the lookup to the transaction facade
        Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(key);
        Object[] object = this.operationalAPI.lookupByKey(this.table.primaryKeyIndex(), valuesOfKey);
        if (object == null) return null;
        // parse object into entity
        return this.parseObjectIntoEntity(object);
    }

    @Override
    public final List<T> lookupByKeys(Collection<PK> keys){
        List<T> resultList = new ArrayList<>(keys.size());
        for(PK obj : keys){
            Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(obj);
            Object[] object = this.operationalAPI.lookupByKey(this.table.primaryKeyIndex(), valuesOfKey);
            if (object == null) continue;
            resultList.add( this.parseObjectIntoEntity(object) );
        }
        return resultList;
    }

    @Override
    public final void insert(T entity) {
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.insert(this.table, values);
    }

    /**
     * An optimization is just setting the PK into the entity passed as parameter.
     * Another optimization is checking if PK is generated before returning the entity,
     * but this is missing for now. However, this method is probably used when the PK
     * must be retrieved
     */
    @Override
    public final T insertAndGet(T entity){
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        Object[] newValues = this.operationalAPI.insertAndGet(this.table, values);
        for(var entry : this.pkFieldMap.entrySet()){
            int i = this.table.schema().columnPosition(entry.getKey());
            if(newValues[i] == null){
                continue;
            }
            entry.getValue().set( entity, newValues[i] );
        }
        return entity;
    }

    @Override
    public final void insertAll(List<T> entities) {
        // acts as a single transaction, so all constraints, of every single row must be present
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (T entityObject : entities){
            Object[] parsed = this.extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }
        // can only add to cache if all items were inserted since it is transactional
        this.operationalAPI.insertAll(this.table, parsedEntities);
    }

    @Override
    public final void upsert(T entity) {
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.upsert(this.table, values);
    }

    @Override
    public final void update(T entity) {
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.update(this.table, values);
    }

    @Override
    public final void updateAll(List<T> entities) {
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (T entityObject : entities){
            Object[] parsed = this.extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }
        this.operationalAPI.updateAll(this.table, parsedEntities);
    }

    @Override
    public final void delete(T entity){
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.delete(this.table, values);
    }

    @Override
    public final void deleteByKey(PK key) {
        Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(key);
        this.operationalAPI.deleteByKey(this.table, valuesOfKey);
    }

    @Override
    public final void deleteAll(List<T> entities) {
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (T entity : entities){
            parsedEntities.add( this.extractFieldValuesFromEntityObject(entity) );
        }
        this.operationalAPI.deleteAll(this.table, parsedEntities);
    }

    @Override
    @SuppressWarnings({"unhecked", "unchecked"})
    public <DTO> DTO fetchOne(SelectStatement statement, Class<DTO> clazz){
        List<Object[]> objects = this.operationalAPI.fetch(this.table, statement);
        if(objects.isEmpty()){
            return null;
        }
        Object[] object = objects.get(0);
        if(clazz.isPrimitive() || Number.class.isAssignableFrom(clazz)){
            return (DTO) object[0];
        }
        Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
        Field[] fields = clazz.getFields();
        return this.buildDtoInstanceFromResult(object, constructor, fields);
    }

    @Override
    public <DTO> List<DTO> fetchMany(SelectStatement statement, Class<DTO> clazz){
        List<Object[]> objects = this.operationalAPI.fetch(this.table, statement);
        Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
        Field[] fields = clazz.getFields();
        List<DTO> result = new ArrayList<>();
        for(Object[] object : objects){
            DTO dto = this.buildDtoInstanceFromResult(object, constructor, fields);
            result.add(dto);
        }
        return result;
    }

    /**
     * This method assumes that the order of the fields is consistent with the result object
     */
    @SuppressWarnings("unchecked")
    private <DTO> DTO buildDtoInstanceFromResult(Object[] object, Constructor<?> constructor, Field[] fields) {
        try {
            DTO dto = (DTO) constructor.newInstance();
            int i = 0;
            for (Field field : fields) {
                // check if field was captured by query
                if(object[i] != null) {
                    field.set(dto, object[i]);
                }
                i++;
            }
            return dto;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Table getTable() {
        return table;
    }

}

