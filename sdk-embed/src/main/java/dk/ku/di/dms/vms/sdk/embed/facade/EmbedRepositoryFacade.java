package dk.ku.di.dms.vms.sdk.embed.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IDTO;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.definition.Row;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.transaction.OperationAPI;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityUtils;

import java.io.Serializable;
import java.lang.invoke.VarHandle;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * The embed repository facade contains references to DBMS components
 * since the application is co-located with the MODB
 */
public final class EmbedRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private static final Logger LOGGER = Logger.getLogger(EmbedRepositoryFacade.class.getName());

    private final VmsDataSchema schema;

    private final Constructor<? extends IEntity<?>> entityConstructor;

    /**
     * Respective consistent index
     */
    private Table table;

    private final Map<String, Tuple<SelectStatement, Type>> staticQueriesMap;

    private final Map<String, VarHandle> entityFieldMap;

    private final Map<String, VarHandle> pkFieldMap;

    // private final VarHandle pkPrimitive;

    /*
     * Cache of objects in memory.
     * Circular buffer of records (represented as object arrays) for a given index
     * Should be used by the repository facade, since it is the one who is converting the payloads from the user code.
     * Key is the hash code of a table
     */
    // private final CircularBuffer objectCacheStore;

    /**
     * Attribute set after database is loaded
     * Not when the metadata is loaded
     * The transaction facade requires DBMS modules (planner, analyzer)
     * along with the catalog. These are not ready on metadata loading time
     */
    private OperationAPI transactionFacade;

    @SuppressWarnings({"unchecked"})
    public EmbedRepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz,
                                 VmsDataSchema schema,
                                 Map<String, Tuple<SelectStatement, Type>> staticQueriesMap
                                 ) throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException {

        this.schema = schema;

        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();

        Class<? extends IEntity<?>> entityClazz = (Class<? extends IEntity<?>>) types[1];

        Class<? extends Serializable> pkClazz = (Class<? extends Serializable>) types[0];

        this.entityConstructor = entityClazz.getDeclaredConstructor();

        this.entityFieldMap = EntityUtils.getFieldsFromEntity(entityClazz, schema);

        if(pkClazz.getPackageName().equalsIgnoreCase("java.lang") || pkClazz.isPrimitive()){
            this.pkFieldMap = EntityUtils.getFieldFromPk(entityClazz, pkClazz, schema);
        } else {
            this.pkFieldMap = EntityUtils.getFieldsFromCompositePk(pkClazz);
        }

        this.staticQueriesMap = staticQueriesMap;

        // this.objectCacheStore = new CircularBuffer(schema.columnNames.length);

    }

    /**
     * Everything that is defined on runtime related to DBMS components
     * For now, table and transaction facade
     */
    public void setDynamicDatabaseModules(TransactionFacade transactionFacade, Table table){
        this.transactionFacade = transactionFacade;
        this.table = table;
    }

    /**
     * The actual facade for database operations called by the application-level code.
     * @param proxy the virtual microservice caller method (a subtransaction)
     * @param method the repository method called
     * @param args the function call parameters
     * @return A DTO (i.e., any class where attribute values are final), a row {@link Row}, or set of rows
     */
    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) {

        String methodName = method.getName();

        switch (methodName) {
            case "lookupByKey" -> {

                // this repository is oblivious to multi-versioning
                // given the single-thread model, we can work with writes easily
                // but reads are another story. multiple threads may be calling the repository facade
                // and requiring different data item versions
                // we always need to offload the lookup to the transaction facade
                Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(args[0]);
                Object[] object = this.transactionFacade.lookupByKey(this.table.primaryKeyIndex(), valuesOfKey);

                // parse object into entity
                if (object != null)
                    return this.parseObjectIntoEntity(object);
                return null;
            }
            case "lookupByKeys" -> {
                List<Object> castedList = (List<Object>) args[0];
                List<IEntity<?>> resultList = new ArrayList<>(castedList.size());
                for(Object obj : castedList){
                    Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(obj);
                    Object[] object = this.transactionFacade.lookupByKey(this.table.primaryKeyIndex(), valuesOfKey);
                    resultList.add(this.parseObjectIntoEntity(object));
                }
                return resultList;
            }
            case "deleteByKey" -> {
                Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(args[0]);
                this.transactionFacade.deleteByKey(this.table, valuesOfKey);
            }
            case "delete" -> {
                Object[] values = this.extractFieldValuesFromEntityObject(args[0]);
                this.transactionFacade.delete(this.table, values);
            }
            case "deleteAll" -> this.deleteAll((List<Object>) args[0]);
            case "update" -> {
                Object[] values = extractFieldValuesFromEntityObject(args[0]);
                this.transactionFacade.update(this.table, values);
            }
            case "updateAll" -> this.updateAll((List<Object>) args[0]);
            case "insert" -> {
                Object[] values = extractFieldValuesFromEntityObject(args[0]);
                this.transactionFacade.insert(this.table, values);
            }
            case "insertAndGet" -> {
                // cache the entity
                Object cached = args[0];
                Object[] values = this.extractFieldValuesFromEntityObject(args[0]);
                Object valuesWithKey = this.transactionFacade.insertAndGet(this.table, values);
                // this.setKeyValueOnObject( key_, cached );
                return cached;
            }
            case "insertAll" -> this.insertAll((List<Object>) args[0]);
            case "fetch" -> {
                // dispatch to analyzer passing the clazz param
                // always select because of the repository API
                SelectStatement selectStatement = ((IStatement) args[0]).asSelectStatement();
                return fetch(selectStatement, (Type) args[1]);
            }
            case "issue" -> {
                try {
                    // no return by default
                    this.transactionFacade.issue(this.table, (IStatement) args[0]);
                } catch (AnalyzerException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> {

                // check if is it static query
                Tuple<SelectStatement, Type> selectStatement = this.staticQueriesMap.get(methodName);

                if (selectStatement == null)
                    throw new IllegalStateException("Unknown repository operation.");

                return fetch(selectStatement.t1(), selectStatement.t2());

            }
        }

        return null;
    }

    private IEntity<?> parseObjectIntoEntity(Object[] object){
        // all entities must have default constructor
        try {
            IEntity<?> entity = this.entityConstructor.newInstance();
            int i;
            for(var entry : this.entityFieldMap.entrySet()){
                // must get the index of the column first
                i = this.table.underlyingPrimaryKeyIndex_().schema().columnPosition(entry.getKey());
                entry.getValue().set( entity, object[i] );
            }
            return entity;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Must be a linked sorted map. Ordered by the columns that appear on the key object.
     */
    private Object[] extractFieldValuesFromKeyObject(Object keyObject) {

        Object[] values = new Object[this.pkFieldMap.size()];

        if(keyObject instanceof Number){
            values[0] = keyObject;
            return values;
        }

        int fieldIdx = 0;
        // get values from key object
        for(String columnName : this.pkFieldMap.keySet()){
            values[fieldIdx] = this.pkFieldMap.get(columnName).get(keyObject);
            fieldIdx++;
        }
        return values;
    }

//    private void setKeyValueOnObject( Object key, Object object ){
//        pkPrimitive.set(object, key);
//    }

    @Override
    public Object[] extractFieldValuesFromEntityObject(Object entityObject) {

        Object[] values = new Object[this.schema.columnNames.length];
        // TODO objectCacheStore.peek()

        int fieldIdx = 0;
        // get values from entity
        for(String columnName : this.schema.columnNames){
            values[fieldIdx] = this.entityFieldMap.get(columnName).get(entityObject);
            fieldIdx++;
        }
        return values;
    }

    // TODO FINISH
//    private Object[] getObjectFromCacheStore(Table table){
//        this.objectCacheStore.get(table) != null
//    }

    @Override
    public Object fetch(SelectStatement selectStatement, Type type) {

        // we need some context about the results in this memory space
        // number of records
        // schema of the return (maybe not if it is a dto)
        var memRes = this.transactionFacade.fetch(this.table, selectStatement);

        // TODO parse output into object
        if(type == IDTO.class) {
            // look in the map of dto types for the setter and getter
            return null;
        }

        // then it is a primitive, just return the value
//        int projectionColumnIndex = scanOperator.asScan().projectionColumns[0];
//        DataType dataType = scanOperator.asScan().index.schema().getColumnDataType(projectionColumnIndex);
//        return DataTypeUtils.getValue(dataType, memRes.address());
        return null;
    }

    @Override
    public void insertAll(List<Object> entities) {

        // acts as a single transaction, so all constraints, of every single row must be present
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (Object entityObject : entities){
            Object[] parsed = extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }

        // can only add to cache if all items were inserted since it is transactional
        this.transactionFacade.insertAll( this.table, parsedEntities );

    }

    private void updateAll(List<Object> entities) {
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (Object entityObject : entities){
            Object[] parsed = extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }
        this.transactionFacade.updateAll(this.table, parsedEntities);
    }

    public void deleteAll(List<Object> entities) {
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (Object entityObject : entities){
            Object[] parsed = extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }
        this.transactionFacade.deleteAll( this.table, parsedEntities );
    }

    @Override
    public InvocationHandler asInvocationHandler() {
        return this;
    }
}

