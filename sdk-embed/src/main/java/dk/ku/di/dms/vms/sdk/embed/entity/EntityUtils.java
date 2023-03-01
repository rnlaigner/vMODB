package dk.ku.di.dms.vms.sdk.embed.entity;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsTableSchema;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public final class EntityUtils {

    private EntityUtils(){}

    private static final MethodHandles.Lookup lookup;
    static {
        lookup = MethodHandles.lookup();
    }

    public static Map<String, VarHandle> getFieldsFromPk(Class<?> pkClazz) throws NoSuchFieldException, IllegalAccessException {

        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(pkClazz, lookup);

        Field[] fields = pkClazz.getDeclaredFields();
        Map<String, VarHandle> fieldMap = new HashMap<>(  );

        for(Field field : fields){
            fieldMap.put(
                    field.getName(),
                    lookup_.findVarHandle(
                            pkClazz,
                            field.getName(),
                            field.getType()
                    )
            );
        }
        return fieldMap;
    }

    public static VarHandle[] getFieldsOfPk(Class<?> pkClazz, VmsTableSchema dataSchema) throws IllegalAccessException {
        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(pkClazz, lookup);
        VarHandle[] handles = new VarHandle[dataSchema.primaryKeyColumns.length];
        int idx = 0;
        for(int columnPk : dataSchema.primaryKeyColumns){
            String pkColumnName = dataSchema.columnNames[columnPk];
            try {
                Field field = pkClazz.getField(pkColumnName);
                handles[idx] = lookup_.findVarHandle(
                        pkClazz,
                        field.getName(),
                        field.getType()
                );
                idx++;
            } catch (NoSuchFieldException ex){
                throw new IllegalStateException("Cannot find var handle of primitive primary key. Table:"+dataSchema.tableName+"; Column: "+pkColumnName);
            }
        }
        return handles;
    }

    public static VarHandle getPrimitiveFieldOfPk(Class<?> pkClazz, VmsTableSchema dataSchema) throws IllegalAccessException {
        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(pkClazz, lookup);
        // usually the first, but to make sure lets do like this
        int pkColumn = dataSchema.primaryKeyColumns[0];
        String pkColumnName = dataSchema.columnNames[pkColumn];
        try {
            Field field = pkClazz.getField(pkColumnName);
            return lookup_.findVarHandle(
                    pkClazz,
                    field.getName(),
                    field.getType()
            );
        } catch (NoSuchFieldException ex){
            throw new IllegalStateException("Cannot find var handle of primitive primary key. Table:"+dataSchema.tableName+"; Column: "+pkColumnName);
        }
    }

    public static Map<String, VarHandle> getFieldsFromEntity(Class<? extends IEntity<?>> entityClazz,
                                                             VmsTableSchema schema) throws NoSuchFieldException, IllegalAccessException {

        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(entityClazz, lookup);

        Map<String, VarHandle> fieldMap = new HashMap<>(schema.columnNames.length);
        int i = 0;
        for(String columnName : schema.columnNames){
            fieldMap.put(
                    columnName,
                    lookup_.findVarHandle(
                            entityClazz,
                            columnName,
                            DataTypeUtils.getJavaTypeFromDataType(schema.columnDataTypes[i])
                    )
            );
            i++;
        }

        return fieldMap;

    }

}
