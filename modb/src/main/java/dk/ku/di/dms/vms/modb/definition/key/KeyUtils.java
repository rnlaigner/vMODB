package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;

public final class KeyUtils {

    private KeyUtils(){}

    public static IKey buildRecordKey(int[] columns, Object... object){

        if(columns.length == 1){
            return SimpleKey.of( object[0] );
        }

        if(columns.length == object.length){
            return CompositeKey.of( object );
        }

        Object[] values = new Object[columns.length];
        for(int i = 0; i < columns.length; i++){
            values[i] = object[columns[i]];
        }

        return CompositeKey.of( values );

    }

    public static IKey buildPrimaryKey(Schema schema, long srcAddress){
        return buildRecordKeyNoHeader(schema, schema.getPrimaryKeyColumns(), srcAddress);
    }

    public static IKey buildPrimaryKey(Schema schema, Object[] object){
        return buildRecordKey(schema.getPrimaryKeyColumns(), object);
    }

    public static IKey buildRecordKeyNoHeader(Schema schema, int[] columns, long srcAddress){

        IKey key;

        // 2 - build the pk
        if(columns.length == 1){
            DataType columnType = schema.columnDataType( columns[0] );
            srcAddress += ( schema.columnOffset()[columns[0]] - Schema.RECORD_HEADER);
            key = SimpleKey.of( DataTypeUtils.getValue(columnType, srcAddress) );
        } else {

            Object[] values = new Object[columns.length];
            long currAddress = srcAddress;

            for(int i = 0; i < columns.length; i++){
                DataType columnType = schema.columnDataType( columns[i] );
                currAddress += (schema.columnOffset()[columns[i]] - Schema.RECORD_HEADER);
                values[i] = DataTypeUtils.getValue(columnType, currAddress);
                // make it default to get the correct offset next iteration
                currAddress = srcAddress;
            }

            key = CompositeKey.of( values );

        }

        return key;

    }

    /**
     * Build a key based on the columns
     * @param schema schema
     * @param columns the columns
     * @param srcAddress the src address
     * @return record key
     */
    public static IKey buildRecordKey(Schema schema, int[] columns, long srcAddress){

        IKey key;

        // 2 - build the pk
        if(columns.length == 1){
            DataType columnType = schema.columnDataType( columns[0] );
            srcAddress += schema.columnOffset()[columns[0]];
            key = SimpleKey.of( DataTypeUtils.getValue(columnType, srcAddress) );
        } else {

            Object[] values = new Object[columns.length];
            long currAddress = srcAddress;

            for(int i = 0; i < columns.length; i++){
                DataType columnType = schema.columnDataType( columns[i] );
                currAddress += schema.columnOffset()[columns[i]];
                values[i] = DataTypeUtils.getValue(columnType, currAddress);
                // make it default to get the correct offset next iteration
                currAddress = srcAddress;
            }

            key = CompositeKey.of( values );

        }

        return key;
    }

    public static IKey buildKey(Object... values){
        if(values.length == 1)
            return SimpleKey.of(values);
        return CompositeKey.of(values);
    }

}
