package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.composite.NCompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.composite.PairCompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.composite.QuadrupleCompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.composite.TripleCompositeKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

import java.util.Arrays;
import java.util.Comparator;

public final class KeyUtils {

    private KeyUtils(){}

    @SuppressWarnings("unchecked")
    public static Comparator<Object[]> createComparator(int length) {
        return switch (length) {
            case 1 -> (a, b) -> compare((Comparable<Object>) a[0], b[0]);
            case 2 -> (a, b) -> {
                int c0 = compare((Comparable<Object>) a[0], b[0]);
                if (c0 != 0) return c0;
                return compare((Comparable<Object>) a[1], b[1]);
            };
            case 3 -> (a, b) -> {
                int c0 = compare((Comparable<Object>) a[0], b[0]);
                if (c0 != 0) return c0;

                int c1 = compare((Comparable<Object>) a[1], b[1]);
                if (c1 != 0) return c1;

                return compare((Comparable<Object>) a[2], b[2]);
            };
            case 4 -> (a, b) -> {
                int c0 = compare((Comparable<Object>) a[0], b[0]);
                if (c0 != 0) return c0;

                int c1 = compare((Comparable<Object>) a[1], b[1]);
                if (c1 != 0) return c1;

                int c2 = compare((Comparable<Object>) a[2], b[2]);
                if (c2 != 0) return c2;

                return compare((Comparable<Object>) a[3], b[3]);
            };
            default -> (a, b) -> {
                for (int i = 0; i < length; i++) {
                    int cmp = compare((Comparable<Object>) a[i], b[i]);
                    if (cmp != 0) return cmp;
                }
                return 0;
            };
        };
    }

    private static int compare(Comparable<Object> a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return a.compareTo(b);
    }

    public static IKey buildRecordKey(Object[] object){
        switch (object.length){
            case 1 -> {
                return SimpleKey.of( object[0] );
            }
            case 2 -> {
                return PairCompositeKey.of(object[0], object[1]);
            }
            case 3 -> {
                return TripleCompositeKey.of(object[0], object[1], object[2]);
            }
            case 4 -> {
                return QuadrupleCompositeKey.of(object[0], object[1], object[2], object[3]);
            }
            default -> {
                return NCompositeKey.of( object );
            }
        }
    }

    public static IKey buildRecordKey(int[] columns, Object[] object){
        switch (columns.length){
            case 1 -> {
                return SimpleKey.of( object[columns[0]] );
            }
            case 2 -> {
                return PairCompositeKey.of(object[columns[0]], object[columns[1]]);
            }
            case 3 -> {
                return TripleCompositeKey.of(object[columns[0]], object[columns[1]], object[columns[2]]);
            }
            case 4 -> {
                return QuadrupleCompositeKey.of(object[columns[0]], object[columns[1]], object[columns[2]], object[columns[3]]);
            }
            default -> {
                if(columns.length == object.length){
                    return NCompositeKey.of( object );
                }
                Object[] values = new Object[columns.length];
                for(int i = 0; i < columns.length; i++){
                    values[i] = object[columns[i]];
                }
                return NCompositeKey.of( values );
            }
        }
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
            key = NCompositeKey.of( values );
        }
        return key;
    }

    public static IIndexKey buildIndexKey(int[] values){
        if(values.length == 1) return SimpleKey.of(values[0]);
        return NCompositeKey.of(Arrays.stream(values).boxed().toArray(Integer[]::new));
    }

}
