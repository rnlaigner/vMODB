package dk.ku.di.dms.vms.modb.definition;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.type.Constants;
import dk.ku.di.dms.vms.modb.common.type.DataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class describes the logical schema (or logical data model or table description) of a {@link Table}.
 * <a href="https://en.wikipedia.org/wiki/Logical_schema">Logical schema in Wikipedia</a>
 * <a href="https://stackoverflow.com/questions/7022755/whats-the-difference-between-a-catalog-and-a-schema-in-a-relational-database">Useful post is SO</a>
 */
public final class Schema {

    // flag active + materialized hashed PK
    public static final int RECORD_HEADER = Header.SIZE + Integer.BYTES;

    // identification of columns that form the primary key. all tables must have a primary key
    private final int[] primaryKeyColumns;

    // the name of the columns
    private final String[] columnNames;

    // the data types of the columns
    private final DataType[] columnDataTypes;

    // offset of the column in a buffer
    private final int[] columnOffset;

    // the constraints of this schema, where key: column position and value is the actual constraint
    private final Map<Integer, ConstraintReference> constraintMap;

    // basically a map of column name to exact position in row values
    private final Map<String, Integer> columnPositionMap;

    private final int recordSize; // the sum of all possible data types

    public Schema(String[] columnNames, DataType[] columnDataTypes, int[] primaryKeyColumns, ConstraintReference[] constraints) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;

        this.columnOffset = new int[columnDataTypes.length];

        int acc = RECORD_HEADER;
        for(int j = 0; j < columnDataTypes.length; j++){
            columnOffset[j] = acc;
            switch (columnDataTypes[j]){
                case LONG, DATE -> acc += Long.BYTES;
                case CHAR -> acc += Character.BYTES;
                case STRING -> acc += (Character.BYTES * Constants.DEFAULT_MAX_SIZE_STRING);
                case INT -> acc += Integer.BYTES;
                case FLOAT -> acc += Float.BYTES;
                case DOUBLE -> acc += Double.BYTES;
                case BOOL -> acc += 1; // byte size
            }
        }

        this.recordSize = acc;

        int size = columnNames.length;
        this.columnPositionMap = new HashMap<>(size);
        // build index map
        for(int i = 0; i < size; i++){
            columnPositionMap.put(columnNames[i],i);
        }
        this.primaryKeyColumns = primaryKeyColumns;

        if(constraints != null && constraints.length > 0){
            this.constraintMap = new HashMap<>( constraints.length );
            for( ConstraintReference constraintReference : constraints ){
                this.constraintMap.put( constraintReference.column, constraintReference );
            }
        } else {
            // to avoid null pointer downstream
            this.constraintMap = Collections.emptyMap();
        }
    }

    public Integer columnPosition(String columnName){
        return this.columnPositionMap.get(columnName);
    }

    public String[] columnNames(){
        return this.columnNames;
    }

    public DataType columnDataType(int columnIndex){
        return this.columnDataTypes[columnIndex];
    }

    public int[] getPrimaryKeyColumns(){
        return this.primaryKeyColumns;
    }

    public Map<Integer, ConstraintReference> constraints(){
        return this.constraintMap;
    }

    public int columnOffset(int columnIndex){
        return this.columnOffset[columnIndex];
    }

    public int[] columnOffset(){
        return this.columnOffset;
    }

    public int getRecordSize(){
        return this.recordSize;
    }

    public int getRecordSizeWithoutHeader(){
        return this.recordSize - RECORD_HEADER;
    }

}
