package dk.ku.di.dms.vms.modb.common.schema.meta;

import dk.ku.di.dms.vms.modb.common.type.DataType;

/**
 * Model a replicated table in a VMS
 */
public class VmsReplicatedTableSchema {

    public final String tableName;

    // identification of columns that form the primary key. all tables must have a primary key
    public final int[] primaryKeyColumns;

    // the name of the columns
    public final String[] columnNames;

    // the data types of the columns
    public final DataType[] columnDataTypes;

    public VmsReplicatedTableSchema(String tableName, int[] primaryKeyColumns,
                                    String[] columnNames, DataType[] columnDataTypes) {
        this.tableName = tableName;
        this.primaryKeyColumns = primaryKeyColumns;
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
    }

}