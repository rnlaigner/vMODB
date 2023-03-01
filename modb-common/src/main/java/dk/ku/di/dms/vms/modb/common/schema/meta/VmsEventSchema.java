package dk.ku.di.dms.vms.modb.common.schema.meta;

import dk.ku.di.dms.vms.modb.common.type.DataType;

/**
 * The class record describes the schema of Transactional Events.
 */
public class VmsEventSchema {

    // the respective queue name
    public final String event;

    // the name of the columns
    public final String[] columnNames;

    // the data types of the columns
    public final DataType[] columnDataTypes;

    public VmsEventSchema(String event, String[] columnNames, DataType[] columnDataTypes) {
        this.event = event;
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
    }

}