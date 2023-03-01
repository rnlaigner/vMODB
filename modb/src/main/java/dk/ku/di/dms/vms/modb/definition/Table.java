package dk.ku.di.dms.vms.modb.definition;

import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Basic building block
 * This class holds the metadata to other data structures that concern a table and its operations
 * In other words, it does not hold/store rows, since this is the task of an index
 */
public final class Table {

    // at first, I am considering the table name is immutable. the hash code is cached to uniquely identify the table in java maps
    public final int hashCode;

    public final String name;

    public final Schema schema;

    /**
     * All tables must have a pk, thus a primary index.
     * Besides, used for fast path on planner
     */
    private final PrimaryIndex primaryIndex;

    /**
     * Why foreign keys is on table and not on {@link Schema}?
     * (i) To avoid circular dependence schema <-> table.
     *      This way Schema does not know about the details of a Table
     *      (e.g., the primary index structure, the table name, the secondary indexes, etc).
     * (ii) The planner needs the foreign keys in order to define an optimal plan.
     *      As the table carries the PK, the table holds references to other tables.
     * (iii) Foreign key necessarily require coordination across different indexes that are maintaining records.
     *       In other words, foreign key maintenance is a concurrency control behavior.
     * /
     * The array int[] are the column positions, ordered, that form the foreign key (i.e., refer to the other table).
     * Who I am pointing to? The referenced or parent table.
     * -
     * Why references to foreign key constraints are here?
     * Because an index should not know about other indexes.
     * It is better to have an upper class taking care of this constraint.
     * Can be made parallel.
     */
    private final Map<PrimaryIndex, int[]> foreignKeys;

    /**
     * Why external foreign key is not handled through the {@link PrimaryIndex} class?
     * Because there are no updatesPerKeyMap and key generator in replicated tables.
     */
    private final Map<PrimaryIndex, int[]> externalForeignKeys;

    /**
     * Indexes from other tables pointing to the primary index of this table.
     * This attribute answers: Who is pointing to this table? Who is this table parenting?
      */
    public final List<NonUniqueSecondaryIndex> children;

    /**
     * Other indexes from this table, hashed by the column set in order of the schema
     *  logical key - column list in order that appear in the schema
     *  physical key - column list in order of index definition
      */
    public final Map<IIndexKey, NonUniqueSecondaryIndex> secondaryIndexMap;

    public Table(String name, Schema schema, PrimaryIndex primaryIndex,
                    Map<PrimaryIndex, int[]> foreignKeys, Map<PrimaryIndex, int[]> externalForeignKeys,
                    List<NonUniqueSecondaryIndex> secondaryIndexes, List<NonUniqueSecondaryIndex> children){
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
        this.primaryIndex = primaryIndex;
        this.externalForeignKeys = externalForeignKeys;
        this.foreignKeys = foreignKeys;
        this.secondaryIndexMap = secondaryIndexes.stream().collect(Collectors.toMap(NonUniqueSecondaryIndex::key, Function.identity()));
        this.children = children;
    }

    public Table(String name, Schema schema, PrimaryIndex primaryIndex){
        this.name = name;
        this.schema = schema;
        this.hashCode = name.hashCode();
        this.primaryIndex = primaryIndex;
        this.externalForeignKeys = Collections.emptyMap();
        this.secondaryIndexMap = Collections.emptyMap();
        this.foreignKeys = Collections.emptyMap();
        this.children = Collections.emptyList();
    }

    @Override
    public boolean equals(Object anotherTable){
        return anotherTable instanceof Table && this.hashCode == anotherTable.hashCode();
    }

    @Override
    public int hashCode(){
        return this.hashCode;
    }

    public Schema getSchema(){
        return this.schema;
    }

    public String getName(){
        return this.name;
    }

    public PrimaryIndex primaryKeyIndex(){
        return this.primaryIndex;
    }

    public Map<PrimaryIndex, int[]> foreignKeys(){
        return this.foreignKeys;
    }

    public Map<PrimaryIndex, int[]> externalForeignKeys() {
        return this.externalForeignKeys;
    }

}
