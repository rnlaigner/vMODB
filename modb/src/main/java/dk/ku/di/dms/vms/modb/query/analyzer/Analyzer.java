package dk.ku.di.dms.vms.modb.query.analyzer;

import dk.ku.di.dms.vms.modb.api.query.clause.GroupBySelectElement;
import dk.ku.di.dms.vms.modb.api.query.clause.JoinClauseElement;
import dk.ku.di.dms.vms.modb.api.query.clause.OrderByClauseElement;
import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.enums.JoinTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.DeleteStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.UpdateStatement;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.OrderByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Class responsible for analyzing a statement {@link IStatement}
 */
public final class Analyzer {

    private static final System.Logger LOGGER = System.getLogger(Analyzer.class.getName());

    private final Map<String, Table> catalog;

    public Analyzer(final Map<String, Table> catalog){
        this.catalog = catalog;
    }

    /**
     * basically transforms the raw input into known and safe metadata, e.g., whether a table, column exists
     * <a href="https://docs.microsoft.com/en-us/sql/t-sql/queries/select-transact-sql?view=sql-server-ver15#logical-processing-order-of-the-select-statement">T-SQL</a>
     * @param statement The statement to process
     * @return The resulting query tree
     * @throws AnalyzerException Unexpected statement type
     */
    public QueryTree analyze(final IStatement statement) throws AnalyzerException {
        switch (statement.getType()){
            case SELECT -> { return this.analyzeSelectStatement( statement.asSelectStatement() ); }
            // case INSERT -> { return null; }
            case UPDATE -> { return this.analyzeUpdateStatement( statement.asUpdateStatement()); }
            case DELETE -> { return this.analyzeDeleteStatement( statement.asDeleteStatement()); }
            default -> throw new IllegalStateException("No statement type identified.");
        }
    }

    private QueryTree analyzeSelectStatement(final SelectStatement statement) throws AnalyzerException {
        QueryTree queryTree = new QueryTree();
        // from
        // obtain the tables to look for the columns in projection first
        for(String tableStr : statement.fromClause){
            Table table = this.catalog.get(tableStr);
            if(table != null) queryTree.tables.put(tableStr,table);
        }
        // join
        if(statement.joinClause != null) {
            List<JoinClauseElement> joinClauseElements = statement.joinClause;
            for (JoinClauseElement join : joinClauseElements) {
                // TODO an optimization is iterating through the foreign keys of the table to find the match faster
                Table tableLeft = queryTree.tables.get(join.tableLeft);
                // find in catalog if not found in query yet
                if (tableLeft == null) {
                    tableLeft = catalog.get(join.tableLeft);
                    if (tableLeft != null) {
                        queryTree.tables.put(join.tableLeft, tableLeft);
                    } else {
                        throw new AnalyzerException("Unknown " + join.tableLeft + " table.");
                    }
                }
                // find left column
                ColumnReference columnLeftReference = new ColumnReference(join.columnLeft, tableLeft);
                Table tableRight = queryTree.tables.get(join.tableRight);
                if (tableRight == null) {
                    tableRight = catalog.get(join.tableRight);
                    if (tableRight != null) {
                        queryTree.tables.put(join.tableRight, tableRight);
                    } else {
                        throw new AnalyzerException("Unknown " + join.tableRight + " table.");
                    }
                }
                // find right column
                ColumnReference columnRightReference = new ColumnReference(join.columnRight, tableRight);
                // build typed join clause
                JoinPredicate joinClause = new JoinPredicate(columnLeftReference, columnRightReference, join.expression, join.joinType);
                queryTree.joinPredicates.add(joinClause);
            }
        }
        // projection
        // columns in projection may come from join
        List<String> columns = statement.selectClause;
        // case where the user input is '*'
        if (columns.size() == 1 && columns.getFirst().contentEquals("*")) {
            // iterate over all tables involved
            for (final Table table : queryTree.tables.values()) {
                Schema tableSchema = table.schema();
                int colPos = 0;
                for (String columnName : tableSchema.columnNames()) {
                    queryTree.projections.add(new ColumnReference(columnName, colPos, table));
                    colPos++;
                }
            }
        } else {
            // cannot allow same column name without AS from multiple tables
            for (String columnRefStr : columns) {
                if (columnRefStr.contains(".")) {
                    String[] split = columnRefStr.split("\\.");
                    ColumnReference columnReference = findColumnReference(split[1], split[0], queryTree.tables);
                    queryTree.projections.add(columnReference);
                } else {
                    ColumnReference columnReference = findColumnReference(columnRefStr, queryTree.tables);
                    queryTree.projections.add(columnReference);
                }
            }
        }
        // group by
        List<ColumnReference> groupByColumnsReference = null;
        if(statement.groupByClause != null) {
            for (String column : statement.groupByClause) {
                if (groupByColumnsReference == null) {
                    groupByColumnsReference = new ArrayList<>(statement.groupByClause.size());
                }
                ColumnReference columnReference;
                if (column.contains(".")) {
                    String[] split = column.split("\\.");
                    columnReference = findColumnReference(split[1], split[0], queryTree.tables);
                } else {
                    columnReference = findColumnReference(column, queryTree.tables);
                }
                queryTree.groupByColumns.add(columnReference);
            }
        }
        if(statement.groupBySelectClause != null) {
            List<GroupBySelectElement> groupByProjections = statement.groupBySelectClause;
            for (GroupBySelectElement element : groupByProjections) {
                ColumnReference columnReference;
                if (element.column().contains(".")) {
                    String[] split = element.column().split("\\.");
                    columnReference = this.findColumnReference(split[1], split[0], queryTree.tables);
                } else {
                    columnReference = this.findColumnReference(element.column(), queryTree.tables);
                }
                // that means each aggregate final value must take into consideration the set of group by columns
                queryTree.groupByProjections.add(new GroupByPredicate(columnReference, element.operation() ));
            }
        }
        // having clause is missing for now
        // TODO make sure these exceptions coming from where clause are thrown in the analyzer
        //  e.g., numeric comparisons between numbers and string/characters
        // where
        for (WhereClauseElement currWhere : statement.whereClause) {
            ColumnReference columnReference;
            String tableName;
            String columnName;
            if (currWhere.column().contains(".")) {
                String[] split = currWhere.column().split("\\.");
                tableName = split[0];
                columnName = split[1];
                Table table = queryTree.tables.get(tableName);
                if (table != null) {
                    columnReference = new ColumnReference(columnName, table);
                } else {
                    throw new AnalyzerException("Table not defined in the query: " + tableName);
                }
            } else {
                columnReference = this.findColumnReference(currWhere.column(), queryTree.tables);
            }
            if (currWhere.value() == null) {
                // throw new AnalyzerException("Parameter of where clause cannot be null value");
                LOGGER.log(System.Logger.Level.DEBUG, "Query: "+statement.SQL+" \nParameter "+currWhere.column()+"\n Value in the where clause is null.");
            } else {
                // 1. is it a reference to a table or a char? e.g., "'something'"
                // 2. check if there is some inner join. i.e., the object is a literal?
                if (currWhere.value() instanceof String value) {
                    ColumnReference columnReference1;
                    if (value.contains(".")) {
                        // <table>.<column>
                        String[] split = value.split("\\.");
                        tableName = split[0];
                        columnName = split[1];
                        Table table = queryTree.tables.get(tableName);
                        if (table != null) {
                            columnReference1 = new ColumnReference(columnName, table);
                            // build typed join clause
                            JoinPredicate joinClause = new JoinPredicate(columnReference, columnReference1, currWhere.expression(), JoinTypeEnum.INNER_JOIN);
                            queryTree.joinPredicates.add(joinClause);
                            continue;
                        }  // table is null, so no table, it is a literal
                    }
                }
                // check if the type is correct
                if (CHECK_PARAM_TYPE) {
                    DataType dataType = DataTypeUtils.getColumnDataTypeFromAttributeType(currWhere.value().getClass());
                    if (dataType != columnReference.dataType) {
                        throw new AnalyzerException("Incompatible data types. Expected: " + columnReference.dataType + " Provided: " + dataType + "\nQuery:\n" + statement.SQL);
                    }
                }
            }
            WherePredicate whereClause = new WherePredicate(columnReference, currWhere.expression(), currWhere.value());
            // the order of the columns declared in the index definition matters
            queryTree.addWhereClauseSortedByColumnIndex(whereClause);
        }
        if(statement.orderByClause != null) {
            for (OrderByClauseElement element : statement.orderByClause) {
                ColumnReference columnReference = new ColumnReference(element.column, this.catalog.get(statement.fromClause.getFirst()));
                queryTree.orderByPredicates.add(new OrderByPredicate(columnReference, element.expression));
            }
        }
        if(statement.limit != null){
            queryTree.limit = Optional.of( statement.limit );
        } else {
            queryTree.limit = Optional.empty();
        }
        if(statement.distinct){
            queryTree.distinct = Optional.of(true);
        } else {
            queryTree.distinct = Optional.empty();
        }
        return queryTree;
    }

    private static final boolean CHECK_PARAM_TYPE = false;

    /**
     * Analyze simple where clauses, those not involving join, only a table
     * @param whereClause the passed where clause
     * @return the parsed predicates
     */
    public List<WherePredicate> analyzeWhere(Table table, List<WhereClauseElement> whereClause) throws AnalyzerException {
        List<WherePredicate> newList = new ArrayList<>(whereClause.size());
        // this assumes param is a value (number, char, or string)
        for(WhereClauseElement element : whereClause){
            if(!columnNameIsFoundInSchema(element.column(), table.schema())) {
                throw new AnalyzerException("Column does not exist in the table");
            }
            ColumnReference columnReference = new ColumnReference(element.column(), table);
            newList.add( new WherePredicate(columnReference, element.expression(), element.value()) );
        }
        if(newList.size() > 1)
            newList.sort(null);
        return newList;
    }

    /**
     * For now, return a query tree. Later, revisit this choice
     * @param deleteStatement delete with where
     * @return query tree
     */
    private QueryTree analyzeDeleteStatement(DeleteStatement deleteStatement) throws AnalyzerException {
        Table table = catalog.get(deleteStatement.table);
        return new QueryTree(analyzeWhere(table, deleteStatement.whereClause));
    }

    private QueryTree analyzeUpdateStatement(UpdateStatement updateStatement) throws AnalyzerException {
        Table table = catalog.get(updateStatement.table);
        return new QueryTree(analyzeWhere(table, updateStatement.whereClause));
    }

    private ColumnReference findColumnReference(String columnStr, Map<String, Table> tables) throws AnalyzerException {
        ColumnReference columnReferenceToResult = null;
        for(Table table : tables.values()){
            final Schema schema = table.schema();
            final Integer columnIndex = schema.columnPosition(columnStr);
            if(columnIndex != null) {
                if(columnReferenceToResult == null) {
                    columnReferenceToResult = new ColumnReference(columnStr, columnIndex, table);
                } else {
                    throw new AnalyzerException("Cannot refer to a column name that appear in more than a table without proper reference in the query");
                }
            }
        }
        if(columnReferenceToResult != null) {
            return columnReferenceToResult;
        }
        throw new AnalyzerException("Column " + columnStr +" does not exist in the catalog of tables");
    }

    private boolean columnNameIsFoundInSchema(String columnStr, Schema schema) {
        Integer columnIndex = schema.columnPosition(columnStr);
        return columnIndex != null;
    }

    /**
     * Another strategy is giving to the constructor of ColumnReference the duty to check whether the columnIndex exists
     * This way we avoid checking == null everytime.
     * @param columnName The column name
     * @param tableName The table name
     * @param tables The tables mapped in the query tree (to be formed) so far
     * @return The column reference
     * @throws AnalyzerException Column does not exist in the table
     */
    private ColumnReference findColumnReference(final String columnName, final String tableName, final Map<String,Table> tables) throws AnalyzerException {

        if(tables.get(tableName) == null ){
            throw new AnalyzerException("Table "+ tableName + " does not exist in the catalog.");
        }
        final Table table = tables.get(tableName);
        final Schema schema = table.schema();
        Integer columnIndex = schema.columnPosition(columnName);
        if(columnIndex == null){
            throw new AnalyzerException("Column does not exist in the table "+ tableName);
        }
        return new ColumnReference(columnName, columnIndex, table);
    }

}
