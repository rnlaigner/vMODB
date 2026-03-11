package dk.ku.di.dms.vms.modb.api.query.builder;

import dk.ku.di.dms.vms.modb.api.query.clause.*;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.modb.api.query.enums.JoinTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.enums.OrderBySortOrderEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The clauses from a SELECT statement. Additional clauses found in {@link AbstractStatementBuilder}
 * <a href="https://docs.microsoft.com/en-us/sql/t-sql/queries/queries?view=sql-server-ver15">...</a>
 */
public class SelectStatementBuilder extends AbstractStatementBuilder  {

    private final EntryPoint entryPoint;

    protected SelectStatementBuilder(){
        SelectStatement statement = new SelectStatement();
        this.entryPoint = new EntryPoint(statement);
    }

    /**
     * @param param The column name
     * @return Resulting select with column
     */
    public NewProjectionOrFromClause project(String param) {
        return this.entryPoint.project(param);
    }

    public NewProjectionOrFromClause max(String param){
        return this.entryPoint.max(param);
    }

    public NewProjectionOrFromClause avg(String param){
        return this.entryPoint.avg(param);
    }

    public NewProjectionOrFromClause sum(String param){
        return this.entryPoint.sum(param);
    }

    public NewProjectionOrFromClause count(String param){
        return this.entryPoint.count(param);
    }

    protected class EntryPoint {

        private final SelectStatement statement;
        public EntryPoint(SelectStatement statement){
            this.statement = statement;
            this.statement.selectClause = new ArrayList<>();
            this.statement.groupBySelectClause = new ArrayList<>();
        }

        public NewProjectionOrFromClause project(String param) {
            String[] projection = param.replace(" ","").split(",");
            this.statement.selectClause.addAll(Arrays.asList(projection));
            this.statement.SQL.append("select ");
            this.statement.SQL.append(param);
            return new NewProjectionOrFromClause(this.statement,this);
        }

        public NewProjectionOrFromClause notFirstProject(String param) {
            String[] projection = param.replace(" ","").split(",");
            this.statement.selectClause.addAll(Arrays.asList(projection));
            this.statement.SQL.append(",");
            this.statement.SQL.append(param);
            return new NewProjectionOrFromClause(this.statement,this);
        }

        public NewProjectionOrFromClause avg(String param){
            GroupBySelectElement element = new GroupBySelectElement( param, GroupByOperationEnum.AVG );
            this.statement.SQL.append( GroupByOperationEnum.AVG.name() );
            return this.agg(element);
        }

        public NewProjectionOrFromClause min(String param){
            GroupBySelectElement element = new GroupBySelectElement( param, GroupByOperationEnum.MIN );
            this.statement.SQL.append( ", " );
            this.statement.SQL.append( GroupByOperationEnum.MIN.name() );
            this.statement.SQL.append( " ( " );
            this.statement.SQL.append( param );
            this.statement.SQL.append( " ) " );
            return this.agg(element);
        }

        public NewProjectionOrFromClause max(String param){
            GroupBySelectElement element = new GroupBySelectElement( param, GroupByOperationEnum.MAX );
            this.statement.SQL.append( ", " );
            this.statement.SQL.append( GroupByOperationEnum.MAX.name() );
            this.statement.SQL.append( " ( " );
            this.statement.SQL.append( param );
            this.statement.SQL.append( " ) " );
            return this.agg(element);
        }

        public NewProjectionOrFromClause count(String param){
            GroupBySelectElement element = new GroupBySelectElement( param, GroupByOperationEnum.COUNT );
            this.statement.SQL.append( GroupByOperationEnum.COUNT.name() );
            return this.agg(element);
        }

        public NewProjectionOrFromClause sum(String param){
            GroupBySelectElement element = new GroupBySelectElement( param, GroupByOperationEnum.SUM );
            this.statement.SQL.append( GroupByOperationEnum.SUM.name() );
            return this.agg(element);
        }

        private NewProjectionOrFromClause agg(GroupBySelectElement element){
            this.statement.groupBySelectClause.add( element );
            return new NewProjectionOrFromClause(statement,this);
        }

    }

    public class NewProjectionOrFromClause {

        private final SelectStatement statement;
        private final EntryPoint entryPoint;

        protected NewProjectionOrFromClause(SelectStatement selectStatement, EntryPoint entryPoint){
            this.statement = selectStatement;
            this.entryPoint = entryPoint;
        }

        public NewProjectionOrFromClause project(String param) {
            return this.entryPoint.notFirstProject(param);
        }

        public NewProjectionOrFromClause avg(String param){
            return this.entryPoint.avg(param);
        }

        public NewProjectionOrFromClause min(String param){
            return this.entryPoint.min(param);
        }

        public NewProjectionOrFromClause max(String param){
            return this.entryPoint.max(param);
        }

        public NewProjectionOrFromClause count(String param){
            return this.entryPoint.count(param);
        }

        public NewProjectionOrFromClause sum(String param){
            return this.entryPoint.sum(param);
        }

        public OrderByGroupByJoinWhereClauseBridge from(String param) {
            String[] projection = param.replace(" ","").split(",");
            this.statement.fromClause = new ArrayList<>(Arrays.asList(projection));
            this.statement.SQL.append(" from ");
            this.statement.SQL.append(param);
            return new OrderByGroupByJoinWhereClauseBridge(this.statement);
        }

    }

    public class JoinClausePredicate {

        private final SelectStatement statement;

        /**
         * The following attributes are used to cache the information received in the previous
         * join method, so it can be used in the "on" method for correctly matching the table and attributes
         */
        private final String table;
        private final String column;
        private final JoinTypeEnum joinType;

        protected JoinClausePredicate(SelectStatement statement, String table, String column){
            this.statement = statement;
            this.table = table;
            this.column = column;
            this.joinType = JoinTypeEnum.INNER_JOIN;
            this.statement.joinClause = new ArrayList<>();
        }

        public CondJoinWhereClauseBridge on(ExpressionTypeEnum expression, String table, String columnParam) {
            JoinClauseElement joinClauseElement =
                    new JoinClauseElement(this.table,this.column,this.joinType, expression, table, columnParam);
            this.statement.joinClause.add(joinClauseElement);
            /*
               cannot nullify now given I may still need in case of another join condition for this same JOIN
               this.tempJoinTable = null;
               this.tempJoinType = null;
            */
            this.statement.SQL.append(this.table);
            this.statement.SQL.append(this.column);
            this.statement.SQL.append(this.joinType.name);
            this.statement.SQL.append(table);
            this.statement.SQL.append(columnParam);
            return new CondJoinWhereClauseBridge( joinClauseElement, this.statement );
        }

    }

    /**
     * Since after a FROM clause both WHERE and JOIN clauses can be specified, this class serves as bridge to define what comes next
     */
    public class JoinWhereClauseBridge extends WhereClausePredicate<SelectStatement> {

        protected final SelectStatement statement;

        protected JoinWhereClauseBridge(SelectStatement statement){
            super(statement);
            this.statement = statement;
        }

        public JoinClausePredicate join(String table, String column) {
            return new JoinClausePredicate(this.statement, table, column);
        }

        public SelectStatement build(){
            return statement;
        }

    }

    /**
     * A join clause predicate can be proceeded by a where clause, another join or another join condition
     */
    public class CondJoinWhereClauseBridge extends JoinWhereClauseBridge {

        private final JoinClauseElement joinClauseElement;

        protected CondJoinWhereClauseBridge(JoinClauseElement joinClauseElement, SelectStatement statement){
            super(statement);
            this.joinClauseElement = joinClauseElement;
        }

        public CondJoinWhereClauseBridge and(String columnLeft, ExpressionTypeEnum expression, String columnRight){
            this.joinClauseElement.addCondition( columnLeft, expression, columnRight );
            return new CondJoinWhereClauseBridge(joinClauseElement, this.statement);
        }

        // JOIN with OR condition implement later or leave like this?
        // public CondJoinWhereClauseBridge or()
    }

    public class OrderByGroupByJoinWhereClauseBridge extends JoinWhereClauseBridge {

        private final SelectStatement statement;

        private boolean firstWhere = true;

        protected OrderByGroupByJoinWhereClauseBridge(SelectStatement statement) {
            super(statement);
            this.statement = statement; // to avoid cast
        }

        public OrderByGroupByJoinWhereClauseBridge where(final String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement element = new WhereClauseElement(param, expr, value);
            this.statement.whereClause.add( element );
            if(this.firstWhere) {
                this.statement.SQL.append(" where ");
                this.firstWhere = false;
            }
            this.statement.SQL.append(param);
            this.statement.SQL.append(expr.value);
            this.statement.SQL.append('?');
            return this;
        }

        @Override
        public OrderByGroupByJoinWhereClauseBridge and(String param, final ExpressionTypeEnum expr, final Object value){
            super.and(param, expr, value);
            return this;
        }

        public OrderByHavingBridge groupBy(String... params) {
            for( int i = 0; i < params.length; i++ ) {
                params[i] = params[i].replace(" ", "");
            }
            this.statement.groupByClause = new ArrayList<>(Arrays.asList(params));
            this.statement.SQL.append(" group by ");
            for(String param : params){
                this.statement.SQL.append(param);
            }
            return new OrderByHavingBridge(this.statement);
        }

        public OrderByClausePredicate orderBy(String... params){
            List<OrderByClauseElement> orderByClauseElements = new ArrayList<>();
            for (String param : params) {
                orderByClauseElements.add(new OrderByClauseElement(param.replace(" ", "")));
                this.statement.SQL.append(param);
            }
            this.statement.orderByClause = orderByClauseElements;
            return new OrderByClausePredicate(this.statement);
        }

    }

    public static class OrderByHavingBridge implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected OrderByHavingBridge(SelectStatement statement) {
            this.statement = statement; // to avoid cast
        }

        public QuerySeal limit(int limit){
            this.statement.limit = limit;
            return new QuerySeal(this.statement);
        }

        // only allowing one having for now
        public OrderByClause having(GroupByOperationEnum operation, String column, ExpressionTypeEnum expression, Number value){
            if(this.statement.havingClause == null) {
                this.statement.havingClause = new ArrayList<>();
            }
            this.statement.havingClause.add( new HavingClauseElement<>(operation, column, expression,value));
            return new OrderByClause(statement);
        }

        public OrderByClausePredicate orderBy(String... params){
            List<OrderByClauseElement> orderByClauseElements = new ArrayList<>();
            for (String param : params) {
                orderByClauseElements.add(new OrderByClauseElement(param.replace(" ", "")));
                this.statement.SQL.append(param);
            }
            this.statement.orderByClause = orderByClauseElements;
            return new OrderByClausePredicate(this.statement);
        }

        public SelectStatement build(){
            return statement;
        }
    }

    public static class OrderByClause implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected OrderByClause(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public OrderByClausePredicate orderBy(String... params){
            List<OrderByClauseElement> orderByClauseElements = new ArrayList<>();
            for (String param : params) {
                orderByClauseElements.add(new OrderByClauseElement(param.replace(" ", "")));
                this.statement.SQL.append(param);
            }
            this.statement.orderByClause = orderByClauseElements;
            return new OrderByClausePredicate(this.statement);
        }

        public SelectStatement build(){
            return statement;
        }

    }

    public static class OrderByClausePredicate implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected OrderByClausePredicate(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public QuerySeal desc(){
            for( OrderByClauseElement predicate : this.statement.orderByClause ){
                predicate.expression = OrderBySortOrderEnum.DESC;
            }
            return new QuerySeal(this.statement);
        }

        public SelectStatement build(){
            return this.statement;
        }

    }

    public static class QuerySeal implements IQueryBuilder<SelectStatement> {

        private final SelectStatement statement;

        protected QuerySeal(SelectStatement selectStatement){
            this.statement = selectStatement;
        }

        public QuerySeal limit(int limit){
            this.statement.limit = limit;
            return new QuerySeal(this.statement);
        }

        public SelectStatement build(){
            return statement;
        }
    }

}
