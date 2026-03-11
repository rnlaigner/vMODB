package dk.ku.di.dms.vms.modb.api.query.builder;

import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.AbstractStatement;

import static dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum.AND;
import static dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum.OR;

/**
 * A class that embraces the commonalities found in both SELECT
 * DELETE, and UPDATE statements such as where clauses
 * Bypassing the parsing from strings
 * Look at <a href="https://github.com/19WAS85/coollection#readme">...</a> and
 * Interesting to take a look: <a href="https://www.jinq.org/">JINQ</a>
 */
public abstract class AbstractStatementBuilder {

    public static class WhereClauseBridge<T extends AbstractStatement> implements IQueryBuilder<T> {

        protected final T statement;

        protected WhereClauseBridge(T statement) {
            this.statement = statement;
        }

        public WhereClausePredicate<T> where(final String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement element = new WhereClauseElement(param, expr, value);
            this.statement.whereClause.add(element);
            this.statement.SQL.append(param);
            this.statement.SQL.append(expr.value);
            this.statement.SQL.append('?');
            return new WhereClausePredicate<>(this.statement);
        }

        public T build(){
            return statement;
        }

    }

    public static class WhereClausePredicate<T extends AbstractStatement> implements IQueryBuilder<T> {

        private final T statement;

        protected WhereClausePredicate(T statement){
            this.statement = statement;
        }

        public WhereClausePredicate<T> and(String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement element = new WhereClauseElement(param,expr,value);
            this.statement.whereClause.add( element );
            this.statement.SQL.append(AND.value);
            this.statement.SQL.append(param);
            this.statement.SQL.append(expr.value);
            this.statement.SQL.append('?');
            return this;
        }

        public WhereClausePredicate<T> or(String param, final ExpressionTypeEnum expr, final Object value) {
            WhereClauseElement element = new WhereClauseElement(param,expr,value);
            this.statement.whereClause.add( element );
            this.statement.SQL.append(OR.value);
            this.statement.SQL.append(param);
            this.statement.SQL.append(expr.value);
            this.statement.SQL.append('?');
            return this;
        }

        public T build(){
            return statement;
        }

    }

}
