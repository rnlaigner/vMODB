package dk.ku.di.dms.vms.database.api;

import dk.ku.di.dms.vms.database.api.modb.BuilderException;
import dk.ku.di.dms.vms.database.query.parser.stmt.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;

public interface IQueryBuilder {

    public IQueryBuilder select(String param) throws BuilderException;

    public IQueryBuilder from(String param);

    public IQueryBuilder where(final String param, final ExpressionEnum expr, final Object value);

    public IQueryBuilder and(String param, final ExpressionEnum expr, final Object value);

    public IQueryBuilder or(String param, final ExpressionEnum expr, final Object value);

    public IQueryBuilder join(String param);

    public IQueryBuilder update(String param) throws BuilderException;

    public IQueryBuilder set(String param, Object value);

    public IStatement build();

}
