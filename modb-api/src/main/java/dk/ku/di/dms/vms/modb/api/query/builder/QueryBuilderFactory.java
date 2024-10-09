package dk.ku.di.dms.vms.modb.api.query.builder;

/**
 * The objective of this class is to provide an easy way to build SQL
 * queries in an object-oriented manner. Inspired by <a href="https://www.jooq.org">jooq</a>
 */

public final class QueryBuilderFactory {

    public static SelectStatementBuilder select() {
        return new SelectStatementBuilder();
    }

    public static UpdateStatementBuilder update() {
        return new UpdateStatementBuilder();
    }

    public static InsertStatementBuilder insert() {
        return new InsertStatementBuilder();
    }

    public static DeleteStatementBuilder delete() { return new DeleteStatementBuilder(); }
}
