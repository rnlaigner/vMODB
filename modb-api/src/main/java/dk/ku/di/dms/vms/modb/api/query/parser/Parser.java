package dk.ku.di.dms.vms.modb.api.query.parser;

import dk.ku.di.dms.vms.modb.api.query.clause.OrderByClauseElement;
import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Only parses simple SELECT statements
 */
public final class Parser {

    /**
     *
     * @param sql a simple select statement (select <x,y,x,...> from table where <> order by
     */
    public static SelectStatement parse(String sql) {

        String[] tokens = sql.split(" ");

        assert tokens.length > 0 && tokens[0].equalsIgnoreCase("select");

        int i = 1;

        List<String> projection = new ArrayList<>(2);

        while(i < tokens.length && !tokens[i].equalsIgnoreCase("from")){
            // remove comma from all
            projection.add(tokens[i].replace(',',' ').trim());
            i++;
        }

        i++;
        assert i < tokens.length;
        String table = tokens[i];
        i++;
        assert i < tokens.length && tokens[i].equalsIgnoreCase("where");
        i++;

        List<WhereClauseElement> whereClauseElements = new ArrayList<>(2);

        // get triples of values
        while(i < tokens.length && !tokens[i].equalsIgnoreCase("order")){
            // remove comma from all
            String left = tokens[i];
            i++;
            ExpressionTypeEnum exp = getExpressionFromString(tokens[i]);

            // skip the input
            whereClauseElements.add( new WhereClauseElement(left, exp, null) );
            i = i + 2;

            // for now all where clauses ony contain AND
            if(i < tokens.length && (tokens[i].equalsIgnoreCase("and") || tokens[i].equalsIgnoreCase("or"))){
                i++;
            }
        }

        if(i == tokens.length) {
            return new SelectStatement(new StringBuilder(sql), projection, table, whereClauseElements);
        }

        // ORDER BY
        i = i + 2;
        String orderByColumn = tokens[i];
        List<OrderByClauseElement> orderByClauseElement;
        i++;
        if(i == tokens.length){
            orderByClauseElement = List.of(new OrderByClauseElement(orderByColumn));
            return new SelectStatement(new StringBuilder(sql), projection, List.of(table), whereClauseElements, orderByClauseElement);
        } else {
            String sortOrder = tokens[i];
            orderByClauseElement = List.of(new OrderByClauseElement(orderByColumn, sortOrder));
        }

        i++;
        // limit
        if(i == tokens.length) {
            return new SelectStatement(new StringBuilder(sql), projection, List.of(table), whereClauseElements, orderByClauseElement);
        } else {
            return new SelectStatement(new StringBuilder(sql), projection, List.of(table), whereClauseElements, orderByClauseElement, Integer.valueOf(tokens[i+1]));
        }
    }

    private static ExpressionTypeEnum getExpressionFromString(String exp){

        if (ExpressionTypeEnum.EQUALS.name.equalsIgnoreCase(exp)) {
            return ExpressionTypeEnum.EQUALS;
        }

        if (ExpressionTypeEnum.IN.name.equalsIgnoreCase(exp)) {
            return ExpressionTypeEnum.IN;
        }

        // complete
        return null;

    }

}
