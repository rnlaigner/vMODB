package dk.ku.di.dms.vms.modb.api.query.clause;

import dk.ku.di.dms.vms.modb.api.query.enums.OrderBySortOrderEnum;

public class OrderByClauseElement {

    public final String column;
    public OrderBySortOrderEnum expression;

    public OrderByClauseElement(String column, String expression) {
        this.column = column;
        this.expression = expression.equalsIgnoreCase("desc") ? OrderBySortOrderEnum.DESC : OrderBySortOrderEnum.ASC;
    }

    public OrderByClauseElement(String column) {
        this.column = column;
        this.expression = OrderBySortOrderEnum.ASC;
    }

}
