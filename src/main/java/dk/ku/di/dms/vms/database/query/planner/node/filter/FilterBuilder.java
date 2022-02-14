package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.store.meta.DataType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A filter builder.
 */
public class FilterBuilder {

    /**
     * The cache is progressively built (during application execution) instead of eagerly at startup
     */
    public static final Map<DataType, Map<ExpressionEnum,IFilter<?>>> cachedFilters = new ConcurrentHashMap<>();

    public static IFilter<?> build(final WherePredicate wherePredicate) throws Exception {

        DataType dataType = wherePredicate.columnReference.dataType;
        ExpressionEnum expressionEnum = wherePredicate.expression;

        Map<ExpressionEnum,IFilter<?>> filterDataTypeMap = cachedFilters.getOrDefault(dataType,null);
        if(filterDataTypeMap != null){
            if(filterDataTypeMap.get(expressionEnum) != null){
                return filterDataTypeMap.get(expressionEnum);
            }
        } else {
            filterDataTypeMap = new ConcurrentHashMap<>();
            cachedFilters.put( dataType, filterDataTypeMap );
        }

        IFilter<?> filter;

        switch(dataType){

            case INT: {
                filter = getFilter( wherePredicate.expression, Integer::compareTo );
                break;
            }
            case STRING: {
                filter = getFilter( wherePredicate.expression, String::compareTo );
                break;
            }
            case CHAR: {
                filter = getFilter( wherePredicate.expression, Character::compareTo );
                break;
            }
            case LONG: {
                filter = getFilter( wherePredicate.expression, Long::compareTo );
                break;
            }
            case DOUBLE: {
                filter = getFilter( wherePredicate.expression, Double::compareTo);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + dataType);
        }

        filterDataTypeMap.put(expressionEnum,filter);
        return filter;

    }

    public static <V> IFilter<V> getFilter(
            final ExpressionEnum expression,
            final Comparator<V> comparator) throws Exception {

        switch(expression){
            case EQUALS:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) == 0;
                    }
                };
            case NOT_EQUALS:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) != 0;
                    }
                };
            case LESS_THAN_OR_EQUAL:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) <= 0;
                    }
                };
            case LESS_THAN:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) < 0;
                    }
                };
            case GREATER_THAN:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) > 0;
                    }
                };
            case GREATER_THAN_OR_EQUAL:
                return new Filter<V>(comparator) {
                    @Override
                    public boolean eval(V x, V y) {
                        return this.comparator.compare( x, y ) >= 0;
                    }
                };
            case IS_NULL:
//                can be like this
                return new IFilter<V>() {
                    public boolean eval(V v) {
                        return v == null;
                    }
                };
//                can also be like this
//                return v -> v == null;
            // not a functional interface anymore
//                return Objects::isNull;
            case IS_NOT_NULL:
//                return Objects::nonNull;
                return new IFilter<V>() {
                    public boolean eval(V v) {
                        return v != null;
                    }
                };
            case LIKE: throw new Exception("Like does not apply to integer value.");
            default: throw new Exception("Predicate not implemented");
        }

    }

}
