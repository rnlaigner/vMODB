package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterType;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;

import java.util.Iterator;

public interface IMultiVersionIndex {

    int[] indexColumns();

    boolean containsColumn(int columnPos);

    void undoTransactionWrites(TransactionContext txCtx);

    void installWrites(TransactionContext txCtx);

    boolean insert(TransactionContext txCtx, IKey key, Object[] record);

    boolean update(TransactionContext txCtx, IKey key, Object[] record);

    boolean remove(TransactionContext txCtx, IKey key);

    Object[] lookupByKey(TransactionContext txCtx, IKey key);

    default Iterator<Object[]> iterator(TransactionContext txCtx) { throw new UnsupportedOperationException(); }

    Iterator<Object[]> iterator(TransactionContext txCtx, IKey[] keys);

    default Iterator<Object[]> iterator(TransactionContext txCtx, IKey key){
        return this.iterator(txCtx, new IKey[] { key });
    }

    /**
     * This is the basic check condition. Does not take into consideration the versioned values.
     * @param record record values
     * @param filterContext the filter to be applied
     * @return the correct data item version
     */
    @SuppressWarnings("unchecked")
    default boolean checkCondition(FilterContext filterContext, Object[] record){
        if(filterContext == null) return true;
        boolean conditionHolds = true;
        // the number of filters to apply
        int filterIdx = 0;
        // the filter index on which a given param (e.g., literals, zero, 1, 'SURNAME', etc.) should apply
        int biPredIdx = 0;
        // simple predicates, do not involve input params (i.e, NULL, NOT NULL, EXISTS?, etc)
        int predIdx = 0;
        while( conditionHolds && filterIdx < filterContext.filterTypes.size() ){
            int columnIndex = filterContext.filterColumns.get(filterIdx);
            Object val = record[columnIndex];
            // it is a literal passed to the query
            if(filterContext.filterTypes.get(filterIdx) == FilterType.BP) {
                conditionHolds = filterContext.biPredicates.get(biPredIdx)
                        .apply(val, filterContext.biPredicateParams.get(biPredIdx));
                biPredIdx++;
            } else {
                conditionHolds = filterContext.predicates.get(predIdx).test( val );
                predIdx++;
            }
            filterIdx++;
        }
        return conditionHolds;
    }

    void reset();

    default boolean isSorted() {
        return false;
    }

}
