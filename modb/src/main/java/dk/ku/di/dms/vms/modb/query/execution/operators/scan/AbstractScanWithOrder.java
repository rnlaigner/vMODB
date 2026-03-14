package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.List;
import java.util.function.BiPredicate;

public abstract class AbstractScanWithOrder extends AbstractScan {

    protected final int orderByColumn;
    protected final boolean descending;

    @SuppressWarnings("rawtypes")
    protected final BiPredicate<Object[], Comparable> comparableBiPredicate;

    @SuppressWarnings("unchecked")
    public AbstractScanWithOrder(IMultiVersionIndex index,
                                 int[] projectionColumns,
                                 int orderByColumn,
                                 boolean descending,
                                 int entrySize) {
        super(entrySize, index, projectionColumns);
        this.orderByColumn = orderByColumn;
        this.descending = descending;
        if(this.descending){
            this.comparableBiPredicate = (record1, k1) -> k1.compareTo(record1[AbstractScanWithOrder.this.orderByColumn]) > 0;
        } else {
            this.comparableBiPredicate = (record1, k1) -> k1.compareTo(record1[AbstractScanWithOrder.this.orderByColumn]) <= 0;
        }
    }

    protected void insert(List<Object[]> result, Object[] record) {
        result.add(this.getPositionToInsert(result, record), record);
    }

    @SuppressWarnings("rawtypes")
    protected int getPositionToInsert(List<Object[]> result, Object[] record){
        int left = 0, right = result.size();
        while (left < right) {
            int mid = left + (right - left) / 2;
            Comparable k = (Comparable) result.get(mid)[this.orderByColumn];
            if (this.comparableBiPredicate.test(record, k)) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

}
