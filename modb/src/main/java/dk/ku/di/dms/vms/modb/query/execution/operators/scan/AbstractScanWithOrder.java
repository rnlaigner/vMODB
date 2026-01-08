package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.List;

public abstract class AbstractScanWithOrder extends AbstractScan {

    protected final int orderByColumn;

    public AbstractScanWithOrder(IMultiVersionIndex index,
                             int[] projectionColumns,
                             int orderByColumn,
                             int entrySize) {
        super(entrySize, index, projectionColumns);
        this.orderByColumn = orderByColumn;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void insert(List<Object[]> result, Object[] record) {
        int left = 0, right = result.size();
        while (left < right) {
            int mid = left + (right - left) / 2;
            Comparable k = (Comparable) result.get(mid)[this.orderByColumn];
            if (k.compareTo(record[this.orderByColumn]) < 1) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        result.add(left, record);
    }

}
