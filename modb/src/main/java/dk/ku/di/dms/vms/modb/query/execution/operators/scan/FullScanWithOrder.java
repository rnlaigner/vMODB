package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class FullScanWithOrder extends AbstractScanWithOrder {

    public FullScanWithOrder(IMultiVersionIndex index,
                             int[] projectionColumns,
                             int orderByColumn,
                             boolean descending,
                             int entrySize) {
        super(index, projectionColumns, orderByColumn, descending, entrySize);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, Integer limit){
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()) {
            this.insert(result, this.getProjection(iterator.next()));
        }
        if(result.isEmpty()) {
            return result;
        }
        if(limit == Integer.MAX_VALUE) {
            return result;
        }
        return result.subList(0, limit);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, FilterContext filterContext, Integer limit) {
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()) {
            Object[] record = iterator.next();
            if(this.index.checkCondition(filterContext, record)) {
                this.insert(result, this.getProjection(record));
            }
        }
        if(result.isEmpty()) {
            return result;
        }
        if(limit == Integer.MAX_VALUE) {
            return result;
        }
        return result.subList(0, limit);
    }

    @Override
    public boolean isFullScanWithOrder() {
        return true;
    }

    @Override
    public FullScanWithOrder asFullScanWithOrder() {
        return this;
    }

}
