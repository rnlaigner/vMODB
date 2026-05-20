package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class FullScan extends AbstractScan {

    public FullScan(IMultiVersionIndex index,
                    int[] projectionColumns,
                    int entrySize) {
        super(entrySize, index, projectionColumns);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, Integer limit) {
        if(limit == null || limit == Integer.MAX_VALUE) {
            List<Object[]> res = new ArrayList<>();
            Iterator<Object[]> iterator = this.index.iterator(txCtx);
            while(iterator.hasNext()) {
                res.add(this.getProjection(iterator.next()));
            }
            return res;
        }
        List<Object[]> res = new ArrayList<>(limit);
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()) {
            res.add(this.getProjection(iterator.next()));
            if(res.size() == limit){
                return res;
            }
        }
        return res;
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, FilterContext filterContext, Integer limit) {
        if(limit == null || limit == Integer.MAX_VALUE) {
            List<Object[]> res = new ArrayList<>();
            Iterator<Object[]> iterator = this.index.iterator(txCtx);
            while(iterator.hasNext()) {
                Object[] obj = iterator.next();
                if(this.index.checkCondition(filterContext, obj)) {
                    res.add(this.getProjection(obj));
                }
            }
            return res;
        }
        List<Object[]> res = new ArrayList<>(limit);
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()) {
            Object[] obj = iterator.next();
            if(this.index.checkCondition(filterContext, obj)) {
                res.add(this.getProjection(obj));
                if(res.size() == limit){
                    return res;
                }
            }
        }
        return res;
    }

    @Override
    public boolean isFullScan() {
        return true;
    }

    @Override
    public FullScan asFullScan() {
        return this;
    }

}
