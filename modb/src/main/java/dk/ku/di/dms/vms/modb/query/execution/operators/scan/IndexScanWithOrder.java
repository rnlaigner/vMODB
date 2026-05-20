package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class IndexScanWithOrder extends AbstractScanWithOrder {

    public IndexScanWithOrder(IMultiVersionIndex index, int[] projectionColumns, int orderByColumn, boolean descending, int entrySize) {
        super(index, projectionColumns, orderByColumn, descending, entrySize);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key, Integer limit) {
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        Object[] next;
        int pos;
        // prevent more projection than necessary
        if(limit == null || limit == Integer.MAX_VALUE) {
            while(iterator.hasNext()){
                next = iterator.next();
                pos = this.getPositionToInsert(result, next);
                result.add(pos, this.getProjection(next));
            }
            return result;
        }
        while(iterator.hasNext()){
            next = iterator.next();
            pos = this.getPositionToInsert(result, next);
            result.add(pos, this.getProjection(next));
        }
        if(result.size() < limit){
            return result;
        }
        return result.subList(0, limit);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key, FilterContext filterContext, Integer limit) {
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        while(iterator.hasNext()){
            Object[] record = iterator.next();
            if(this.index.checkCondition(filterContext, record)) {
                this.insert(result, this.getProjection(record));
            }
        }
        if(limit == null || limit == Integer.MAX_VALUE) {
            return result;
        }
        return result.subList(0, limit);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey[] keys, Integer limit) {
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, keys);
        while(iterator.hasNext()){
            this.insert(result, this.getProjection(iterator.next()));
        }
        if(limit == null || limit == Integer.MAX_VALUE) {
            return result;
        }
        return result.subList(0, limit);
    }

    @Override
    public boolean isIndexScanWithOrder() {
        return true;
    }

    @Override
    public IndexScanWithOrder asIndexScanWithOrder() {
        return this;
    }

}