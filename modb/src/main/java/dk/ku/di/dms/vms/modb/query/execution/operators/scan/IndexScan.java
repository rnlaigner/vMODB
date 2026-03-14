package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.*;

/**
 * On-flight scanning, filtering, and projection in a single operator.
 * The caller knows the output format.
 * But called does not know how many records are.
 * Header:
 * int - number of rows returned
 * This must be performed by a proper method at the end of the procedure
 * We can have a method called seal or close() in abstract operator
 * It will put the information on header.
 */
public final class IndexScan extends AbstractScan {

    public IndexScan(IMultiVersionIndex index, int[] projectionColumns, int entrySize) {
        super(entrySize, index, projectionColumns);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key) {
        List<Object[]> res = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        while(iterator.hasNext()){
            res.add(this.getProjection(iterator.next()));
        }
        return res;
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key, FilterContext filterContext) {
        List<Object[]> res = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        while(iterator.hasNext()){
            Object[] record = iterator.next();
            if(this.index.checkCondition(filterContext, record)) {
                res.add(this.getProjection(record));
            }
        }
        return res;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey[] keys, boolean distinct) {
        Iterator<Object[]> iterator = this.index.iterator(txCtx, keys);
        if(distinct) {
            Set<Object[]> res = new TreeSet<>((o1, o2) -> {
                for(int i = 0; i < o1.length; i++) {
                    if(!o1[i].equals(o2[i])) {
                        return ((Comparable)o1[i]).compareTo(o2[i]);
                    }
                }
                return 0;
            });
            while(iterator.hasNext()) {
                res.add(this.getProjection(iterator.next()));
            }
            return new ArrayList<>(res);
        } else {
            List<Object[]> res = new ArrayList<>();
            while(iterator.hasNext()) {
                res.add(this.getProjection(iterator.next()));
            }
            return res;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey[] keys, boolean distinct, FilterContext filterContext) {
        Iterator<Object[]> iterator = this.index.iterator(txCtx, keys);
        if(distinct) {
            Set<Object[]> res = new TreeSet<>((o1, o2) -> {
                for(int i = 0; i < o1.length; i++) {
                    if(!o1[i].equals(o2[i])) {
                        return ((Comparable)o1[i]).compareTo(o2[i]);
                    }
                }
                return 0;
            });
            while(iterator.hasNext()) {
                Object[] record = iterator.next();
                if(this.index.checkCondition(filterContext, record)) {
                    res.add(this.getProjection(record));
                }
            }
            return new ArrayList<>(res);
        } else {
            List<Object[]> res = new ArrayList<>();
            while(iterator.hasNext()){
                Object[] record = iterator.next();
                if(this.index.checkCondition(filterContext, record)) {
                    res.add(this.getProjection(record));
                }
            }
            return res;
        }
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public IndexScan asIndexScan() {
        return this;
    }

}