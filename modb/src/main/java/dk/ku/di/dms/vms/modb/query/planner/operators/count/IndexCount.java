package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

/**
 * No projecting any other column for now
 *
 * Count is always integer. But can be indexed or not, like the scan
 * If DISTINCT, must maintain state.
 * 
 */
public class IndexCount extends AbstractOperator {

    private final AbstractIndex<IKey> index;

    private final FilterContext filterContext;

    private final IKey[] keys;

    private int count;

    public IndexCount(int id, AbstractIndex<IKey> index,
                                   FilterContext filterContext,
                                   IKey... keys) {
        super(id, Integer.BYTES);
        this.index = index;
        this.filterContext = filterContext;
        this.keys = keys;
        this.count = 0;
    }

    public MemoryRefNode run(){

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(checkCondition(address, filterContext, index)){
                    this.count++;
                }
            }

            append(count);
            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        long address;
        for(IKey key : keys){
            RecordBucketIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){

                address = iterator.next();

                if(checkCondition(address, filterContext, index)){
                    this.count++;
                }

            }
        }

        append(count);
        return memoryRefNode;

    }

}
