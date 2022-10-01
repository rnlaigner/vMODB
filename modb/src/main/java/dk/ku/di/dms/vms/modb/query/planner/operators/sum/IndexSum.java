package dk.ku.di.dms.vms.modb.query.planner.operators.sum;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 *
 * On the other side, the sum is type-dependent. Can be done while the records are scanned.
 */
public class IndexSum extends Sum {

    public IndexSum(DataType dataType,
                    int columnIndex,
                    AbstractIndex<IKey> index) {
        super(dataType, columnIndex, index);
    }

    @SuppressWarnings("unchecked, rawtypes")
    public MemoryRefNode run(FilterContext filterContext, IKey... keys){

        SumOperation sumOperation = buildOperation(dataType);
        long address;
        int columnOffset = this.index.schema().getColumnOffset(columnIndex);

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();

            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(index.checkCondition(key, address, filterContext)){
                    address = index.getColumnAddress(key, address, columnIndex);
                    Object val = DataTypeUtils.getValue( dataType, address );
                    sumOperation.accept(val);

                }
            }

            appendResult(sumOperation);
            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        for(IKey key : keys){
            IRecordIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){
                if(index.checkCondition(iterator, filterContext)){
                    address = index.getColumnAddress(iterator, columnIndex);
                    Object val = DataTypeUtils.getValue( dataType, address );
                    sumOperation.accept(val);
                }
                iterator.next();
            }
        }

        appendResult(sumOperation);
        return memoryRefNode;

    }

}
