package dk.ku.di.dms.vms.modb.query.planner.operators;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.query.planner.operators.join.HashJoinWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.AbstractScan;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.storage.memory.DataTypeUtils;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

public abstract class AbstractOperator {

    // the first node of the memory segment nodes
    protected MemoryRefNode memoryRefNode = null;

    protected AppendOnlyBuffer currentBuffer;

    protected final int entrySize;

    public AbstractOperator(int entrySize) {
        this.entrySize = entrySize;
    }

    @SuppressWarnings("unchecked")
    protected boolean checkCondition(long address, FilterContext filterContext, AbstractIndex<IKey> index){

        if(filterContext == null) return true;

        boolean conditionHolds = true;

        // the number of filters to apply
        int filterIdx = 0;

        // the filter index on which a given param (e.g., literals, zero, 1, 'SURNAME', etc) should apply
        int biPredIdx = 0;

        // simple predicates, do not involve input params (i.e, NULL, NOT NULL, EXISTS?, etc)
        int predIdx = 0;

        while( conditionHolds && filterIdx < filterContext.filterTypes.size() ){

            // no need to read active bit

            int columnIndex = filterContext.filterColumns.get(filterIdx);
            int columnOffset = index.getTable().getSchema().getColumnOffset( columnIndex );
            DataType dataType = index.getTable().getSchema().getColumnDataType( columnIndex );

            Object val = DataTypeUtils.getValue( dataType, address + columnOffset );

            // it is a literal passed to the query
            if(filterContext.filterTypes.get(filterIdx) == FilterType.BP) {
                conditionHolds = filterContext.biPredicates.get(biPredIdx).
                        apply(val, filterContext.biPredicateParams.get(biPredIdx));
                biPredIdx++;
            } else {
                conditionHolds = filterContext.predicates.get(predIdx).test( val );
                predIdx++;
            }

            filterIdx++;

        }

        return conditionHolds;

    }

    /**
     * Just abstracts on which memory segment a result will be written to
     *
     * Default method. Operators can create their own
     */
    protected void ensureMemoryCapacity(){

        if(currentBuffer.capacity() - currentBuffer.address() > entrySize){
            return;
        }

        // else, get a new memory segment
        MemoryRefNode claimed = MemoryManager.claim();

        claimed.next = memoryRefNode;
        memoryRefNode = claimed;

        this.currentBuffer = new AppendOnlyBuffer(claimed.address(), claimed.bytes());

    }

    /**
     * Only used by count operator
     * @param count
     */
    protected void append( int count ) {
        ensureMemoryCapacity();
        this.currentBuffer.append(1); // number of rows
        this.currentBuffer.append(count);
    }

    // must be overridden by the concrete operators
    public boolean isFullScan(){
        return false;
    }

    public boolean isIndexScan(){
        return false;
    }

    public boolean isHashJoin() { return false; }

    public IndexScanWithProjection asIndexScan(){
        throw new IllegalStateException("No index scan operator");
    }

    public FullScanWithProjection asFullScan(){
        throw new IllegalStateException("No full scan operator");
    }

    public AbstractScan asScan(){
        throw new IllegalStateException("No abstract scan operator");
    }

    public HashJoinWithProjection asHashJoin() { throw new IllegalStateException("No hash join operator"); }

}
