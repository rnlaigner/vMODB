//package dk.ku.di.dms.vms.modb.query.complex.planner.operator.scan;
//
//import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
//import dk.ku.di.dms.vms.modb.definition.key.IKey;
//import dk.ku.di.dms.vms.modb.index.AbstractIndex;
//import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
//import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
//import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
//import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
//import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
//
///**
// * On-flight condition check
// */
//public abstract sealed class AbstractScan extends AbstractOperator permits SequentialScan, IndexScan  {
//
//    protected final AbstractIndex<IKey> index;
//
//    protected final FilterContext filterContext;
//
//    protected int conditionMet = 0;
//
//    public AbstractScan(AbstractIndex<IKey> index, FilterContext filterContext) {
//        super(Long.BYTES);
//        this.index = index;
//        this.filterContext = filterContext;
//    }
//
//    /**
//     * Scan methods
//     */
//
//    protected void processIterator(RecordIterator iterator) {
//        long address;
//        while(iterator.hasNext()){
//
//            address = iterator.next();
//
//            // if there is no condition, no reason to do scan
////            if(checkCondition(address, filterContext, index)){
////                append(iterator, address);
////                conditionMet++;
////            }
//
//        }
//    }
//
//    protected void processIterator(IRecordIterator iterator) {
//        long address;
//        while(iterator.hasNext()){
//
//            address = iterator.next();
//
////            if(checkCondition(address, filterContext, index)){
////                append(address);
////                conditionMet++;
////            }
//
//        }
//    }
//
//    /**
//     * Memory management methods specific for scans
//     */
//
//    protected void ensureMemoryCapacity(RecordIterator iterator){
//
//        if(currentBuffer.capacity() - currentBuffer.address() > entrySize){
//            return;
//        }
//
//        // else, get a new memory segment
//        MemoryRefNode claimed = null; // MemoryManager.claim(this.id, index.getTable(), iterator.size(), iterator.progress(), conditionMet);
//
//        claimed.next = memoryRefNode;
//        memoryRefNode = claimed;
//
//        this.currentBuffer = new AppendOnlyBuffer(claimed.address(), claimed.bytes());
//
//    }
//
//    protected void append(RecordIterator iterator, long address) {
//        ensureMemoryCapacity(iterator);
//        // add to the output memory space
//        this.currentBuffer.append(address);
//    }
//
//    /**
//     * Memory management operations.
//     * Data structure:
//     * - srcAddress (long) -> the src address of the record in the PK index
//     *
//     * @param address the src address of the record
//     */
//    protected void append(long address) {
//        ensureMemoryCapacity();
//        this.currentBuffer.append(address);
//    }
//
//}
