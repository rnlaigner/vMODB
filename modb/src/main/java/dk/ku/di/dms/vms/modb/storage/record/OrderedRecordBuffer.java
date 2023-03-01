package dk.ku.di.dms.vms.modb.storage.record;

import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import jdk.internal.misc.Unsafe;

import java.util.LinkedList;

import static dk.ku.di.dms.vms.modb.definition.Header.active;
import static dk.ku.di.dms.vms.modb.definition.Header.inactive;

/**
 * A double-linked list maintained in a buffer.
 * Record-aware, meaning it requires knowing
 * the column of the schema that form the index
 * -
 * The structure of nodes is defined as below:
 * -
 * active (boolean) -> bit whether the record is active
 * previous (long) ->  the address of the previous record (in this buffer)
 * SK (int) -> SK to avoid rebuilding the sk on every iteration (trade-off to speed-up search, but a duplicate info)
 * ordered by the SK... does not mean all the Sk in this bucket belongs to the same sk...
 * implement conflict-free otherwise will be dependent on the input
 * srcAddress (long) -> the src address of the record in the PK index
 * next (long) -> the address of the next record (in this buffer)
 * private static class Entry {
 * public boolean active;
 * public long previous;
 * public int key;
 * public long srcAddress;
 * public long next;
 }
 */
public final class OrderedRecordBuffer {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private final LinkedList<Long> deletedOffsets;

    private final AppendOnlyBuffer buffer;

    // entry size :
    public static final int entrySize = 1 + Integer.BYTES + (3 * Long.BYTES);

    private long first;
    private long last;
    private long half;

    // number of records, excluding the deleted
    private int size;

    public static final int deltaKey = 1 + Long.BYTES;
    public static final int deltaOffset = 1 + Long.BYTES + Integer.BYTES;
    public static final int deltaNext = 1 + Integer.BYTES + (2 * Long.BYTES);
    public static final int deltaPrevious = 1;

    public OrderedRecordBuffer(AppendOnlyBuffer buffer) {
        this.buffer = buffer;
        this.first = buffer.address();
        this.last = buffer.address();
        this.deletedOffsets = new LinkedList<>();
        this.size = 0;
    }

    private void delete(long targetAddress){
        this.deletedOffsets.add(targetAddress);
        UNSAFE.putBoolean(null, targetAddress, false);
        this.size--;
        if(this.size % 2 != 0)
            this.half = UNSAFE.getLong(half + deltaPrevious);
    }

    public void delete(IKey key){
        // find position to insert provides exactly the key we are looking for
        Tuple<Long,Long> beginEndAddresses = findRange(key);
        // should I proceed with the delete operations forward or backward?
        // TODO finish
    }

    /**
     * Considering this bucket contains keys from different primary keys,
     */
    private Tuple<Long,Long> findRange(IKey key){
        return null;
    }

    /**
     * Different from {@link #findPositionToInsert(IKey)}.
     * Decidedly returns the first node
     * Whereas the {@link #findPositionToInsert(IKey)}
     * can return the last node of the key
     */
    public long findFirstOccurrence(IKey key){

        if(this.size == 0) return this.first;

        // size == 1
        if (this.first == this.last) return this.first;

        long bottomAddress;
        long upAddress;
        int bottomKey;
        int upKey;

        // start from half
        int halfKey = UNSAFE.getInt(this.half + deltaKey);

        if (key.hashCode() > halfKey) {

            // get the next of the half node
            bottomAddress = UNSAFE.getLong(this.half + deltaNext);
            upAddress = this.last;

        } else {

            bottomAddress = this.first;

            // get the previous node of the half node
            upAddress = UNSAFE.getLong(this.half + deltaPrevious);

        }

        do {

            bottomKey = UNSAFE.getInt(bottomAddress + deltaKey);

            if(key.hashCode() > bottomKey){
                bottomAddress = UNSAFE.getLong( bottomAddress + deltaNext );
            } else {
                return key.hashCode() == bottomKey ? bottomAddress : -1;
            }

            upKey = UNSAFE.getInt(upAddress + deltaKey);

            if(key.hashCode() < upKey){
                upAddress = UNSAFE.getLong( upAddress + deltaPrevious );
            } else if(key.hashCode() > upKey) { // the "up" has reached the first node first
                // bring the bottom key to this point, since the bottom may still be far
                bottomAddress = UNSAFE.getLong( upAddress + deltaNext );
                bottomKey = UNSAFE.getInt(bottomAddress + deltaKey);
                // stop the iteration
                return key.hashCode() == bottomKey ? bottomAddress : -1;
            }

        } while(true);

    }

    public void deleteFirst(IKey key){

        long targetAddress = existsAddress(key);

        if(targetAddress == -1) return;

        UNSAFE.putBoolean(null, targetAddress, inactive);

        size--;

        deletedOffsets.add(targetAddress);

        // update first, last, half accordingly
        if(this.size == 0){
            this.first = buffer.address();
            this.last = first;
            this.half = first;
            return;
        }

        // target address cannot be first and last at the same time,
        // otherwise the number of records would be 0 (see above)
        // that is why we have the "else" below
        if(targetAddress == first){
            // then the next is now the first
            this.first = UNSAFE.getLong(targetAddress + deltaNext);
        } else if(targetAddress == last){
            this.last = UNSAFE.getLong(targetAddress + deltaPrevious);

        }

        if(this.size == 1){
            this.half = this.first; // or last, does not matter
            return;
        }

        if(this.size == 2){
            this.half = this.last; // half is always the last in case of two records
            return;
        }

        // move the half
        this.half = UNSAFE.getLong(half + deltaPrevious);

    }

    public void update(IKey key, long srcAddress){

        // findPosition
        long targetAddress = existsAddress(key);

        // update srcAddress
        UNSAFE.putLong( targetAddress + deltaOffset, srcAddress );

    }

    /**
     * Insert a new record, leaving the buffer in order
     */
    public void insert(IKey key, long srcAddress){

        // the records may be very far from each other
        // a vacuum procedure may be invoked from time to time
        // to move the records, so they are clustered in memory,
        // avoiding paging and improving performance
        long destOffset;

        // get destination offset
        if(!this.deletedOffsets.isEmpty()){
            destOffset = this.deletedOffsets.removeLast();
        } else {
            destOffset = this.buffer.nextOffset();
            // increasing the "position" in the buffer
            this.buffer.forwardOffset(entrySize);
        }

        // set active
        UNSAFE.putBoolean( null, destOffset, active);

        if(this.size == 0){
            // then it is not possible to build the key from target address
            // put in place
            putRecordInfo( key.hashCode(), destOffset, srcAddress );
            this.half = destOffset;
            this.size++;
            return;
        }

        this.size++;

        long targetAddress = this.findPositionToInsert(key);

        // get the SK of the record stored in the target address
        int targetKey = UNSAFE.getInt(targetAddress + deltaKey);

        if(key.hashCode() > targetKey){
            putAfter(targetAddress, destOffset);
            // walk half forward
            if(this.size % 2 != 0)
                this.half = UNSAFE.getLong(this.half + deltaNext);
        } else {
            putBefore(targetAddress, destOffset);
            // walk "half" backward
            if(this.size % 2 != 0)
                this.half = UNSAFE.getLong(this.half + deltaPrevious);
        }

        // finish writing record info ( key + srcAddress )
        putRecordInfo( key.hashCode(), destOffset, srcAddress );

    }

    private void putRecordInfo(int key, long destOffset, long srcAddress){
        // overwrite the key
        UNSAFE.putInt( destOffset + deltaKey, key );
        // overwrite the srcAddress
        UNSAFE.putLong( destOffset + deltaOffset, srcAddress );
    }

    private void putBefore(long targetAddress, long destOffset) {
        if(targetAddress != first){
            // get the previous node of the target address node
            long auxPrevious = UNSAFE.getLong( targetAddress + deltaPrevious );
            // update the previous to auxPrevious
            UNSAFE.putLong( destOffset + deltaPrevious, auxPrevious );
        } else {
            this.first = destOffset;
        }
        // set previous
        UNSAFE.putLong( targetAddress + deltaPrevious, destOffset );
        // set the next
        UNSAFE.putLong( destOffset + deltaNext, targetAddress );
    }

    private void putAfter(long targetAddress, long destOffset) {
        // walk last if necessary
        if(this.last == targetAddress){
            this.last = destOffset;
        } else {
            // get the next of the target address node
            long auxNext = UNSAFE.getLong(targetAddress + deltaNext);
            // update the next to auxNext
            UNSAFE.putLong(destOffset + deltaNext, auxNext);
        }
        // set next
        UNSAFE.putLong(targetAddress + deltaNext, destOffset);
        // set previous
        UNSAFE.putLong(destOffset + deltaPrevious, targetAddress);
    }

    /**
     * Cannot apply binary search
     * Records are not sequentially placed in the memory
     * Moving records across addresses on every write/delete
     * may be expensive, and cost increase as more records
     * are found.
     * -
     * The optimization possible is keeping two pointers
     * One starts at the first and goes forward
     * The other starts at the last record and goes backwards
     * -
     * Best case: O(1) -> the first or the last
     * Worst case: O(n/2) -> the half of the list
     * Average case: < O(n/2)? depend on the workload
     * -
     * How to optimize this?
     * Maintain the half record.
     * The half moves for every insertion and deletion
     * The find position starts with the half
     * -
     * Guaranteed worst case: O(n/4)...
     *
     */
    private long findPositionToInsert(IKey key){

        if(this.size == 0) return this.first;

        if(this.first == this.last) return this.first;

        long bottomAddress;
        long upAddress;
        int bottomKey;
        int upKey;

        // start from half
        int halfKey = UNSAFE.getInt(half + deltaKey);

        if (key.hashCode() > halfKey) {

            // get the next of the half node
            if(this.half == this.last){
                bottomAddress = first;
            } else {
                // if half == last, the next will point to null
                bottomAddress = UNSAFE.getLong(half + deltaNext);
            }
            upAddress = this.last;

        } else {
            bottomAddress = this.first;
            // get the previous node of the half node
            upAddress = UNSAFE.getLong(half + deltaPrevious);
        }

        do {

            bottomKey = UNSAFE.getInt(bottomAddress + deltaKey);

            if(key.hashCode() > bottomKey){
                bottomAddress = UNSAFE.getLong( bottomAddress + deltaNext );
            } else {
                return bottomAddress;
            }

            upKey = UNSAFE.getInt(upAddress + deltaKey);

            if(key.hashCode() < upKey){
                upAddress = UNSAFE.getLong( upAddress + deltaPrevious );
            } else {
                return upAddress;
            }

        } while(bottomAddress != upAddress); // both arrived at the "half" of the list

        // return any
        return bottomAddress;

    }

    // same logic, but returns an address
    // return -1 if the node does not exist
    public long existsAddress(IKey key){
        // majority of cases
        if(this.size > 2) {

            long bottomAddress;
            long upAddress;
            int bottomKey;
            int upKey;

            // start from half
            int halfKey = UNSAFE.getInt(half + deltaKey);

            if (key.hashCode() > halfKey) {
                // get the next of the half node
                bottomAddress = UNSAFE.getLong(half + deltaNext);
                upAddress = last;
            } else {
                bottomAddress = first;
                // get the previous node of the half node
                upAddress = UNSAFE.getLong(half + deltaPrevious);
            }

            do {

                bottomKey = UNSAFE.getInt(bottomAddress + deltaKey);

                if (key.hashCode() == bottomKey) {
                    return bottomAddress;
                } else {
                    bottomAddress = UNSAFE.getLong(bottomAddress + deltaNext);
                }

                upKey = UNSAFE.getInt(upAddress + deltaKey);

                if (key.hashCode() == upKey) {
                    return upAddress;
                } else {
                    upAddress = UNSAFE.getLong(upAddress + deltaPrevious);
                }

            } while (bottomAddress != upAddress);

            // should make a read. the bottom is pointing to the previous "bottom"
            int mediumKey = UNSAFE.getInt(bottomAddress + deltaKey);

            return mediumKey == key.hashCode() ? bottomAddress : -1;
        }

        if(this.size == 0){
            return -1;
        }
        if(this.size == 1){
            int firstKey = UNSAFE.getInt(first + deltaKey);
            return firstKey == key.hashCode() ? first : -1;
        }
        if(this.size == 2){
            int firstKey = UNSAFE.getInt(first + deltaKey);
            int lastKey = UNSAFE.getInt(last + deltaKey);
            long res = firstKey == key.hashCode() ? first : -1;
            if(res == -1){
                return lastKey == key.hashCode() ? last : -1;
            }
            return res;
        }

        throw new IllegalStateException("Cannot have negative number of records");
    }

    public boolean exists(IKey key){
        long address = existsAddress(key);
        return address != -1;
    }

    /**
     * Minimum swap algorithm:
     * <a href="https://www.hackerrank.com/challenges/minimum-swaps-2">Minimum Swaps 2</a>
     * This is potentially performed in a batch commit
     * to alleviate the overhead of cache misses
     * for the next transactions
     * Strategy:
     * - start in the first bucket pos
     * - find the minimum record
     * - swap the minimum record with the record in the first bucket pos
     * repeat until all buckets are covered (i.e., ordered)
     * This can be optimized compared to the array version since we have a half
     * which allow us to cut in half the cost of finding the next minimum element
     */
    private void reorder(){

        long addressToSwap = this.buffer.address();
        long nextElemToSwap = this.first;

        MemoryRefNode mc = MemoryManager.getTemporaryDirectMemory(entrySize);

        // move along the linked list. if the next is not the next address, then swap
        int activeRecordsVerified = 0;
        while(activeRecordsVerified < size){

            // while the next element is inactive
            while(true){
                boolean active = UNSAFE.getBoolean(null, nextElemToSwap);
                if(active) {
                    break;
                }
                nextElemToSwap = UNSAFE.getLong( nextElemToSwap + deltaNext );
            }

            if(addressToSwap != nextElemToSwap){

                // if the addressToSwap is not active, no need to copy
                boolean active = UNSAFE.getBoolean(null, addressToSwap);

                if(active){
                    // copy first to auxiliary memory segment
                    UNSAFE.copyMemory( nextElemToSwap, mc.address(), entrySize );
                    // move the current address to the swapped elem address
                    UNSAFE.copyMemory( addressToSwap, nextElemToSwap, entrySize );
                    // bring the nextElemToSwap to the correct position
                    UNSAFE.copyMemory( mc.address(), addressToSwap, entrySize );
                } else {
                    // just overwrite
                    UNSAFE.copyMemory(nextElemToSwap, addressToSwap, entrySize);
                }

                // can already read from the new address
                nextElemToSwap = UNSAFE.getLong(addressToSwap+deltaNext);

                // next (physical) address
                addressToSwap += entrySize;
            }

            activeRecordsVerified++;

        }

        // at the end, all deleted offsets will be used
        deletedOffsets.clear();

    }

    public long address(){
        return this.buffer.address();
    }

    public int size(){
        return this.size;
    }

    public int getLastKey() {
        return UNSAFE.getInt( this.last + deltaKey );
    }

    // half of the records are transferred to the parameter buffer
    public void split(OrderedRecordBuffer buffer_){
        long currAddress = UNSAFE.getLong(this.half + deltaNext);
        // store before half is changed by the delete method
        long oldHalf = this.half;
        IKey auxKey;
        while(true) {
            // better than copying and adjusting the linked list, just use the api
            // read key
            auxKey = SimpleKey.of(UNSAFE.getInt(currAddress + deltaKey));
            long offset = UNSAFE.getLong(currAddress + deltaOffset);
            // System.out.println("Current offset is: "+ offset);
            buffer_.insert( auxKey, offset );
            delete(currAddress);
            // move to next
            if(currAddress == this.last) break;
            currAddress = UNSAFE.getLong(currAddress + deltaNext);
        }
        this.last = oldHalf;
    }

}
