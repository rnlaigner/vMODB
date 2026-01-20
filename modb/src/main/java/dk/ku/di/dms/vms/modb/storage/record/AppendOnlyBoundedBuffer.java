package dk.ku.di.dms.vms.modb.storage.record;

import java.lang.foreign.MemorySegment;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;

/**
 * Append-only buffer.
 * A buffer where records are sequentially written.
 * Not hashed as in the hashing technique.
 * --
 * An abstraction to avoid the caller to handle the offset
 * --
 * It abstracts a pre-assigned portion of the memory
 * for the continuous allocation of records.
 * --
 * Limitations:
 * - Non-resizable
 * - Does not support ordering (e.g., by a set of columns)
 * --
 * In the future:
 * - Implement bloom filter to support the exists operation:
 *  <a href="https://notes.volution.ro/v1/2022/07/notes/1290a79c/">Bloom Filter</a>
 * - Can also be used for caching writes of transactions
 *
 */
public final class AppendOnlyBoundedBuffer extends RecordBufferContext {

    // the offset of the bucket
    private long nextOffset;

    public AppendOnlyBoundedBuffer(MemorySegment memorySegment, String fileName) {
        super(memorySegment, fileName);
        this.nextOffset = memorySegment.address();
    }

    // move offset
    public void forwardOffset(long bytes){
        this.nextOffset += bytes;
    }

    public long address(){
        return this.address;
    }

    public long size(){
        return this.memorySegment.byteSize();
    }

    public long nextOffset() {
        return this.nextOffset;
    }

    /**
     *
     * It is responsibility of the caller to
     * ensure the bounds are respected.
     * This design is to favor performance over safety.
     * -
     * The caller must keep track of the remaining space
     * occupied = nextOffset - address - 1
     * remaining = capacity - occupied
     *
     */
    public void append(long srcAddress, long bytes){
        UNSAFE.copyMemory(null, srcAddress, null, this.nextOffset, bytes);
        this.nextOffset += bytes;
    }

    public void append(long value){
        UNSAFE.putLong(this.nextOffset, value);
        this.nextOffset += Long.BYTES;
    }

    public void append(int value){
        UNSAFE.putInt(this.nextOffset, value);
        this.nextOffset += Integer.BYTES;
    }

    public void append(float value){
        UNSAFE.putFloat(this.nextOffset, value);
        this.nextOffset += Float.BYTES;
    }

    public void append(double value){
        UNSAFE.putDouble(this.nextOffset, value);
        this.nextOffset += Float.BYTES;
    }

}
