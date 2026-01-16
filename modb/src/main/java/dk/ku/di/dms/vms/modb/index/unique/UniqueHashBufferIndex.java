package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteBufferIndex;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.unique.KeyRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.unique.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;

import java.util.concurrent.locks.ReentrantLock;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;
import static java.lang.System.Logger.Level.*;

/**
 * This index does not support growing number of keys.
 * Could deal with collisions by having a linked list.
 * This index is oblivious to isolation level and relational constraints.
 */
public class UniqueHashBufferIndex extends ReadWriteIndex<IKey> implements ReadWriteBufferIndex<IKey> {

    protected static final System.Logger LOGGER = System.getLogger(UniqueHashBufferIndex.class.getName());

    protected static final int OPEN_ADDRESSING_ATTEMPTS = 20;

    protected final RecordBufferContext recordBufferCtx;

    protected int size;

    protected final long recordSize;

    // total number of records (of a given schema)
    // the conjunction of all buffers can possibly hold
    protected final int capacity;

    // last addressable record
    private final long limit;

    // trailing zeros
    private final int p;

    // for operations that require exclusive access to the whole buffer like reset and checkpoint
    private final ReentrantLock lock = new ReentrantLock();

    public UniqueHashBufferIndex(RecordBufferContext recordBufferCtx, Schema schema, int[] columnsIndex, int capacity){
        super(schema, columnsIndex);
        this.recordBufferCtx = recordBufferCtx;
        this.recordSize = schema.getRecordSize();
        this.size = 0;
        this.capacity = capacity;
        this.limit = recordBufferCtx.address + (this.recordSize * (this.capacity == 1 ? 1 : this.capacity - 1));
        this.p = Integer.numberOfTrailingZeros(this.capacity);
    }

    @Override
    public void lock(){
        this.lock.lock();
    }

    @Override
    public void unlock(){
        this.lock.unlock();
    }

    /**
     * Initialize all entries
     */
    @Override
    public void reset() {
        this.lock();
        if(this.size == 0){
            this.unlock();
            LOGGER.log(DEBUG, "Size of buffer is zero. No need to reset.");
            return;
        }
        int initialSize = this.size;
        LOGGER.log(DEBUG, "Reset started with initial size: "+initialSize);
        long pos = this.recordBufferCtx.address;
        while(pos <= this.limit){
            if(UNSAFE.getByte(null, pos) == Header.ACTIVE_BYTE){
                UNSAFE.putByte(null, pos, Header.INACTIVE_BYTE);
                this.size--;
                if(this.size == 0) break;
            }
            pos = pos + this.recordSize;
        }
        if(this.size > 0){
            LOGGER.log(WARNING, "Reset did not clean all the entries. Size left out: "+this.size);
        } else {
            LOGGER.log(DEBUG, "Reset cleaned all the entries. Size left out: "+this.size);
        }
        this.size = 0;
        this.unlock();
    }

    /**
     * <a href="https://algs4.cs.princeton.edu/34hash/">Why (key & 0x7fffffff)?</a>
     * % 0x7fffffff returns a positive value if the key is negative
     */
    long getPosition(int keyHash){
        return this.recordBufferCtx.address + (this.recordSize * this.getIndex(keyHash));
    }

    int getIndex(int keyHash){
        return (keyHash * 0x9E3779B9) >>> (32 - this.p);
    }

    @Override
    public void insert(IKey key, long srcAddress) {
        long pos = this.getFreePositionToInsert(key);
        if(pos == -1){
            LOGGER.log(ERROR, "Cannot find an empty entry for inserting the record from address. Perhaps should increase number of entries?\nKey: " + key + " Hash: " + key.hashCode());
            return;
        }
        UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
        UNSAFE.putInt(null, pos, key.hashCode());
        UNSAFE.copyMemory(null, srcAddress, null, pos + Schema.RECORD_HEADER, this.schema.getRecordSizeWithoutHeader());
        this.updateSize(1);
    }

    void updateSize(int val){
        this.size = this.size + val;
    }

    /**
     * The update can be possibly optimized for updating only the fields required instead
     * of the whole record. This method assumes the srcAddress begins with header data
     */
    @Override
    public void update(IKey key, long srcAddress) {
        long pos = this.findRecordAddress(key);
        if(pos != -1) {
            UNSAFE.copyMemory(null, srcAddress, null, pos, this.recordSize);
            return;
        }
        LOGGER.log(WARNING, ERROR_FINDING);
    }

    @Override
    public void update(IKey key, Object[] record){
        long pos = this.findRecordAddress(key);
        if(pos != -1) {
            this.doWrite(pos, record);
            return;
        }
        LOGGER.log(ERROR, "Cannot find an existing record. Perhaps something wrong in the insertion logic?\nKey: " + key+ " Hash: " + key.hashCode());
    }

    void doWrite(long pos, Object[] record) {
        int maxColumns = this.schema.columnOffset().length;
        long currAddress = pos + Schema.RECORD_HEADER;
        for (int index = 0; index < maxColumns; index++) {
            DataType dt = this.schema.columnDataType(index);
            if (record[index] != null) {
                DataTypeUtils.callWriteFunction(currAddress, dt, record[index]);
            }
            currAddress += dt.value;
        }
    }

    /**
     * Overriding to write right away and avoid calling findRecordAddress() again
     */
    @Override
    public void upsert(IKey key, Object[] record){
        long pos = this.findRecordAddress(key);
        if(pos != -1) {
            this.doWrite(pos, record);
            return;
        }
        this.insert(key, record);
    }

    @Override
    public void insert(IKey key, Object[] record){
        long pos = this.getFreePositionToInsert(key);
        if(pos == -1){
            LOGGER.log(ERROR, "Cannot find an empty entry for "+this.recordBufferCtx.fileName+".\nKey: " + key + " Hash: " + key.hashCode());
            return;
        }
        UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
        UNSAFE.putInt(null, pos + Header.SIZE, key.hashCode());
        this.doWrite(pos, record);
        this.updateSize(1);
    }

    @Override
    public void delete(IKey key) {
        long pos = this.findRecordAddress(key);
        if(pos != -1) {
            UNSAFE.putByte(null, pos, Header.INACTIVE_BYTE);
            this.updateSize(-1);
            return;
        }
        LOGGER.log(WARNING, ERROR_FINDING);
    }

    @Override
    public long address(IKey key) {
        return this.findRecordAddress(key);
    }

    /**
     * Linear probing
     */
    long getFreePositionToInsert(IKey key){
        int attemptsToFind = OPEN_ADDRESSING_ATTEMPTS;
        int aux = 1;
        long origPos = this.getPosition(key.hashCode());
        long pos = origPos;
        do {
            if(UNSAFE.getByte(null, pos) != Header.ACTIVE_BYTE) {
                return pos;
            }
            attemptsToFind--;
            pos = origPos + (this.recordSize * aux);
            aux++;
        } while(attemptsToFind > 0 && pos <= this.limit);
        return -1;
    }

    protected long findRecordAddress(IKey key){
        int attemptsToFind = OPEN_ADDRESSING_ATTEMPTS;
        int aux = 1;
        long origPos = this.getPosition(key.hashCode());
        long pos = origPos;
        do {
            if(UNSAFE.getByte(null, pos) == Header.ACTIVE_BYTE) {
                Object[] existingRecord = this.readFromIndex(pos + Schema.RECORD_HEADER);
                IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), existingRecord);
                if (existingKey.equals(key)) {
                    return pos;
                }
            }
            attemptsToFind--;
            pos = origPos + (this.recordSize * aux);
            aux++;
        } while(attemptsToFind > 0 && pos <= this.limit);
        return -1;
    }

    /**
     * Check whether the record is active (if exists)
     */
    @Override
    public boolean exists(IKey key){
        return this.findRecordAddress(key) != -1;
    }
    
    private static final String ERROR_FINDING = "It was not possible to encounter the record";

    @Override
    public Object[] lookupByKey(IKey key){
        long pos = this.findRecordAddress(key);
        if(pos != -1) {
            return this.readFromIndex(pos + Schema.RECORD_HEADER);
        }
        return null;
    }

    @Override
    public boolean exists(long address){
        return UNSAFE.getByte(null, address) == Header.ACTIVE_BYTE;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return new RecordIterator(this.recordBufferCtx.address, this.schema.getRecordSize(), this.capacity);
    }

    @Override
    public IRecordIterator<IKey> iterator(IKey[] keys) {
        return new KeyRecordIterator(this, keys);
    }

    /**
     * This interface must be preferred when using {@link IRecordIterator}
     * since it avoids a duplicate lookup on the key.
     */
    @Override
    public Object[] record(IRecordIterator<IKey> iterator) {
        return this.readFromIndex(iterator.address() + Schema.RECORD_HEADER);
    }

    @Override
    public Object[] record(IKey key) {
        long pos = this.findRecordAddress(key);
        if (pos != -1) {
            return this.readFromIndex(pos + Schema.RECORD_HEADER);
        }
        return null;
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.UNIQUE;
    }

    @Override
    public void flush() {
        this.recordBufferCtx.force();
    }

}
