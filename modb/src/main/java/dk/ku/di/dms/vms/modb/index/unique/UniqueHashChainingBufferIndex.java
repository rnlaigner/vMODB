package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBoundedBuffer;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;
import static java.lang.System.Logger.Level.*;

public final class UniqueHashChainingBufferIndex extends UniqueHashBufferIndex {

    public final Map<Integer, AppendOnlyBoundedBuffer> chainingMap;

    public final String suffix;

    private int chainSize;

    public UniqueHashChainingBufferIndex(String suffix, RecordBufferContext recordBufferContext, Schema schema, int[] columnsIndex, int capacity) {
        super(recordBufferContext, schema, columnsIndex, capacity);
        this.suffix = suffix;
        this.chainingMap = new ConcurrentHashMap<>();
    }

    @Override
    public Object[] lookupByKey(IKey key){
        int index = this.getIndex(key.hashCode());
        if (this.chainingMap.containsKey(index)) {
            long chainedPos = this.chainingMap.get(index).address;
            Object[] chainedRecord = this.readFromIndex(chainedPos + Schema.RECORD_HEADER);
            IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), chainedRecord);
            if (existingKey.equals(key)) {
                return chainedRecord;
            } else {
                return super.lookupByKey(key);
            }
        } else {
            return super.lookupByKey(key);
        }
    }

    @Override
    public void delete(IKey key) {
        long pos = this.findRecordAddress(key);
        if(pos != -1) {
            UNSAFE.putByte(null, pos, Header.INACTIVE_BYTE);
            this.updateSize(-1);
            return;
        }
        int index = this.getIndex(key.hashCode());
        AppendOnlyBoundedBuffer aob = this.chainingMap.get(index);
        if (aob != null) {
            long chainedPos = aob.address;
            final long lastPos = aob.nextOffset();
            long chainCapacity = (lastPos - chainedPos) / this.recordSize;
            while(chainedPos < lastPos) {
                Object[] chainedRecord = this.readFromIndex(chainedPos + Schema.RECORD_HEADER);
                IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), chainedRecord);
                if (existingKey.equals(key)) {
                    // simple solution for now, delete the entire chain if solo entry
                    if (chainCapacity == 1) {
                        this.chainingMap.remove(index);
                        return;
                    }
                    UNSAFE.putByte(null, chainedPos, Header.INACTIVE_BYTE);
                    this.chainSize--;
                    return;
                }
                chainedPos = chainedPos + this.recordSize;
            }
        }
    }

    @Override
    public void update(IKey key, Object[] record){
        if (this.updated(key, record)) return;
        LOGGER.log(ERROR, "Cannot find an existing record. Perhaps something wrong in the insertion logic?\nKey: " + key+ " Hash: " + key.hashCode());
    }

    @Override
    public void upsert(IKey key, Object[] record){
        if (this.updated(key, record)) return;
        // haven't found so far, then insert
        this.insert(key, record);
    }

    private boolean updated(IKey key, Object[] record) {
        // update logic
        long pos = this.findRecordAddress(key);
        if(pos != -1) {
            this.doWrite(pos, record);
            return true;
        }
        // chain logic
        int index = this.getIndex(key.hashCode());
        AppendOnlyBoundedBuffer aob = this.chainingMap.get(index);
        if (aob != null) {
            long chainedPos = aob.address;
            final long lastPos = aob.nextOffset();
            while(chainedPos < lastPos) {
                Object[] chainedRecord = this.readFromIndex(chainedPos + Schema.RECORD_HEADER);
                IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), chainedRecord);
                if (existingKey.equals(key)) {
                    UNSAFE.putByte(null, chainedPos, Header.ACTIVE_BYTE);
                    UNSAFE.putInt(null, chainedPos + Header.SIZE, key.hashCode());
                    this.doWrite(pos, record);
                    return true;
                }
                chainedPos = chainedPos + this.recordSize;
            }
        }
        return false;
    }

    @Override
    public void insert(IKey key, Object[] record){
        int keyHash = key.hashCode();
        long pos = this.getFreePositionToInsert(keyHash);
        if(pos == -1){
            int index = this.getIndex(keyHash);
            AppendOnlyBoundedBuffer aob;
            if(this.chainingMap.containsKey(index)){
                aob = this.chainingMap.get(index);
            } else {
                LOGGER.log(DEBUG, "Cannot find an empty entry for "+this.recordBufferCtx.fileName+". Creating a new chaining....\nKey: " + key + " Hash: " + keyHash);
                aob = StorageUtils.loadAppendOnlyBoundedBuffer(this.suffix, OPEN_ADDRESSING_ATTEMPTS, (int) this.recordSize, STR."\{this.recordBufferCtx.fileName}_\{index}");
                this.chainingMap.put(index, aob);
            }
            pos = aob.nextOffset();
            aob.forwardOffset(this.recordSize);
            UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
            UNSAFE.putInt(null, pos + Header.SIZE, keyHash);
            this.doWrite(pos, record);
            aob.force();
            this.chainSize++;
            return;
        }
        UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
        UNSAFE.putInt(null, pos + Header.SIZE, keyHash);
        this.doWrite(pos, record);
        this.updateSize(1);
    }

    @Override
    public Object[] record(IKey key) {
        int index = this.getIndex(key.hashCode());
        long pos = this.getPositionWithIndex(index);
        Object[] hashedRecord = this.readFromIndex(pos + Schema.RECORD_HEADER);
        IKey hashedKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), hashedRecord);
        if (hashedKey.equals(key)) {
            return hashedRecord;
        }
        AppendOnlyBoundedBuffer aob = this.chainingMap.get(index);
        if (aob != null) {
            long chainedPos = aob.address;
            final long lastPos = aob.nextOffset();
            while(chainedPos < lastPos) {
                Object[] chainedRecord = this.readFromIndex(chainedPos + Schema.RECORD_HEADER);
                IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), chainedRecord);
                if (existingKey.equals(key)) {
                    return chainedRecord;
                }
                chainedPos = chainedPos + this.recordSize;
            }
        }
        return null;
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return new ChainedRecordIterator(this.recordBufferCtx.address, this.schema.getRecordSize(), this.capacity);
    }

    final class ChainedRecordIterator implements IRecordIterator<IKey> {

        private long nextAddress;
        private final int recordSize;

        private final long mainCapacity;
        private int mainProgress;

        private long currChain;
        private long chainCapacity;
        private int chainProgress;

        private final Map<Integer, AppendOnlyBoundedBuffer> chainingMapCopy;

        public ChainedRecordIterator(long address, int recordSize, long capacity){
            this.nextAddress = address;
            this.recordSize = recordSize;
            this.mainCapacity = capacity;
            this.mainProgress = 0;
            this.currChain = -1;
            this.chainingMapCopy = new HashMap<>(chainingMap);
        }

        @Override
        public boolean hasNext() {
            // checks for an active bit
            while(this.mainProgress < this.mainCapacity && UNSAFE.getByte(null, this.nextAddress) != Header.ACTIVE_BYTE){
                this.mainProgress++;
                this.nextAddress += recordSize;
            }

            if(this.currChain == -1) {
                int keyHash = UNSAFE.getInt(null, this.nextAddress + Header.SIZE);
                int index = getIndex(keyHash);
                AppendOnlyBoundedBuffer aob = this.chainingMapCopy.remove(index);
                if(aob != null){
                    this.chainCapacity = (aob.nextOffset() - aob.address) / this.recordSize;
                    this.currChain = aob.address;
                    this.chainProgress = 0;
                    return true;
                }
            } else {
                if(this.chainProgress < this.chainCapacity) return true;
                this.currChain = -1;
            }

            return this.mainProgress < this.mainCapacity;
        }

        @Override
        public IKey next() {
            if(this.currChain > -1){
                SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.currChain + Header.SIZE));
                this.chainProgress++;
                this.currChain += this.recordSize;
                return res;
            }
            SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.nextAddress + Header.SIZE));
            this.mainProgress++;
            this.nextAddress += this.recordSize;
            return res;
        }

        @Override
        public long address(){
            if(this.currChain > -1){
                return this.currChain;
            }
            return this.nextAddress;
        }

    }

    @Override
    public void reset() {
        super.reset();
        Iterator<Map.Entry<Integer, AppendOnlyBoundedBuffer>> it = this.chainingMap.entrySet().iterator();
        Map.Entry<Integer, AppendOnlyBoundedBuffer> entry;
        while(it.hasNext()){
            entry = it.next();
            entry.getValue().unload();
            it.remove();
        }
    }

    @Override
    public int size() {
        return this.size + this.chainSize;
    }

}
