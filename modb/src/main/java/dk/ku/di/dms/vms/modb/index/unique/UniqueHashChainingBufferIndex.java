package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBoundedBuffer;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;
import static java.lang.System.Logger.Level.INFO;

public final class UniqueHashChainingBufferIndex extends UniqueHashBufferIndex {

    public final Map<Long, AppendOnlyBoundedBuffer> chainingMap;

    public final String vmsIdentifier;

    public UniqueHashChainingBufferIndex(String vmsIdentifier, RecordBufferContext recordBufferContext, Schema schema, int[] columnsIndex, int capacity) {
        super(recordBufferContext, schema, columnsIndex, capacity);
        this.vmsIdentifier = vmsIdentifier;
        this.chainingMap = new ConcurrentHashMap<>();
    }

    @Override
    public void insert(IKey key, Object[] record){
        long pos = this.getFreePositionToInsert(key);
        if(pos == -1){
            long headPos = this.getPosition(key.hashCode());
            AppendOnlyBoundedBuffer aob;
            if(this.chainingMap.containsKey(headPos)){
                aob = this.chainingMap.get(headPos);
            } else {
                LOGGER.log(INFO, "Cannot find an empty entry for "+this.recordBufferCtx.fileName+". Creating a new chaining....\nKey: " + key+ " Hash: " + key.hashCode());
                aob = StorageUtils.loadAppendOnlyBoundedBuffer(this.vmsIdentifier, OPEN_ADDRESSING_ATTEMPTS, (int) this.recordSize, STR."\{this.recordBufferCtx.fileName}_\{headPos}", true);
                this.chainingMap.put(headPos, aob);
            }
            pos = aob.address;
            aob.forwardOffset(Schema.RECORD_HEADER + this.recordSize);
        }
        UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
        UNSAFE.putInt(null, pos + Header.SIZE, key.hashCode());
        this.doWrite(pos, record);
        this.updateSize(1);
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return new ChainedRecordIterator(this.recordBufferCtx.address, this.schema.getRecordSize(), this.capacity);
    }

    final class ChainedRecordIterator implements IRecordIterator<IKey> {

        private long nextAddress;
        private final int recordSize;

        private long mainCapacity;
        private int mainProgress;

        private long currChain = 0;
        private long chainCapacity;
        private int chainProgress;

        public ChainedRecordIterator(long address, int recordSize, long capacity){
            this.nextAddress = address;
            this.recordSize = recordSize;
            this.mainCapacity = capacity;
            this.mainProgress = 0;
        }

        @Override
        public boolean hasNext() {
            // checks for an active bit
            while(this.mainProgress < this.mainCapacity && UNSAFE.getByte(null, this.nextAddress) != Header.ACTIVE_BYTE){
                this.mainProgress++;
                this.nextAddress += recordSize;
            }

            if(chainingMap.containsKey(this.nextAddress)){
                if(currChain == 0) {
                    currChain = this.nextAddress;
                    AppendOnlyBoundedBuffer aob = chainingMap.get(this.nextAddress);
                    chainCapacity = (aob.nextOffset() - aob.address) / this.recordSize;
                    chainProgress = 0;
                    return true;
                } else {
                    return chainProgress < chainCapacity;
                }
            }

            return this.mainProgress < this.mainCapacity;
        }

        @Override
        public IKey next() {
            SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.nextAddress + 1));
            this.mainProgress++;
            this.nextAddress += this.recordSize;
            return res;
        }

        @Override
        public long address(){
            return this.nextAddress;
        }

    }

}
