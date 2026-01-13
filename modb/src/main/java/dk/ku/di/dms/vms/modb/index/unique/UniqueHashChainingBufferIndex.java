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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;
import static java.lang.System.Logger.Level.INFO;

public final class UniqueHashChainingBufferIndex extends UniqueHashBufferIndex {

    public final Map<Long, AppendOnlyBoundedBuffer> chainingMap;

    public final String suffix;

    private int chainSize;

    public UniqueHashChainingBufferIndex(String suffix, RecordBufferContext recordBufferContext, Schema schema, int[] columnsIndex, int capacity) {
        super(recordBufferContext, schema, columnsIndex, capacity);
        this.suffix = suffix;
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
                LOGGER.log(INFO, "Cannot find an empty entry for "+this.recordBufferCtx.fileName+". Creating a new chaining....\nKey: " + key + " Hash: " + key.hashCode());
                aob = StorageUtils.loadAppendOnlyBoundedBuffer(this.suffix, OPEN_ADDRESSING_ATTEMPTS, (int) this.recordSize, STR."\{this.recordBufferCtx.fileName}_\{headPos}", true);
                this.chainingMap.put(headPos, aob);
            }
            pos = aob.address;
            aob.forwardOffset(Schema.RECORD_HEADER + this.recordSize);
            UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
            UNSAFE.putInt(null, pos + Header.SIZE, key.hashCode());
            this.doWrite(pos, record);
            aob.force();
            this.chainSize++;
            return;
        }
        UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
        UNSAFE.putInt(null, pos + Header.SIZE, key.hashCode());
        this.doWrite(pos, record);
        this.updateSize(1);
    }

    @Override
    public Object[] record(IKey key) {
        long pos = this.getPosition(key.hashCode());
        if (this.chainingMap.containsKey(pos)) {
            // before returning, make sure it is the same key
            long chainedPos = this.chainingMap.get(pos).address;
            Object[] chainedRecord = this.readFromIndex(chainedPos + Schema.RECORD_HEADER);
            IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), chainedRecord);
            if (existingKey.equals(key)) {
                return chainedRecord;
            }
        }
        return this.readFromIndex(pos + Schema.RECORD_HEADER);
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

        private long currChain = -1;
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
                if(this.currChain == -1) {
                    AppendOnlyBoundedBuffer aob = chainingMap.get(this.nextAddress);
                    this.chainCapacity = (aob.nextOffset() - aob.address) / this.recordSize;
                    this.currChain = aob.address;
                    this.chainProgress = 0;
                    return true;
                } else {
                    if(this.chainProgress < this.chainCapacity) return true;
                    this.currChain = -1;
                }
            }

            return this.mainProgress < this.mainCapacity;
        }

        @Override
        public IKey next() {
            if(this.currChain > -1){
                SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.currChain + Header.ACTIVE_BYTE));
                this.chainProgress++;
                this.currChain += this.recordSize;
                return res;
            }
            SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.nextAddress + Header.ACTIVE_BYTE));
            this.mainProgress++;
            this.nextAddress += this.recordSize;
            return res;
        }

        @Override
        public long address(){
            return this.nextAddress;
        }

    }

    @Override
    public void reset() {
        super.reset();
        var it = this.chainingMap.entrySet().iterator();
        while(it.hasNext()){
            var entry = it.next();
            entry.getValue().unload();
            it.remove();
        }
    }

    @Override
    public int size() {
        return this.size + this.chainSize;
    }

}
