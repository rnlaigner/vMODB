package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
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
                LOGGER.log(INFO, "Cannot find an empty entry for record object. Creating a new chaining.... \nKey: " + key+ " Hash: " + key.hashCode());
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

}
