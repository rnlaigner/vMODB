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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

public final class UniqueHashChainingBufferIndex extends UniqueHashBufferIndex {

    public final Map<Integer, AppendOnlyBoundedBuffer> chainingMap;

    private final String prefix;

    // number of records in internal chains
    private int chainedSize;

    private final Set<Integer> pendingFlush;

    private UniqueHashChainingBufferIndex(String prefix, RecordBufferContext recordBufferContext, Schema schema, int[] columnsIndex, int capacity, Map<Integer, AppendOnlyBoundedBuffer> chainingMap) {
        super(recordBufferContext, schema, columnsIndex, capacity);
        this.prefix = prefix;
        this.chainingMap = chainingMap;
        this.pendingFlush = new HashSet<>();
    }

    public static UniqueHashChainingBufferIndex build(String prefix, RecordBufferContext recordBufferContext, Schema schema, int[] columnsIndex, int capacity, boolean isTruncating){
        if(isTruncating){
            return new UniqueHashChainingBufferIndex(prefix, recordBufferContext, schema, columnsIndex, capacity, new HashMap<>());
        }
        // check existing files
        String basePathStr = StorageUtils.getBasePath(prefix);
        Path basePath = Paths.get(basePathStr);
        try(Stream<Path> paths = Files.walk(basePath)){
            List<Path> chains = paths.filter(path -> path.toString().contains(recordBufferContext.fileName+"_")).toList();
            Map<Integer, AppendOnlyBoundedBuffer> map = new HashMap<>(chains.size());
            for(Path path : chains){
                String fileName = path.getFileName().toString();
                Integer index = Integer.valueOf(fileName.substring(fileName.indexOf("_")+1,fileName.indexOf(".")));
                AppendOnlyBoundedBuffer aob = StorageUtils.loadAppendOnlyBoundedBuffer(prefix, OPEN_ADDRESSING_ATTEMPTS, schema.getRecordSize(), STR."\{recordBufferContext.fileName}_\{index}", false);
                map.put(index, aob);
            }
            return new UniqueHashChainingBufferIndex(prefix, recordBufferContext, schema, columnsIndex, capacity, map);
        } catch (IOException e){
            LOGGER.log(ERROR, "Error captured while trying to access base path: \n"+e);
        }
        return new UniqueHashChainingBufferIndex(prefix, recordBufferContext, schema, columnsIndex, capacity, new HashMap<>());
    }

    @Override
    public boolean exists(IKey key){
        return this.record(key) != null;
    }

    @Override
    public Object[] lookupByKey(IKey key) {
        return this.record(key);
    }

    @Override
    public Object[] record(IKey key) {
        int index = this.getIndex(key.hashCode());
        long pos = this.getPositionWithIndex(index);
        if(UNSAFE.getByte(null, pos) == Header.INACTIVE_BYTE) return null;
        Object[] hashedRecord = this.readFromIndex(pos + Schema.RECORD_HEADER);
        IKey hashedKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), hashedRecord);
        if (hashedKey.equals(key)) {
            return hashedRecord;
        }
        return this.lookupChain(key, index);
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
                    this.chainedSize--;
                    this.pendingFlush.add(index);
                    return;
                }
                chainedPos = chainedPos + this.recordSize;
            }
        }
    }

    @Override
    public void update(IKey key, Object[] record) {
        if (this.updated(key, record)) return;
        LOGGER.log(ERROR, "Cannot find an existing record. Perhaps something wrong in the insertion logic?\nKey: " + key+ " Hash: " + key.hashCode());
    }

    @Override
    public void upsert(IKey key, Object[] record) {
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
        return this.doChain(key, record);
    }

    private boolean doChain(IKey key, Object[] record) {
        int index = this.getIndex(key.hashCode());
        AppendOnlyBoundedBuffer aob = this.chainingMap.get(index);
        if (aob == null) {
            return false;
        }
        long chainedPos = aob.address;
        final long lastPos = aob.nextOffset();
        while(chainedPos < lastPos) {
            Object[] chainedRecord = this.readFromIndex(chainedPos + Schema.RECORD_HEADER);
            IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), chainedRecord);
            if (existingKey.equals(key)) {
                UNSAFE.putByte(null, chainedPos, Header.ACTIVE_BYTE);
                UNSAFE.putInt(null, chainedPos + Header.SIZE, key.hashCode());
                this.doWrite(chainedPos, record);
                this.pendingFlush.add(index);
                return true;
            }
            chainedPos = chainedPos + this.recordSize;
        }
        return false;
    }

    @Override
    public void insert(IKey key, Object[] record) {
        int keyHash = key.hashCode();
        long pos = this.getFreePositionToInsert(keyHash);
        if(pos == -1) {
            int index = this.getIndex(keyHash);
            AppendOnlyBoundedBuffer aob;
            if(this.chainingMap.containsKey(index)){
                aob = this.chainingMap.get(index);
            } else {
                LOGGER.log(DEBUG, "Cannot find an empty entry for "+this.recordBufferContext.fileName+". Creating a new chaining....\nKey: " + key + " Hash: " + keyHash);
                aob = StorageUtils.loadAppendOnlyBoundedBuffer(this.prefix, OPEN_ADDRESSING_ATTEMPTS, (int) this.recordSize, STR."\{this.recordBufferContext.fileName}_\{index}", true);
                this.chainingMap.put(index, aob);
            }
            pos = aob.nextOffset();
            aob.forwardOffset(this.recordSize);
            UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
            UNSAFE.putInt(null, pos + Header.SIZE, keyHash);
            this.doWrite(pos, record);
            this.chainedSize++;
            this.pendingFlush.add(index);
            return;
        }
        UNSAFE.putByte(null, pos, Header.ACTIVE_BYTE);
        UNSAFE.putInt(null, pos + Header.SIZE, keyHash);
        this.doWrite(pos, record);
        this.updateSize(1);
    }

    private Object[] lookupChain(IKey key, int index) {
        AppendOnlyBoundedBuffer aob = this.chainingMap.get(index);
        if (aob != null) {
            long chainedPos = aob.address;
            final long lastPos = aob.nextOffset();
            while(chainedPos < lastPos) {
                if(UNSAFE.getByte(null, chainedPos) == Header.ACTIVE_BYTE) {
                    Object[] chainedRecord = this.readFromIndex(chainedPos + Schema.RECORD_HEADER);
                    IKey existingKey = KeyUtils.buildRecordKey(this.schema().getPrimaryKeyColumns(), chainedRecord);
                    if (existingKey.equals(key)) {
                        return chainedRecord;
                    }
                }
                chainedPos = chainedPos + this.recordSize;
            }
        }
        return null;
    }

    @Override
    public void flush() {
        this.recordBufferContext.force();
        Iterator<Integer> it = this.pendingFlush.iterator();
        while(it.hasNext()){
            this.chainingMap.get(it.next()).force();
            it.remove();
        }
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return new ChainedRecordIterator(this.recordBufferContext.address, this.schema.getRecordSize(), this.capacity);
    }

    final class ChainedRecordIterator implements IRecordIterator<IKey> {

        private long nextAddress;
        private final int recordSize;

        private final long mainCapacity;
        private int mainProgress;

        private long currChainAddress;
        private long chainCapacity;
        private int chainProgress;

        private final SortedMap<Integer, AppendOnlyBoundedBuffer> chainingMapCopy;

        public ChainedRecordIterator(long address, int recordSize, long capacity) {
            this.nextAddress = address;
            this.recordSize = recordSize;
            this.mainCapacity = capacity;
            this.mainProgress = 0;
            this.currChainAddress = -1;
            this.chainingMapCopy = new TreeMap<>(chainingMap);
        }

        @Override
        public boolean hasNext() {
            // iterate main entries to check for an active bit
            while(this.mainProgress < this.mainCapacity && UNSAFE.getByte(null, this.nextAddress) != Header.ACTIVE_BYTE) {
                this.mainProgress++;
                this.nextAddress += recordSize;
            }

            // check if must setup chain address
            if(this.currChainAddress == -1) {
                int keyHash = UNSAFE.getInt(null, this.nextAddress + Header.SIZE);
                int index = getIndex(keyHash);
                AppendOnlyBoundedBuffer aob = this.chainingMapCopy.remove(index);
                if(aob != null){
                    this.chainCapacity = (aob.nextOffset() - aob.address) / this.recordSize;
                    this.currChainAddress = aob.address;
                    this.chainProgress = 0;
                    return true;
                }
            } else {
                if(this.chainProgress < this.chainCapacity) return true;
                this.currChainAddress = -1;
            }

            if(this.mainProgress < this.mainCapacity) return true;

            if(!this.chainingMapCopy.isEmpty()) {
                AppendOnlyBoundedBuffer aob = this.chainingMapCopy.remove(this.chainingMapCopy.firstKey());
                this.chainCapacity = (aob.nextOffset() - aob.address) / this.recordSize;
                this.currChainAddress = aob.address;
                this.chainProgress = 0;
                return true;
            }

            return false;
        }

        @Override
        public IKey next() {
            if(this.currChainAddress > -1) {
                SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.currChainAddress + Header.SIZE));
                this.chainProgress++;
                this.currChainAddress += this.recordSize;
                return res;
            }
            SimpleKey res = SimpleKey.of(UNSAFE.getInt(this.nextAddress + Header.SIZE));
            this.mainProgress++;
            this.nextAddress += this.recordSize;
            return res;
        }

        @Override
        public long address() {
            if(this.currChainAddress > -1) {
                return this.currChainAddress;
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
        this.chainedSize = 0;
    }

    @Override
    public int size() {
        return this.size + this.chainedSize;
    }

}
