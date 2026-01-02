package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;

import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("rawtypes")
public final class QueueTableIterator {
    private final UniqueHashBufferIndex index;
    private final IRecordIterator<IKey> iterator;
    private final EntityHandler entityHandler;
    private final ReentrantLock lock;
    public QueueTableIterator(UniqueHashBufferIndex index, EntityHandler entityHandler){
        this.index = index;
        this.iterator = index.iterator();
        this.lock = new ReentrantLock();
        this.entityHandler = entityHandler;
    }
    public String poll(){
        this.lock.lock();
        if(this.iterator.hasNext()){
            Object[] record = this.index.record(this.iterator);
            IEntity entity = this.entityHandler.parseObjectIntoEntity(record);
            this.iterator.next();
            this.lock.unlock();
            return entity.toString();
        }
        this.lock.unlock();
        return null;
    }
}