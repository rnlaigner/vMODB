package dk.ku.di.dms.vms.modb.transaction.multiversion.pk;

import java.util.concurrent.atomic.AtomicLong;

public class LongPrimaryKeyGenerator implements IPrimaryKeyGenerator<Long> {

    private final AtomicLong sequencer;

    public LongPrimaryKeyGenerator() {
        this.sequencer = new AtomicLong(1);
    }

    @Override
    public Long next() {
        return this.sequencer.getAndAdd(1L);
    }

}
