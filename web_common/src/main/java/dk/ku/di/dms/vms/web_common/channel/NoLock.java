package dk.ku.di.dms.vms.web_common.channel;

import java.util.concurrent.locks.ReentrantLock;

public final class NoLock extends ReentrantLock {

    @Override
    public void lock() { }

    @Override
    public void unlock() { }

}
