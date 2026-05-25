package dk.ku.di.dms.vms.modb.common.runnable;

/**
 * Abstract class that provides common features for server classes
 */
public abstract class StoppableRunnable implements Runnable {

    private volatile boolean running;

    public StoppableRunnable() {
        // starts running as default
        this.running = true;
    }

    public boolean isRunning() {
        return this.running;
    }

    public void stop() {
        this.running = false;
    }

}
