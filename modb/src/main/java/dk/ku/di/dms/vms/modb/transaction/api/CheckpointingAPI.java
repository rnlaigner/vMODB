package dk.ku.di.dms.vms.modb.transaction.api;

/**
 * Interface to which client classes
 * (e.g. event handler)
 * can request a state checkpoint
 */
public interface CheckpointingAPI {

    void checkpoint();

}
