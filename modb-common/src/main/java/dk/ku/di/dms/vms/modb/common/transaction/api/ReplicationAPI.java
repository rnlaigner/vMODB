package dk.ku.di.dms.vms.modb.common.transaction.api;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;

import java.util.List;
import java.util.Map;

/**
 * The event handler carries an instance of this interface to
 * communicate with the transaction manager
 */
public interface ReplicationAPI {

    /**
     * Used by the Event Handler
     * -
     * Mark a table to be replicated
     */
    boolean addSubscription(String table);

    /**
     * Discharge a table from being replicated
     */
    boolean removeSubscription(String table);

    /**
     * Used by the Scheduler to make sure the
     * precedence updates of replicated tables
     * are applied previously to a transaction
     * task execution
     */
    void applyUpdates(Map<String, List<TransactionWrite>> updates);

}
