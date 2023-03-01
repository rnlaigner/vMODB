package dk.ku.di.dms.vms.modb.transaction.api;

import dk.ku.di.dms.vms.modb.common.transaction.api.ReplicationAPI;

/**
 * Interface that unifies all features required by a transaction manager in MODB
 * The event handler in client should not have access to the operations API
 * RepositoryFacade should only access operation API
 */
public interface TransactionManagerAPI extends OperationAPI, CheckpointingAPI, ReplicationAPI {


}
