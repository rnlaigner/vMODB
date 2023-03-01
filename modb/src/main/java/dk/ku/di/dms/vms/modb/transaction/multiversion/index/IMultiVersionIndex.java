package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;

import java.util.List;

public interface IMultiVersionIndex extends ReadOnlyIndex<IKey> {

    void undoTransactionWrite(IKey key);

    /*
     * need to erase the transactions that are not seen by any more new transactions
     * it is performed during checkpoint
     */
    void installWrites();

    boolean insert(IKey key, Object[] record);

    boolean update(IKey key, Object[] record);

    boolean delete(IKey key);

}
