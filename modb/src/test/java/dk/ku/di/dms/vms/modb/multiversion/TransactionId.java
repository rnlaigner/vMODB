package dk.ku.di.dms.vms.modb.multiversion;

/**
 * This was initially designed to handle different tasks of the same transaction to see writes of other tasks from same transaction
 * However, this has lost priority due to the new benchmark created, that does not require that
 * @param tid
 * @param identifier
 */
public record TransactionId ( long tid, int identifier ) implements Comparable<TransactionId> {

    @Override
    public int compareTo(TransactionId anotherTransactionId) {
        return compare(this, anotherTransactionId);
    }

    private static int compare(TransactionId x, TransactionId y) {
        return
                 ( (x.tid() > y.tid()) || (x.tid() == y.tid() && x.identifier() > y.identifier()) ) ? 1 :
                         ( (x.tid() < y.tid()) || (x.tid() == y.tid() && x.identifier() < y.identifier()) ) ? -1 :
                        0;

    }

}
