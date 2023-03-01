package dk.ku.di.dms.vms.modb.common.transaction;

/**
 * This class is used differently depending on the actual deployment
 * If embedded, only one instance of this class is used across modules
 * If modb-service, then we need another mapping scheme.
 * <a href="https://stackoverflow.com/questions/21884359/java-threadlocal-vs-concurrenthashmap">...</a>
 * This is a bridge between sdk-core and sdk-embed modules, sdk-core cannot see sdk-embed.
 */
public final class TransactionMetadata {

    public static final ThreadLocal<TransactionContext> TRANSACTION_CONTEXT = new ThreadLocal<>();

    // avoid threads to get this value from their thread cache memory, force the thread to read from cpu mem space
    // as I only have one thread, I don't need synchronization. java gives before-or-after atomicity by design
    // this value is only set when all writes of the tid is written to their respective history map
    private static volatile TransactionId lastWriteTaskFinished = new TransactionId(0,0);

    // a tid might have multiple tasks
    // so a tid can be executed by two different threads
    public static void registerWriteTransactionFinish(){
        lastWriteTaskFinished = TRANSACTION_CONTEXT.get().tid;
    }

    public static TransactionContext registerTransactionStart(long tid, int identifier, boolean readOnly){
        TransactionContext txCtx = new TransactionContext(
                new TransactionId(tid, identifier),
                lastWriteTaskFinished,
                readOnly);
        TRANSACTION_CONTEXT.set( txCtx );
        return txCtx;
    }

    // tid 1 W -- executing
    // tid 2 R --- this transaction does not have any previous WRITE transaction. what is this lastWrite
    // tid 1 finished... lastWriteTaskFinished = tid 1
    // new tid 3 R. what was the last write transaction finished? tid 1, right? then tid 3 can "see" the updates of tid 1

    // guarantee for readers: no fractured reads, read from a consistent snapshot. the newer the READER, the newer the data seen
    // guarantee for writes: new writers will always see the writes from the latest writer.

}
