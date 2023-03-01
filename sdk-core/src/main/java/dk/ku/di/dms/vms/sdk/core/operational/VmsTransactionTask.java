package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;

import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * A class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 */
public class VmsTransactionTask implements Callable<VmsTransactionTaskResult> {

    // this is the global tid
    private final long tid;

    private final long batch;

    // the information necessary to run the method
    private final VmsTransactionSignature signature;

    /*
     * internal identification of this specific task in the scheduler
     * used to allow atomic visibility
     * (can be later used to specify ordering criteria between tasks)
     */
    private int identifier;

    private final Object[] inputs;

    private int remainingInputs;

    public VmsTransactionTask (long tid, long batch, VmsTransactionSignature signature, int inputSize){
        this.tid = tid;
        this.batch = batch;
        this.signature = signature;
        this.inputs = new Object[inputSize];
        this.remainingInputs = inputSize;
    }

    public void putEventInput(int index, Object event){
        this.inputs[index] = event;
        this.remainingInputs--;
    }

    /**
     * Only set for write transactions.
     * @param identifier a task identifier among the tasks of a TID
     */
    public void setIdentifier(int identifier){
        this.identifier = identifier;
    }

    public TransactionTypeEnum transactionType(){
        return this.signature.transactionType();
    }

    public long tid() {
        return this.tid;
    }

    public boolean isReady(){
        return this.remainingInputs == 0;
    }

    public VmsTransactionSignature signature(){
        return this.signature;
    }

    @Override
    public VmsTransactionTaskResult call() {

        // register thread in the transaction facade
        TransactionContext txCtx = TransactionMetadata.registerTransactionStart(this.tid, this.identifier, this.signature.transactionType() == TransactionTypeEnum.R);

        try {

            // can be null, given we have terminal events (void method)
            // could also be terminal and generate event... maybe an external system wants to consume
            // then send to the leader...
            Object output = this.signature.method().invoke(this.signature.vmsInstance(), this.inputs);

            OutboundEventResult eventOutput;

            // need to move the writer tid
            if(this.signature.transactionType() == TransactionTypeEnum.W){
                eventOutput = new OutboundEventResult(this.tid, this.batch, this.signature.outputQueue(), output, txCtx.writes);
                // works like a commit, but not exactly. it serves to set the visibility of data items for future tasks
                TransactionMetadata.registerWriteTransactionFinish();
            } else {
                eventOutput = new OutboundEventResult(this.tid, this.batch, this.signature.outputQueue(), output, Collections.emptyMap());
            }

            // TODO optimize: read tasks must return a simpler object.
            //  for instance, no need for status since reads always succeed
            //  or maybe create a specific vms transaction task for read. maybe vmsReadTask and vmsWriteTask
            // then makes it easier to identify in the scheduler
            return new VmsTransactionTaskResult(
                    this.tid,
                    this.identifier,
                    eventOutput,
                    VmsTransactionTaskResult.Status.SUCCESS);


        } catch (Exception e) {
            // (i) whether to return to the scheduler or (ii) to push to the payload handler for forwarding it to the output queue
            // must go for (i) because many tasks may belong to a single transaction. so all outputs must be forwarded together
            // in case of aborts, no succeeded output will be forwarded
            return new VmsTransactionTaskResult(
                    this.tid,
                    this.identifier,
                    null,
                    VmsTransactionTaskResult.Status.FAILURE);
        }

    }

}