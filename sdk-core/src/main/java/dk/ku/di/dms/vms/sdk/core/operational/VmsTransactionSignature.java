package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;

import java.lang.reflect.Method;

/**
 * A data class that stores the method, respective class,
 * and the name of the inbound events that trigger this method.
 * This method can be reused so the scheduler can keep track of the missing events.
 * This is part of a global cross-microservice transaction
 * <a href="https://stackoverflow.com/questions/4685563/how-to-pass-a-function-as-a-parameter-in-java">Method call</a>
 */
public final class VmsTransactionSignature {

    // the instance of a class annotated with @Microservice
    private final Object vmsInstance;

    // the method to run, i.e., the sub-transaction to be called
    private final Method method;

    // the type of the transaction, R, W, RW
    private final TransactionTypeEnum transactionType;

    private final ExecutionModeEnum executionMode;

    // only used if execution mode is partitioned
    private final Method partitionByMethod;

    // the identification of the input queues. i.e., these events must have arrived in order to execute the method
    private final String[] inputQueues;

    private final String outputQueue;

    public VmsTransactionSignature(Object vmsInstance, Method method, TransactionTypeEnum transactionType, ExecutionModeEnum executionMode, Method partitionByMethod, String[] inputQueues, String outputQueue) {
        this.vmsInstance = vmsInstance;
        this.method = method;
        this.transactionType = transactionType;
        this.executionMode = executionMode;
        this.partitionByMethod = partitionByMethod;
        this.inputQueues = inputQueues;
        this.outputQueue = outputQueue;
    }

    public VmsTransactionSignature(Object vmsInstance, Method method, TransactionTypeEnum transactionType, ExecutionModeEnum executionMode, String[] inputQueues, String outputQueue) {
        this.vmsInstance = vmsInstance;
        this.method = method;
        this.transactionType = transactionType;
        this.executionMode = executionMode;
        this.partitionByMethod = null;
        this.inputQueues = inputQueues;
        this.outputQueue = outputQueue;
    }

    public Object vmsInstance() {
        return vmsInstance;
    }

    public Method method() {
        return method;
    }

    public TransactionTypeEnum transactionType() {
        return transactionType;
    }

    public String[] inputQueues() {
        return inputQueues;
    }

    public String outputQueue() {
        return outputQueue;
    }

    public ExecutionModeEnum executionMode() {
        return executionMode;
    }

    public Method partitionByMethod() {
        return partitionByMethod;
    }

}