package dk.ku.di.dms.vms.coordinator.options;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;

/**
 * Rules that govern how a coordinator must behave.
 * Values initialized with default parameters.
 */
public final class CoordinatorOptions {

    private static final int NUM_CPUS = Runtime.getRuntime().availableProcessors();

    // the batch window. a second by default
    private int batchWindow = 1000;

    private int maxTransactionsPerBatch = Integer.MAX_VALUE;

    // thread pool to execute tasks, e.g., batch replication to replicas
    private int numWorkersPerVms = 1;

    private int networkBufferSize = MemoryUtils.DEFAULT_PAGE_SIZE;

    private int osBufferSize = 0;

    private int networkSendTimeout = 1000;

    /**
     * thread pool for handling network events.
     * default is number of cores divided by 2
     */
    private int networkThreadPoolSize = NUM_CPUS;

    private int numTransactionWorkers = 1;

    private int maxSleep = 0;

    private int numQueuesVmsWorker = 1;

    private boolean compression = false;

    private boolean logging = false;

    public int getNetworkThreadPoolSize() {
        return this.networkThreadPoolSize;
    }

    public CoordinatorOptions withNetworkThreadPoolSize(int networkThreadPoolSize) {
        this.networkThreadPoolSize = networkThreadPoolSize;
        return this;
    }

    public CoordinatorOptions withNetworkBufferSize(int networkBufferSize) {
        if(networkBufferSize > 0)
            this.networkBufferSize = networkBufferSize;
        return this;
    }

    public int getOsBufferSize(){
        return this.osBufferSize;
    }

    public CoordinatorOptions withOsBufferSize(int osBufferSize) {
        this.osBufferSize = osBufferSize;
        return this;
    }

    public int getNetworkBufferSize(){
        return this.networkBufferSize;
    }

    public CoordinatorOptions withNetworkSendTimeout(int networkSendTimeout) {
        this.networkSendTimeout = networkSendTimeout;
        return this;
    }

    public int getNetworkSendTimeout(){
        return this.networkSendTimeout;
    }

    public CoordinatorOptions withNumWorkersPerVms(int numWorkersPerVms){
        this.numWorkersPerVms = numWorkersPerVms;
        return this;
    }

    public int getNumWorkersPerVms() {
        return this.numWorkersPerVms;
    }

    public int getBatchWindow() {
        return this.batchWindow;
    }

    public CoordinatorOptions withBatchWindow(int batchWindow) {
        this.batchWindow = batchWindow;
        return this;
    }

    public CoordinatorOptions withMaxTransactionsPerBatch(int maxTransactionsPerBatch) {
        this.maxTransactionsPerBatch = maxTransactionsPerBatch;
        return this;
    }

    public int getMaxTransactionsPerBatch() {
        return this.maxTransactionsPerBatch;
    }

    public CoordinatorOptions withNumTransactionWorkers(int numTransactionWorkers) {
        this.numTransactionWorkers = numTransactionWorkers;
        return this;
    }

    public int getNumTransactionWorkers() {
        return this.numTransactionWorkers;
    }

    public int getNumQueuesVmsWorker() {
        return this.numQueuesVmsWorker;
    }

    public CoordinatorOptions withNumQueuesVmsWorker(int numQueuesVmsWorker) {
        this.numQueuesVmsWorker = numQueuesVmsWorker;
        return this;
    }

    public CoordinatorOptions withMaxVmsWorkerSleep(int maxSleep) {
        this.maxSleep = maxSleep;
        return this;
    }

    public int getMaxVmsWorkerSleep() {
        return this.maxSleep;
    }

    public boolean logging() {
        return this.logging;
    }

    public CoordinatorOptions withLogging(boolean value) {
        this.logging = value;
        return this;
    }

    public CoordinatorOptions withCompression(boolean value) {
        this.compression = value;
        return this;
    }

    public boolean isCompressing() {
        return this.compression;
    }
}
