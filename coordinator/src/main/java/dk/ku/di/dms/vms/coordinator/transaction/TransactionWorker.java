package dk.ku.di.dms.vms.coordinator.transaction;

import dk.ku.di.dms.vms.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.vms.IVmsWorker;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

import static java.lang.System.Logger.Level.*;

public final class TransactionWorker extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(TransactionWorker.class.getName());

    private static final String LIVELOCK_KEY = "_livelock_";

    private final int id;

    private final Deque<TransactionInput> inputQueue;
    private long startingTidBatch;
    private long tid;
    private final int maxNumberOfTIDsBatch;
    private final int batchWindow;
    private final int numWorkers;

    private final Map<String, TransactionDAG> transactionMap;
    private final Map<String, VmsTracking[]> vmsPerTransactionMap;
    private final Map<String, VmsTracking> vmsTrackingMap;
    private final Map<String, IVmsWorker> vmsWorkerContainerMap;

    private final IVmsSerdesProxy serdesProxy;

    private BatchContext lastBatchContext;
    private BatchContext currBatchContext;
    private final TreeMap<Long, List<PendingTransactionInput>> pendingInputMap;

    private final Queue<Map<String, PrecedenceInfo>> precedenceMapInputQueue;
    private final Queue<Map<String, PrecedenceInfo>> precedenceMapOutputQueue;
    private final Map<Long, Map<String, PrecedenceInfo>> precedenceMapCache;
    private final Queue<Object> coordinatorQueue;

    private static class VmsTracking {
        public final String identifier;
        public long batch;
        public long lastTid;
        public long previousBatch;
        public int numberOfTIDsCurrentBatch;

        public VmsTracking(VmsNode vmsNode) {
            this.identifier = vmsNode.identifier;
            this.batch = vmsNode.batch;
            this.lastTid = vmsNode.lastTid;
            this.previousBatch = vmsNode.previousBatch;
            this.numberOfTIDsCurrentBatch = vmsNode.numberOfTIDsCurrentBatch;
        }
    }

    private record PendingTransactionInput (long tid, long batch, TransactionInput input, Set<String> pendingVMSs, Map<String, Long> previousTidPerVms){}

    /**
     * Build private VmsTracking objects
     */
    @SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
    public static TransactionWorker build(int id, Deque<TransactionInput> inputQueue,
                                          long startingTid, int maxNumberOfTIDsBatch,
                                          int batchWindow, int numWorkers,
                                          Queue<Map<String, PrecedenceInfo>> precedenceMapInputQueue,
                                          Queue<Map<String, PrecedenceInfo>> precedenceMapOutputQueue,
                                          Map<String, TransactionDAG> transactionMap,
                                          Map<String, VmsNode[]> vmsIdentifiersPerDAG,
                                          Map<String, IVmsWorker> vmsWorkerContainerMap,
                                          Queue<Object> coordinatorQueue,
                                          IVmsSerdesProxy serdesProxy){
        Map<String, VmsTracking> vmsTrackingMap = new HashMap<>();
        Map<String, VmsTracking[]> vmsPerTransactionMap = new HashMap<>(vmsIdentifiersPerDAG.size());
        for(var txEntry : vmsIdentifiersPerDAG.entrySet()) {
            if(txEntry.getValue() == null){
                throw new IllegalStateException("Null transaction entry for transaction id " + txEntry.getKey());
            }
            var list = new ArrayList<VmsTracking>();
            for(VmsNode vmsNode : txEntry.getValue()) {
                if(vmsNode == null) {
                    throw new IllegalStateException("Null transaction entry for transaction id " + txEntry.getKey());
                }
                if(!vmsTrackingMap.containsKey(vmsNode.identifier)){
                    vmsTrackingMap.put(vmsNode.identifier, new VmsTracking(vmsNode));
                }
                list.add(vmsTrackingMap.get(vmsNode.identifier));
            }
            vmsPerTransactionMap.put(txEntry.getKey(), list.toArray(new VmsTracking[list.size()]));
        }
        return new TransactionWorker(id, inputQueue, startingTid, maxNumberOfTIDsBatch, batchWindow, numWorkers,
                precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap,
                vmsPerTransactionMap, vmsTrackingMap, vmsWorkerContainerMap, coordinatorQueue, serdesProxy);
    }

    private TransactionWorker(int id, Deque<TransactionInput> inputQueue,
                              long startingTidBatch, int maxNumberOfTIDsBatch, int batchWindow, int numWorkers,
                              Queue<Map<String, PrecedenceInfo>> precedenceMapInputQueue,
                              Queue<Map<String, PrecedenceInfo>> precedenceMapOutputQueue,
                              Map<String, TransactionDAG> transactionMap, Map<String, VmsTracking[]> vmsPerTransactionMap,
                              Map<String, VmsTracking> vmsTrackingMap, Map<String, IVmsWorker> vmsWorkerContainerMap,
                              Queue<Object> coordinatorQueue, IVmsSerdesProxy serdesProxy){
        this.id = id;
        this.inputQueue = inputQueue;
        this.startingTidBatch = startingTidBatch;
        this.tid = startingTidBatch;
        this.maxNumberOfTIDsBatch = maxNumberOfTIDsBatch;
        this.batchWindow = batchWindow;
        this.numWorkers = numWorkers;
        this.precedenceMapInputQueue = precedenceMapInputQueue;
        this.precedenceMapOutputQueue = precedenceMapOutputQueue;
        this.transactionMap = transactionMap;
        this.vmsPerTransactionMap = vmsPerTransactionMap;
        this.vmsTrackingMap = vmsTrackingMap;
        this.vmsWorkerContainerMap = vmsWorkerContainerMap;
        this.serdesProxy = serdesProxy;

        this.pendingInputMap = new TreeMap<>();
        this.precedenceMapCache = new HashMap<>();

        // define first batch context based on data from constructor
        this.currBatchContext = new BatchContext((startingTidBatch + maxNumberOfTIDsBatch - 1) / maxNumberOfTIDsBatch);
        this.lastBatchContext = new BatchContext(0);
        this.lastBatchContext.lastTid = 0;
        this.lastBatchContext.numTIDsOverall = 0;
        this.lastBatchContext.previousBatchPerVms = new HashMap<>();
        this.lastBatchContext.numberOfTIDsPerVms = new HashMap<>();

        this.coordinatorQueue = coordinatorQueue;
    }

    @Override
    public void run() {
        LOGGER.log(INFO, "Starting transaction worker # " + this.id);
        TransactionInput data;
        long lastTidBatch;
        long end;
        while (this.isRunning()) {
            lastTidBatch = this.tid + this.maxNumberOfTIDsBatch - 1;
            end = System.currentTimeMillis() + this.batchWindow;
            do {
                // drain transaction inputs
                // inner while to avoid calling currentTimeMillis for every item
                while (this.tid <= lastTidBatch && (data = this.inputQueue.poll()) != null) {
                    // process precedence from previous worker in the ring
                    // we could do it in advance current batch, but can lead to higher wait in vms
                    this.processTransactionInput(data);
                }
            } while (this.tid <= lastTidBatch && System.currentTimeMillis() < end);

            if (this.noProgress()) {
                continue;
            }

            do {
                this.processPendingInput();
            } while(!this.advanceCurrentBatch() && this.isRunning());

            this.tid = this.getTidNextBatch();
            this.startingTidBatch = this.tid;
        }
        LOGGER.log(INFO, "Finishing transaction worker # " + this.id);
    }

    private boolean noProgress() {
        // no tid was processed in this batch
        if(this.tid == this.startingTidBatch) {
            if(this.numWorkers == 1) return true;
            // no tid was processed in the ring
            if(this.precedenceMapInputQueue.peek() == null) {
                return true;
            } else {
                // check if there has been actual progress in the ring
                Map<String, PrecedenceInfo> precedenceMap = this.precedenceMapInputQueue.peek();
                return precedenceMap.containsKey(LIVELOCK_KEY) && precedenceMap.get(LIVELOCK_KEY).lastBatch >= this.lastBatchContext.batchOffset;
            }
        }
        return false;
    }

    private long getTidNextBatch() {
        if(this.numWorkers == 1) return this.tid;
        return this.startingTidBatch + ((long) this.numWorkers * this.maxNumberOfTIDsBatch);
    }

    private static final Deque<Set<String>> PENDING_VMSES_CACHE = new ConcurrentLinkedDeque<>();
    private static final Deque<Map<String, Long>> PREVIOUS_TID_PER_VMS_CACHE = new ConcurrentLinkedDeque<>();

    private void processTransactionInput(TransactionInput transactionInput) {
        TransactionDAG transactionDAG = this.transactionMap.get( transactionInput.name );
        if(transactionDAG == null){
            throw new RuntimeException("The DAG for transaction "+transactionInput.name+" cannot be found");
        }
        EventIdentifier event = transactionDAG.inputEvents.get(transactionInput.event.name);
        if(event == null){
            throw new RuntimeException("The input event "+transactionInput.event.name+" for transaction DAG "+transactionDAG.name+" does not exist");
        }
        // get the vms
        VmsTracking inputVms = this.vmsTrackingMap.get(event.targetVms);
        VmsTracking[] vmsList = this.vmsPerTransactionMap.get(transactionDAG.name);

        // reuse hashmap
        Map<String, Long> previousTidPerVms = PREVIOUS_TID_PER_VMS_CACHE.pollFirst();
        if(previousTidPerVms == null) previousTidPerVms = new HashMap<>(vmsList.length);

        // if any vms in the dag shows a previous batch offset, then this input must be marked as pending
        // until we get the precedence from the transaction worker that precedes this one in the ring,
        // we cannot submit this input
        Set<String> pendingVMSes = PENDING_VMSES_CACHE.pollFirst();
        if(pendingVMSes == null) pendingVMSes = new HashSet<>();
        for (VmsTracking vms_ : vmsList) {
            previousTidPerVms.put(vms_.identifier, vms_.lastTid);
            if(vms_.batch != this.currBatchContext.batchOffset){
                // previous batch will be updated later, when precedence map is received from another worker
                vms_.batch = this.currBatchContext.batchOffset;
                vms_.numberOfTIDsCurrentBatch = 0;
                pendingVMSes.add(vms_.identifier);
                // can't assign last tid here since it is unknown
                // whether the previous worker has assigned a tid
                // for this VMS. must wait until precedence set
                // is received, on batch completion
            }
            vms_.lastTid = this.tid;
            vms_.numberOfTIDsCurrentBatch++;
        }
        this.currBatchContext.terminalVMSes.addAll( transactionDAG.terminalNodes );

        if(!pendingVMSes.isEmpty()) {
            this.generatePendingTransactionInput(pendingVMSes, previousTidPerVms, transactionInput);
        } else {
            String precedenceMapStr = this.serdesProxy.serializeMap(previousTidPerVms);
            TransactionEvent.PayloadRaw txEvent = TransactionEvent.of(this.tid, this.currBatchContext.batchOffset,
                    transactionInput.event.name, transactionInput.event.payload, precedenceMapStr);
            LOGGER.log(DEBUG,"Leader: Transaction worker "+id+" adding event "+event.name+" to "+inputVms.identifier+" worker:\n"+txEvent+"\n"+previousTidPerVms);
            this.vmsWorkerContainerMap.get(inputVms.identifier).queueTransactionEvent(txEvent);
            previousTidPerVms.clear();
            PREVIOUS_TID_PER_VMS_CACHE.addLast(previousTidPerVms);
        }
        this.tid++;
    }

    private void generatePendingTransactionInput(Set<String> pendingVMSs, Map<String, Long> previousTidPerVms, TransactionInput transactionInput) {
        // this has to be emitted when batch info from previous worker in the ring arrives
        long lastBatchOffset = this.currBatchContext.batchOffset - this.numWorkers;
        PendingTransactionInput pendingInput = new PendingTransactionInput(
                this.tid, this.currBatchContext.batchOffset, transactionInput, pendingVMSs, previousTidPerVms);
        this.pendingInputMap.computeIfAbsent(lastBatchOffset, _ -> new ArrayList<>()).add(pendingInput);
    }

    public static class PrecedenceInfo {
        volatile long lastTid;
        final long lastBatch;
        final long previousToLastBatch;
        public PrecedenceInfo(long lastTid, long lastBatch, long previousToLastBatch) {
            this.lastBatch = lastBatch;
            this.lastTid = lastTid;
            this.previousToLastBatch = previousToLastBatch;
        }
        public long lastTid() {
            return lastTid;
        }
        public long lastBatch() {
            return lastBatch;
        }
        public long previousToLastBatch() {
            return previousToLastBatch;
        }
        @Override
        public String toString() {
            return "{"
                    + "\"lastBatch\":" + lastBatch
                    + ",\"lastTid\":" + lastTid
                    + ",\"previousToLastBatch\":" + previousToLastBatch
                    + "}";
        }
    }

    private void processPendingInput() {
        // comes in order always
        Map<String, PrecedenceInfo> precedenceMap = this.precedenceMapInputQueue.poll();
        if(precedenceMap == null){ return; }

        LOGGER.log(DEBUG, "Tx_Worker "+id+": Received a precedence map\n"+precedenceMap);

        Map.Entry<Long, List<PendingTransactionInput>> entry = this.pendingInputMap.firstEntry();
        if(entry == null) {
            // no worker pending input
            this.precedenceMapCache.put(this.currBatchContext.batchOffset - this.numWorkers, precedenceMap);
            return;
        }

        List<PendingTransactionInput> pendingInputs = entry.getValue();
        for(PendingTransactionInput pendingInput : pendingInputs){
            // building map only those VMSs that participate in the transaction
            TransactionDAG transactionDAG = this.transactionMap.get(pendingInput.input.name);
            VmsTracking[] vmsList = this.vmsPerTransactionMap.get(transactionDAG.name);

            for (VmsTracking vms_ : vmsList) {
                PrecedenceInfo precedenceInfo = precedenceMap.get(vms_.identifier);
                if(pendingInput.pendingVMSs.contains(vms_.identifier)) {
                    pendingInput.previousTidPerVms.put(vms_.identifier, precedenceInfo.lastTid);
                }
                // update precedence info for next pending input
                precedenceInfo.lastTid = vms_.lastTid;
            }

            String precedenceMapStr = this.serdesProxy.serializeMap(pendingInput.previousTidPerVms);
            EventIdentifier event = transactionDAG.inputEvents.get(pendingInput.input.event.name);
            VmsTracking inputVms = this.vmsTrackingMap.get(event.targetVms);
            TransactionEvent.PayloadRaw txEvent = TransactionEvent.of(pendingInput.tid, pendingInput.batch,
                    pendingInput.input.event.name, pendingInput.input.event.payload, precedenceMapStr);
            LOGGER.log(DEBUG,"Leader: Transaction worker "+id+" adding event "+event.name+" to "+inputVms.identifier+" worker:\n"+txEvent+"\n"+pendingInput.previousTidPerVms);
            this.vmsWorkerContainerMap.get(inputVms.identifier).queueTransactionEvent(txEvent);

            // reuse previous tid per vms map
            pendingInput.previousTidPerVms.clear();
            PREVIOUS_TID_PER_VMS_CACHE.addLast(pendingInput.previousTidPerVms);

            // reuse pending VMSes set
            pendingInput.pendingVMSs.clear();
            PENDING_VMSES_CACHE.addLast(pendingInput.pendingVMSs);
        }

        this.pendingInputMap.remove(entry.getKey());

        // store precedenceMap for processing inside advanceCurrentBatch
        // store for batch completion time
        this.precedenceMapCache.put(entry.getKey(), precedenceMap);
    }

    private boolean advanceCurrentBatch() {
        Map<String, PrecedenceInfo> precedenceMap = this.precedenceMapCache.remove(this.currBatchContext.batchOffset - this.numWorkers);
        if(precedenceMap == null) {
            return false;
        }
        // cannot issue a batch if we don't know the last batch of each VMS in this batch
        // the last batch might have been updated by previous workers in the ring
        // update for those vms that have not participated in this batch
        // WARNING: DO NOT REUSE THESE DATA STRUCTURES SINCE THEY CAN BE ACCESSED CONCURRENTLY BY THE COORDINATOR
        Map<String, Long> previousBatchPerVms = new HashMap<>();
        Map<String, Integer> numberOfTIDsPerVms = new HashMap<>();

        for(Map.Entry<String, VmsTracking> vmsEntry : this.vmsTrackingMap.entrySet()){
            VmsTracking vms = vmsEntry.getValue();
            PrecedenceInfo precedenceInfo = precedenceMap.get(vmsEntry.getKey());
            if(precedenceInfo == null){
                LOGGER.log(ERROR, "Precedence info for "+vmsEntry.getKey()+" is null. It is not possible to update the previous batch!");
                continue;
            }
            if(vms.batch != this.currBatchContext.batchOffset){
                vms.previousBatch = precedenceInfo.previousToLastBatch;
                vms.batch = precedenceInfo.lastBatch;
                vms.lastTid = precedenceInfo.lastTid;
            } else {
                // should not update last tid here
                // only need the last batch coming from a worker for the transaction input precedence map
                vms.previousBatch = precedenceInfo.lastBatch;
                previousBatchPerVms.put(vms.identifier, vms.previousBatch);
                numberOfTIDsPerVms.put(vms.identifier, vms.numberOfTIDsCurrentBatch);
            }
            // update the same map
            precedenceMap.put(vms.identifier, new PrecedenceInfo(vms.lastTid, vms.batch, vms.previousBatch));
        }

        if(this.numWorkers > 1) {
            precedenceMap.put(LIVELOCK_KEY, new PrecedenceInfo(0, currBatchContext.batchOffset, 0));
        }
        // send batch precedence map for next worker in the ring
        this.precedenceMapOutputQueue.add(precedenceMap);
        long lastTid = this.tid == this.startingTidBatch ? this.lastBatchContext.lastTid : this.tid - 1;
        this.currBatchContext.seal(this.tid - this.startingTidBatch, lastTid, previousBatchPerVms, numberOfTIDsPerVms);
        this.coordinatorQueue.add(this.currBatchContext);

        // optimization: iterate over all vms in the last batch, filter those which last tid != this.tid
        // after filtering, send a map containing the vms (identifier) and their corresponding last TIDs to the next transaction worker in the ring
        this.lastBatchContext = this.currBatchContext;
        this.currBatchContext = new BatchContext(this.currBatchContext.batchOffset + this.numWorkers);

        return true;
    }

    public void clearInputQueue() {
        this.inputQueue.clear();
    }

}
