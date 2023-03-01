package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.common.data_structure.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionWrite;
import dk.ku.di.dms.vms.modb.common.transaction.api.ReplicationAPI;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.*;
import dk.ku.di.dms.vms.sdk.core.scheduler.tracking.ComplexVmsTransactionTrackingContext;
import dk.ku.di.dms.vms.sdk.core.scheduler.tracking.IVmsTransactionTrackingContext;
import dk.ku.di.dms.vms.sdk.core.scheduler.tracking.SimpleVmsTransactionTrackingContext;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;

/**
 * The brain of the virtual microservice runtime
 * It is agnostic to batch. It only deals with transactions and their precedence
 * It is also agnostic to serialization and deserialization of objects
 * It consumes events, verifies whether a data operation is ready for execution
 * and dispatches them for execution. If an operation is not ready yet, given the
 * payload dependencies, it is stored in a waiting list until pending events arrive
 * TODO abort management
 * must store submitted tasks in case we need to re-execute (iff not PK, FK).
 *     what could go wrong?
 *         (i) a constraint not being met, would need to abort
 *         (ii) lack of machine resources. can we do something in this case?
 */
public final class VmsTransactionScheduler extends StoppableRunnable {

    private static final Logger logger = LoggerFactory.getLogger(VmsTransactionScheduler.class);

    // payload that cannot execute because some dependence need to be fulfilled
    // payload A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    private final Map<Long, List<VmsTransactionTask>> waitingTasksPerTidMap;

    private final Map<Long, IVmsTransactionTrackingContext> transactionContextMap;

    // offset tracking for execution
    // setting it volatile does not solve non-deterministic problem
    private OffsetTracker currentOffset;

    // offset tracking. i.e., cannot issue a task if predecessor transaction is not ready yet
    private final Map<Long, OffsetTracker> offsetMap;

    // map the last tid
    private final Map<Long, Long> lastTidToTidMap;

    /**
     * Thread pool for read-only queries
     */
    private final ExecutorService readTaskPool;

    /**
     * Thread pool for write-only and read-write queries
     */
    private final ExecutorService writeTaskPool;

    private final IVmsInternalChannels vmsChannels;

    // mapping event input to transaction metadata
    private final Map<String, VmsTransactionMetadata> transactionMetadataMap;

    // reuse same collection to avoid many allocations
    private final Collection<InboundEvent> inputEvents;

    private final ReplicationAPI replicationAPI;

    private final List<ISchedulerHandler> decorators;

    public VmsTransactionScheduler(ExecutorService readTaskPool,
                                   IVmsInternalChannels vmsChannels,
                                   // (input) queue to transaction metadata map
                                   Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                   // input events may carry replicated items that must be applied before transaction start
                                   ReplicationAPI replicationAPI,
                                   List<ISchedulerHandler> decorators){
        super();

        // thread pools
        this.readTaskPool = readTaskPool;
        this.writeTaskPool = Executors.newSingleThreadExecutor();

        // infra (come from external)
        this.transactionMetadataMap = transactionMetadataMap;
        this.vmsChannels = vmsChannels;

        // operational (internal control of transactions and tasks)
        this.waitingTasksPerTidMap = new HashMap<>();
        this.transactionContextMap = new HashMap<>();
        this.offsetMap = new HashMap<>();
        this.lastTidToTidMap = new HashMap<>();

        this.inputEvents = new ArrayList<>(50);
        this.replicationAPI = replicationAPI == null ? new ConsoleOutReplicationAPI() : replicationAPI;

        this.decorators = decorators == null ? Collections.emptyList() : decorators;
    }

    private static final class ConsoleOutReplicationAPI implements ReplicationAPI {

        @Override
        public boolean addSubscription(String table) {
            return false;
        }

        @Override
        public boolean removeSubscription(String table) {
            return false;
        }

        @Override
        public void applyUpdates(Map<String, List<TransactionWrite>> updates) {

        }
    }

    /**
     * Another way to implement this is make this a fine-grained task.
     * that is, a pool of available tasks for receiving and processing the
     * events instead of an infinite while loop.
     * virtual threads are a good choice: <a href="https://jdk.java.net/loom/">...</a>
     * However, "Virtual threads help to improve the throughput of typical
     * server applications precisely because such applications consist
     * of a great number of concurrent tasks that spend much of their time waiting."
     * Which is not this case... we are not doing I/O to wait
     * But virtual threads can be beneficial to transactional tasks
     * ---------------------
     * Tasks are dispatched respecting the single-thread model for RW tasks
     * In other words, no RW tasks for the same transaction can be scheduled
     * concurrently. One at a time. Read tasks can be scheduled concurrently.
     * To avoid interleaving between tasks of the same transaction,
     * a simple strategy is now adopted:
     * (i) All read tasks are executed first. Bad for throughput
     * (ii) Look at the prescribed precedences provided by the user. Also, bad for throughput.
     * (iii) Concurrency control with abort and restarts in case of deadlock.
     *          A read transaction may be allowed to see writes installed by different tasks.
     * (iv) Have assumptions. The user may have different tasks. ACID is only provided if the
     *      tasks do not see each other writes.
     *      In other words, no concurrency control for tasks of the same TID.
     *
     */
    @Override
    public void run() {

        logger.info("Scheduler has started");

        this.initializeOffset();

        while(isRunning()) {
            try{
                this.checkForNewEvents();
                this.executeReadyTasks();
                this.processTaskResult();
                this.moveOffsetPointerIfNecessary();
                // decorates may run after an offset forwards
                this.decorate();
            } catch(Exception e){
                logger.warn("Error on scheduler loop: "+e.getMessage());
            }
        }
    }

    private void decorate() throws ExecutionException, InterruptedException {
        // blocking for now. in the future, more semantics (e.g., no wait) may be necessary
        for(ISchedulerHandler decorator : this.decorators) {
            if (decorator.conditionHolds()) {
                decorator.run().get();
            }
        }
    }

    private void executeReadyTasks() {
        IVmsTransactionTrackingContext txContext = this.transactionContextMap.get( currentOffset.tid() );

        // no event arrived yet for the current tid
        if(txContext == null) return;

        // check if the tasks are available (i.e., are inputs completed?)
        if(txContext.isSimple() && txContext.asSimple().task.isReady()){
            this.dispatchReadySimpleTask(txContext.asSimple());
        } else {
            this.dispatchReadyComplexTasks(txContext.asComplex());
        }

    }

    private void initializeOffset(){
        this.currentOffset = new OffsetTracker(0, 1);
        this.currentOffset.signalTaskFinished();
        this.offsetMap.put(0L, this.currentOffset);
        logger.info("Offset initialized");
    }

    /**
     * an idea to optimize is to pass a completion handler to the thread
     * the task thread then update the task result list
     * TODO we have to deal with failures
     *  not only container failures but also constraints being violated
     */
    private void processTaskResult() {

        IVmsTransactionTrackingContext context = this.transactionContextMap.get( this.currentOffset.tid() );
        if( context == null ) return;

        if(context.isSimple()){
            if(context.asSimple().future == null) return;
            if(!context.asSimple().future.isDone()) return;
            try {
                context.asSimple().result = context.asSimple().future.get();
                context.asSimple().future = null;
                if(context.asSimple().result.status() != VmsTransactionTaskResult.Status.SUCCESS) {
                    this.currentOffset.signalError();
                    return;
                }
                this.currentOffset.signalTaskFinished();
                this.vmsChannels.transactionOutputQueue().add(
                        new VmsTransactionResult(this.currentOffset.tid(),
                                List.of(context.asSimple().result.result())) );
                this.transactionContextMap.remove(this.currentOffset.tid());
            } catch (Exception e){
                logger.warn("A task supposedly done returned an exception: "+e.getMessage());
            }
            return;
        }

        ComplexVmsTransactionTrackingContext txCtx = context.asComplex();
        if(txCtx.submittedTasks.isEmpty()) return;
        List<Future<VmsTransactionTaskResult>> list = txCtx.submittedTasks;

        for(int i = list.size() - 1; i >= 0; --i){
            Future<VmsTransactionTaskResult> resultFuture = list.get(i);
            if(resultFuture == null) continue;
            if(!resultFuture.isDone()) continue;
            VmsTransactionTaskResult res;
            try {
                res = resultFuture.get();
                txCtx.resultTasks.add(res);

                if(res.status() == VmsTransactionTaskResult.Status.SUCCESS) {
                    this.currentOffset.signalTaskFinished();
                    list.remove(i);

                    if (this.currentOffset.status() == OffsetTracker.OffsetStatus.FINISHED_SUCCESSFULLY) {

                        List<OutboundEventResult> outbounds = new ArrayList<>(txCtx.resultTasks.size());
                        for(var resultTask : txCtx.resultTasks) {
                            outbounds.add(resultTask.result());
                        }

                        // TODO for the write/read-write tasks, just leave the last updated version of each record

                        // now can send all to output queue
                        this.vmsChannels.transactionOutputQueue().add(
                                new VmsTransactionResult(this.currentOffset.tid(), outbounds) );

                        this.transactionContextMap.remove(this.currentOffset.tid());
                    }

                } else {
                    this.currentOffset.signalError();
                    // TODO must deal with errors (i.e., abort)
                }

            } catch (InterruptedException | ExecutionException e) {
                logger.warn("A task supposedly done returned an exception: "+e.getMessage());
            }

        }

    }

    /**
     * If an additional semantic is required for the scheduler (e.g., cannot block because another type
     * of event apart from transaction input might arrive such as batch),
     * this method needs to be overridden and the first IF block removed
     */
    private void checkForNewEvents() {

        // a safe condition to block waiting is when the current offset is finished (no result tasks to be processed)
        // however, cannot block waiting for input queue because of an unknown bug
        if(this.vmsChannels.transactionInputQueue().isEmpty()){
            return;
        }

        /* because of an unknown reason, after event handler stop, this queue still output an already consumed event
        if(this.vmsChannels.transactionInputQueue().size() == 1) {
            this.processNewEvent(this.vmsChannels.transactionInputQueue().take());
            return;
        }
         */

        this.vmsChannels.transactionInputQueue().drainTo(this.inputEvents);

        for(InboundEvent input : this.inputEvents){
            this.processNewEvent(input);
        }

        // clear previous round
        this.inputEvents.clear();

    }

    private void processNewEvent(InboundEvent inboundEvent){
        // have I created the task already?
        // in other words, a previous payload for the same tid have been processed?
        if(this.waitingTasksPerTidMap.containsKey(inboundEvent.tid())){
            this.processNewEventFromKnownTransaction(inboundEvent);
        } else if (this.offsetMap.get(inboundEvent.tid()) == null) {
            this.processNewEventFromUnknownTransaction(inboundEvent);
        } else {
            logger.warn("Queue '" + inboundEvent.event() + "' Batch: " + inboundEvent.batch() + " TID: " + inboundEvent.tid());
        }
    }

    private void processNewEventFromUnknownTransaction(InboundEvent inboundEvent) {
        // new tid: create it and put it in the payload list

        VmsTransactionMetadata transactionMetadata = this.transactionMetadataMap.get(inboundEvent.event());

        // create the offset tracker
        OffsetTracker offsetTracker = new OffsetTracker(inboundEvent.tid(), transactionMetadata.signatures.size());
        this.offsetMap.put( inboundEvent.tid(), offsetTracker );

        // mark the last tid, so we can get the next to execute when appropriate
        this.lastTidToTidMap.put( inboundEvent.lastTid(), inboundEvent.tid() );

        // single task transaction
        if(transactionMetadata.signatures.size() == 1){
            IdentifiableNode<VmsTransactionSignature> node = transactionMetadata.signatures.get(0);
            VmsTransactionTask task = new VmsTransactionTask(
                    inboundEvent.tid(),
                    inboundEvent.batch(),
                    node.object(),
                    node.object().inputQueues().length
            );
            // put the input event on the correct slot (i.e., the correct parameter position)
            task.putEventInput(node.id(), inboundEvent.input());
            SimpleVmsTransactionTrackingContext txContext = new SimpleVmsTransactionTrackingContext(task, inboundEvent.updates());
            this.transactionContextMap.put( inboundEvent.tid(), txContext );
            if(!task.isReady()){
                // unknown transaction, then must create the entry
                this.waitingTasksPerTidMap.computeIfAbsent(task.tid(),
                        (x) -> new ArrayList<>(transactionMetadata.numTasksWithMoreThanOneInput)).add( task );
            }
        } else {
            // create the vms transaction context TODO check why ms2 not detecting write tx number
            ComplexVmsTransactionTrackingContext txContext = new ComplexVmsTransactionTrackingContext(
                    transactionMetadata.numReadTasks,
                    transactionMetadata.numReadWriteTasks,
                    transactionMetadata.numWriteTasks);

            // read tasks with updates must become writes by default because of concurrency control
            if(!inboundEvent.updates().isEmpty()){
                txContext.updates.putAll( inboundEvent.updates() );
            }

            // for each signature, create an appropriate task to run
            for (IdentifiableNode<VmsTransactionSignature> node : transactionMetadata.signatures) {
                VmsTransactionSignature signature = node.object();
                VmsTransactionTask task = new VmsTransactionTask(
                        inboundEvent.tid(),
                        inboundEvent.batch(),
                        node.object(),
                        signature.inputQueues().length
                );

                // put the input event on the correct slot (i.e., the correct parameter position)
                task.putEventInput(node.id(), inboundEvent.input());

                if(!task.isReady()){
                    // unknown transaction, then must create the entry
                    this.waitingTasksPerTidMap.computeIfAbsent(task.tid(),
                            (x) -> new ArrayList<>(transactionMetadata.numTasksWithMoreThanOneInput)).add( task );
                } else {
                    task.setIdentifier(txContext.asComplex().readAndIncrementNextTaskIdentifier());
                    if(node.object().transactionType() == R){
                        txContext.asComplex().readTasks.add(task);
                    } else {
                        txContext.asComplex().writeTasks.add(task);
                    }
                }
            }

            this.transactionContextMap.put( inboundEvent.tid(), txContext );

        }

    }

    private void processNewEventFromKnownTransaction(InboundEvent inboundEvent) {
        List<VmsTransactionTask> notReadyTasks = this.waitingTasksPerTidMap.get( inboundEvent.tid() );

        VmsTransactionTask task;
        for( int i = notReadyTasks.size() - 1; i >= 0; i-- ){

            task = notReadyTasks.get(i);

            // based on the input queue, find the position based on the transaction signature
            boolean found = false;
            int j;
            for(j = 0; j < task.signature().inputQueues().length; j++){
                if(task.signature().inputQueues()[j].equalsIgnoreCase(inboundEvent.event())){
                    found = true;
                    break;
                }
            }

            if(!found){
                throw new IllegalStateException("Input event not mapped correctly.");
            }

            task.putEventInput( j, inboundEvent.input() );
            if(task.isReady()){
                notReadyTasks.remove(i);
                var txCtx = this.transactionContextMap.get( inboundEvent.tid() );
                if(!txCtx.isSimple()){
                    task.setIdentifier(txCtx.asComplex().readAndIncrementNextTaskIdentifier());
                    if(task.transactionType() == R){
                        txCtx.asComplex().readTasks.add(task);
                    } else {
                        txCtx.asComplex().writeTasks.add(task);
                    }
                }
            }

        }

    }

    private void dispatchReadyReadTaskList(ComplexVmsTransactionTrackingContext txCtx){
        if(!txCtx.updates.isEmpty()){
            this.replicationAPI.applyUpdates(txCtx.updates);
        }
        int index = txCtx.readTasks.size() - 1;
        while (index >= 0) {
            VmsTransactionTask task = txCtx.readTasks.get(index);
            // later, we may have precedence between tasks of the same tid
            // i.e., right now any ordering is fine
            txCtx.submittedTasks.add( this.readTaskPool.submit(task) );
            txCtx.readTasks.remove(index);
            index--;
        }
    }

    private void dispatchReadySimpleTask(SimpleVmsTransactionTrackingContext txCtx){
        if(!txCtx.updates.isEmpty()){
            this.replicationAPI.applyUpdates(txCtx.updates);
        }
        if(txCtx.task.transactionType() == R){
            txCtx.future = this.readTaskPool.submit(txCtx.task);
        } else {
            txCtx.future = this.writeTaskPool.submit(txCtx.task);
        }
    }

    private void dispatchReadyComplexTasks(ComplexVmsTransactionTrackingContext txCtx) {
        int numRead = txCtx.readTasks.size();
        if(numRead > 0) this.dispatchReadyReadTaskList( txCtx );

        if(!txCtx.writeTasks.isEmpty()) {
            // make sure already arrived updates are applied before task execution
            if(!txCtx.updates.isEmpty()){
                this.replicationAPI.applyUpdates(txCtx.updates);
            }
            txCtx.submittedTasks.add( this.writeTaskPool.submit( txCtx.writeTasks.poll() ) );
        }
    }

    /**
     * Assumption: we always have at least one offset in the list. Of course,
     * I could do this by design but the code guarantees that
     * Is it safe to move the offset pointer? this method takes care of that
     */
    private void moveOffsetPointerIfNecessary(){

        // if next is the right one ---> the concept of "next" may change according to recovery from failures and aborts
        if(this.currentOffset.status() == OffsetTracker.OffsetStatus.FINISHED_SUCCESSFULLY
                && this.lastTidToTidMap.get( this.currentOffset.tid() ) != null ){

            var nextTid = this.offsetMap.get( this.lastTidToTidMap.get( this.currentOffset.tid() ) );

            // has the "next" arrived already?
            if(nextTid == null) return;

            // should be here to remove the tid 0. the tid 0 never receives a result task
            this.offsetMap.remove( this.currentOffset.tid() );

            // don't need anymore
            this.lastTidToTidMap.remove( this.currentOffset.tid() );

            this.waitingTasksPerTidMap.remove( this.currentOffset.tid() );

            this.currentOffset = nextTid;

            this.currentOffset.signalScheduled();

        }

    }

}
