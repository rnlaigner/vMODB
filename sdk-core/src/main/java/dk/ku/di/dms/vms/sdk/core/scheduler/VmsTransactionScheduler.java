package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.ISchedulerCallback;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.scheduler.complex.VmsComplexTransactionScheduler;
import jdk.internal.misc.Unsafe;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static java.lang.System.Logger.Level.*;

/**
 * A transaction scheduler aware of partitioned and parallel tasks.
 * Besides, for simplicity, it only considers transactions (i.e., event inputs) that spawn a single task
 * in each VMS (in opposite of possible many tasks as found in {@link VmsComplexTransactionScheduler}).
 */
public final class VmsTransactionScheduler extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsTransactionScheduler.class.getName());

    // must be concurrent since different threads are writing and reading from it concurrently
    private final NonBlockingHashMapLong<VmsTransactionTask> transactionTaskMap;

    // map the last tid
    private final MutableLongLongMap lastTidToTidMap;

    /**
     * Thread pool for partitioned and parallel tasks
     */
    private final ExecutorService sharedTaskPool;

    private final Set<Long> parallelTasksRunning = ConcurrentHashMap.newKeySet();

    private final Set<Long> partitionedTasksRunning = ConcurrentHashMap.newKeySet();

    private volatile boolean singleThreadWriterTaskRunning = false;

    // the callback atomically updates this variable
    // used to track progress in the presence of parallel and partitioned tasks
    @SuppressWarnings("unused")
    private volatile long lastTidFinished;

    private static final Unsafe U;
    private static final long L_TID_F_OFFSET;
    static {
        U = Unsafe.getUnsafe();
        L_TID_F_OFFSET = U.objectFieldOffset(VmsTransactionScheduler.class, "lastTidFinished");
    }

    private final Set<Object> partitionKeyTrackingMap = ConcurrentHashMap.newKeySet();

    private final BlockingQueue<InboundEvent> transactionInputQueue;

    // transaction metadata mapping
    private final Map<String, VmsTransactionMetadata> transactionMetadataMap;

    // used to identify in which VMS this scheduler is running
    private final String vmsIdentifier;

    private final VmsTransactionTaskBuilder vmsTransactionTaskBuilder;

    public static VmsTransactionScheduler build(String vmsIdentifier,
                                                BlockingQueue<InboundEvent> transactionInputQueue,
                                                Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                                ITransactionManager transactionalHandler,
                                                Consumer<IVmsTransactionResult> eventHandler,
                                                int vmsThreadPoolSize){
        LOGGER.log(INFO, vmsIdentifier+ ": Building transaction scheduler with thread pool size of "+ vmsThreadPoolSize);
        return new VmsTransactionScheduler(
                vmsIdentifier,
                vmsThreadPoolSize == 0 ? ForkJoinPool.commonPool() :
                        Executors.newFixedThreadPool( vmsThreadPoolSize, Thread.ofPlatform().name("vms-task-thread").factory() ),
                transactionInputQueue,
                transactionMetadataMap,
                transactionalHandler,
                eventHandler);
    }

    private VmsTransactionScheduler(String vmsIdentifier,
                                    ExecutorService sharedTaskPool,
                                    BlockingQueue<InboundEvent> transactionInputQueue,
                                    Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                    ITransactionManager transactionalHandler,
                                    Consumer<IVmsTransactionResult> eventHandler){
        super();

        this.vmsIdentifier = vmsIdentifier;
        // thread pools
        this.sharedTaskPool = sharedTaskPool;

        // infra (come from external)
        this.transactionMetadataMap = transactionMetadataMap;
        this.transactionInputQueue = transactionInputQueue;

        // operational (internal control of transactions and tasks)
        this.transactionTaskMap = new NonBlockingHashMapLong<>(2048*10);
        SchedulerCallback callback = new SchedulerCallback(eventHandler);
        this.vmsTransactionTaskBuilder = new VmsTransactionTaskBuilder(transactionalHandler, callback);
        this.transactionTaskMap.put( 0L, this.vmsTransactionTaskBuilder.buildFinished(0) );
        this.lastTidToTidMap = new LongLongHashMap(2048*10);
    }

    /**
     * Inspired by <a href="https://stackoverflow.com/questions/826212/java-executors-how-to-be-notified-without-blocking-when-a-task-completes">link</a>,
     * This method can block on checkForNewEvents, leaving the task threads itself, via callback, modify
     * the class state appropriately. Care must be taken with some variables.
     */
    @Override
    public void run() {
        LOGGER.log(INFO,this.vmsIdentifier+": Transaction scheduler has started");
        while(this.isRunning()) {
            try {
                this.checkForNewEvents();
                this.executeReadyTasks();
            } catch(Exception e){
                e.printStackTrace(System.out);
                LOGGER.log(ERROR, this.vmsIdentifier+": Error on scheduler loop: "+(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
            }
        }
        LOGGER.log(INFO,this.vmsIdentifier+": Transaction scheduler has terminated");
    }

    private final class SchedulerCallback implements ISchedulerCallback, Thread.UncaughtExceptionHandler {

        private final Consumer<IVmsTransactionResult> eventHandler;

        private SchedulerCallback(Consumer<IVmsTransactionResult> eventHandler) {
            this.eventHandler = eventHandler;
        }

        @Override
        public void success(ExecutionModeEnum executionMode, OutboundEventResult outboundEventResult) {
            // do not remove right now because a new task to be scheduled may never find the entry if it is deleted in the scheduling loop
            VmsTransactionTask task = transactionTaskMap.get(outboundEventResult.tid());
            // signal finished first to avoid confusion (i.e., removing an "in progress" tid from the taskRunning sets)
            task.signalFinished();
            this.updateSchedulerTaskStats(executionMode, task);
            updateLastFinishedTid(outboundEventResult.tid());
            // dispatching the event later leads to lower latency and higher throughput
            this.eventHandler.accept(outboundEventResult);
        }

        @Override
        public void error(ExecutionModeEnum executionMode, long tid, Exception e) {
            // a simple mechanism to handle error is by re-executing, depending on the nature of the error
            // if constraint violation, it cannot be re-executed
            // in this case, the error must be informed to the event handler, so the event handler
            // can forward the error to downstream VMSs. if input VMS, easier to handle, just send a noop to them
            LOGGER.log(WARNING, "Error captured during application execution: \n"+e.getCause().getMessage());
            // remove from map to avoid rescheduling? no, it will lead to null pointer in scheduler loop
            VmsTransactionTask task = transactionTaskMap.get(tid);
            task.signalFailed();
            this.updateSchedulerTaskStats(executionMode, task);
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LOGGER.log(ERROR, "Uncaught exception captured during application execution: \n"+e.getCause().getMessage());
        }

        private void updateSchedulerTaskStats(ExecutionModeEnum executionMode, VmsTransactionTask task) {
            switch (executionMode){
                case SINGLE_THREADED -> singleThreadWriterTaskRunning = false;
                case PARALLEL -> parallelTasksRunning.remove(task.tid());
                case PARTITIONED -> {
                    if(!task.partitionKeys().isEmpty()){
                        for(Object partitionKey : task.partitionKeys()) {
                            if (!partitionKeyTrackingMap.remove(partitionKey)) {
                                LOGGER.log(WARNING, vmsIdentifier + ": Partitioned task " + task.tid() + " did not find its partition ID (" + partitionKey + ") in the tracking map!");
                            }
                        }
                        partitionedTasksRunning.remove(task.tid());
                        LOGGER.log(DEBUG, vmsIdentifier + ": Partitioned task " + task.tid() + " finished execution.");
                    } else {
                        singleThreadWriterTaskRunning = false;
                    }
                }
            }
        }
    }

    /**
     * This method makes sure that TIDs always increase so the next single thread tasks can be executed
     */
    private void updateLastFinishedTid(final long tid){
        long v;
        do {
            v =  this.lastTidFinished();
        } while (v < tid && !U.weakCompareAndSetLong(this, L_TID_F_OFFSET, v, tid));
    }

    /**
     * To avoid the scheduler to remain in a busy loop while no new input events arrive
     */
    private boolean mustWaitForInputEvent = false;

    private long nextTidToDelete = 0;
    private long lastSeenTidFinished = 0;

    private void executeReadyTasks() {
        long lastTidFinished_ = this.lastTidFinished();
        long nextTid = this.lastTidToTidMap.get(lastTidFinished_);
        // if nextTid == null then the scheduler must block until a new event arrive to progress
        if(nextTid == 0) {
            // keep scheduler sleeping since next tid is unknown
            this.mustWaitForInputEvent = true;

            // prevent map from growing arbitrarily
            if(lastTidFinished_ > this.lastSeenTidFinished){
                while(this.nextTidToDelete <= this.lastSeenTidFinished){
                    // will it always find it finished? no. due to concurrent execution, a "hole" may appear
                    if(!this.transactionTaskMap.get(this.nextTidToDelete).isFinished()) {
                        this.lastSeenTidFinished = this.nextTidToDelete;
                        return;
                    }
                    this.transactionTaskMap.remove(this.nextTidToDelete);
                    this.nextTidToDelete = this.lastTidToTidMap.removeKeyIfAbsent(this.nextTidToDelete, this.nextTidToDelete);
                }
                this.lastSeenTidFinished = lastTidFinished_;
            }

            return;
        }
        VmsTransactionTask task = this.transactionTaskMap.get(nextTid);
        while(true) {
            if(task == null || !task.isNew()){
                return;
            }
            switch (task.signature().executionMode()) {
                case SINGLE_THREADED -> {
                    if (!this.canSingleThreadTaskRun()) {
                        return;
                    }
                    LOGGER.log(DEBUG, this.vmsIdentifier + ": Scheduling single-threaded task for execution:\n" + task);
                    this.submitSingleThreadTaskForExecution(task);
                }
                case PARALLEL -> {
                    if (!this.canParallelTaskRun()) {
                        return;
                    }
                    this.parallelTasksRunning.add(task.tid());
                    task.signalReady();
                    LOGGER.log(DEBUG, this.vmsIdentifier + ": Scheduling parallel task for execution:\n" + task);
                    this.sharedTaskPool.submit(task);
                }
                case PARTITIONED -> {
                    if (task.partitionKeys().isEmpty()) {
                        if (this.canSingleThreadTaskRun()) {
                            LOGGER.log(DEBUG, this.vmsIdentifier + ": Task will run as single-threaded even though it is marked as partitioned:\n" + task);
                            this.submitSingleThreadTaskForExecution(task);
                        }
                        return;
                    }
                    if (!this.canPartitionedTaskRun()) {
                        return;
                    }
                    for (Object partitionKey : task.partitionKeys()) {
                        if (this.partitionKeyTrackingMap.contains(partitionKey)) return;
                    }
                    this.submitPartitionedTaskForExecution(task);
                }
            }
            // bypass the single-thread execution if possible
            if (!this.singleThreadWriterTaskRunning && this.lastTidToTidMap.containsKey(task.tid())) {
                task = this.transactionTaskMap.get(this.lastTidToTidMap.get(task.tid()));
            }
        }
    }

    private void submitPartitionedTaskForExecution(VmsTransactionTask task) {
        this.partitionKeyTrackingMap.addAll(task.partitionKeys());
        this.partitionedTasksRunning.add(task.tid());
        task.signalReady();
        LOGGER.log(DEBUG, this.vmsIdentifier+": Scheduling partitioned task for execution:\n"+ task);
        this.sharedTaskPool.submit(task);
    }

    private void submitSingleThreadTaskForExecution(VmsTransactionTask task) {
        // read-only tasks are not counted as single thread task so to prevent blocking writer tasks
        if(task.signature().transactionType() != R) this.singleThreadWriterTaskRunning = true;
        task.signalReady();
        // can the scheduler itself run it? yes and it would avoid a context switch cost
        // however, it would block the scheduler (i.e., processing inputs) until the task finishes
        this.sharedTaskPool.submit(task);
    }

    /**
     * This function assumes read-only tasks are not annotated with partition by or parallel annotations
     */
    private boolean canSingleThreadTaskRun() {
        return !this.singleThreadWriterTaskRunning &&
            (
                // this prevents possible "holes" in the termination of concurrent tasks (i.e., partitioned task with TID lower than lastTidFinished still running)
                (this.parallelTasksRunning.isEmpty() && partitionedTasksRunning.isEmpty()) ||
                (this.areAllReadOnly(this.parallelTasksRunning) && this.areAllReadOnly(this.partitionedTasksRunning))
            );
    }

    private boolean canPartitionedTaskRun(){
        return !this.singleThreadWriterTaskRunning &&
                (this.parallelTasksRunning.isEmpty() || this.areAllReadOnly(this.parallelTasksRunning));
    }

    private boolean canParallelTaskRun(){
        return !this.singleThreadWriterTaskRunning &&
                (this.partitionedTasksRunning.isEmpty() || this.areAllReadOnly(this.partitionedTasksRunning));
    }

    private boolean areAllReadOnly(Set<Long> runningSet){
        for(Long tid : runningSet){
            if(this.transactionTaskMap.get(tid).signature().transactionType() != R) return false;
        }
        return true;
    }

    private final List<InboundEvent> drained = new ArrayList<>(1024*10);

    private void checkForNewEvents() throws InterruptedException {
        InboundEvent inboundEvent;
        if(this.mustWaitForInputEvent) {
            inboundEvent = this.transactionInputQueue.take();
            // disable block
            this.mustWaitForInputEvent = false;
        } else {
            inboundEvent = this.transactionInputQueue.poll();
            if(inboundEvent == null) return;
        }
        // drain all
        this.drained.add(inboundEvent);
        this.transactionInputQueue.drainTo(this.drained);
        for(InboundEvent inboundEvent_ : this.drained){
            this.processNewEvent(inboundEvent_);
        }
        this.drained.clear();
    }

    private void processNewEvent(InboundEvent inboundEvent) {
        if (this.transactionTaskMap.containsKey(inboundEvent.tid())) {
            LOGGER.log(WARNING, this.vmsIdentifier+": Event TID has already been processed! Queue '" + inboundEvent.event() + "' Batch: " + inboundEvent.batch() + " TID: " + inboundEvent.tid());
            return;
        }
        this.transactionTaskMap.put(inboundEvent.tid(), this.vmsTransactionTaskBuilder.build(
                inboundEvent.tid(),
                inboundEvent.lastTid(),
                inboundEvent.batch(),
                this.transactionMetadataMap.get(inboundEvent.event()).signatures.getFirst().object(),
                inboundEvent.input()
        ));
        // mark the last tid, so we can get the next to execute when appropriate
        if(this.lastTidToTidMap.containsKey(inboundEvent.lastTid())){
            LOGGER.log(ERROR, this.vmsIdentifier+": Inbound event is attempting to overwrite precedence of TIDs. \nOriginal last TID:" + this.lastTidToTidMap.get(inboundEvent.lastTid()) + "\n Corrupt event:" + inboundEvent);
        } else {
            this.lastTidToTidMap.put(inboundEvent.lastTid(), inboundEvent.tid());
        }
    }

    public long lastTidFinished(){
        return U.getLongVolatile(this, L_TID_F_OFFSET);
    }

}
