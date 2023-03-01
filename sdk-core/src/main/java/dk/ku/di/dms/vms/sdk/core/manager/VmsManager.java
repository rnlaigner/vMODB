package dk.ku.di.dms.vms.sdk.core.manager;

import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.event.handler.VmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.facade.NetworkRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.web_common.runnable.VmsDaemonThreadFactory;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.event.channel.VmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.*;

/**
 * Manager is a class that manages the lifecycle of components:
 * {@link VmsTransactionScheduler} {@link IVmsEventHandler}
 */
public final class VmsManager {

    private ManagerMetadata metadata;

    public VmsManager() {}

    private void doHealthCheck(){

        // TODO verify how healthy the threads are

        // the future interface is done is cancelled ...

    }

    public void init(String vmsName) {

        // does a thread has stopped suddenly?
        // do we have several events without being processed?
        // is there some problem that should be reported to the log?

        // https://stackoverflow.com/questions/1323408/get-a-list-of-all-threads-currently-running-in-java/3018672#3018672
        // https://stackoverflow.com/questions/1323408/get-a-list-of-all-threads-currently-running-in-java

        this.metadata = new ManagerMetadata();

//        int availableCPUs = Runtime.getRuntime().availableProcessors();
//        final ExecutorService executorService;
//        if(availableCPUs == 1){
//            executorService = ForkJoinPool.commonPool();
//        } else {
//            // 1 CPU is free for OS and JVM tasks
//            executorService = Executors.newFixedThreadPool(availableCPUs - 1);
//        }

        // this.metadata.executorService = executorService;
        IVmsInternalChannels vmsInternalPubSubService = VmsInternalChannels.getInstance();

        VmsRuntimeMetadata vmsMetadata;
        try {

            @SuppressWarnings("unchecked")
            Constructor<IVmsRepositoryFacade> constructor = (Constructor<IVmsRepositoryFacade>) NetworkRepositoryFacade.class.getConstructors()[0];

            vmsMetadata = VmsMetadataLoader.load(null, constructor);
        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Cannot start VMs, error loading metadata.");
        }

        // pool for socket tasks. daemon to not preventing the JVM from closing
        ExecutorService socketTaskPool = Executors.newFixedThreadPool(2, new VmsDaemonThreadFactory());

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build(  );

        this.metadata.eventHandler = new VmsEventHandler(vmsName, vmsInternalPubSubService, vmsMetadata, serdes, socketTaskPool);

        // pool for application logic tasks
        ExecutorService vmsAppLogicTaskPool = Executors.newSingleThreadExecutor();

        // scheduler
        this.metadata.scheduler = new VmsTransactionScheduler(vmsAppLogicTaskPool, vmsInternalPubSubService,
                vmsMetadata.queueToVmsTransactionMap(), null, null);

        // executor
        // this.metadata.executor = new VmsTransactionExecutor(this.metadata.internalPubSub);

//            I dont need a pool for vms event handler and scheduler. but i need a pool for socket channels and app-logic tasks

        //  schedule vms management tasks, such as heartbeat, cpu consumption, and also change configurations online depending on system resource
        // Executors.newScheduledThreadPool()

        // perhaps the app-logic threads must have higher priority?

//        this.metadata.eventHandlerThread = new Thread(this.metadata.eventHandler);
//        this.metadata.schedulerThread = new Thread(this.metadata.scheduler);

//        this.metadata.eventHandlerThread.start();

        ExecutorService vmsTasksPool = Executors.newFixedThreadPool(2, new VmsDaemonThreadFactory());

        CompletableFuture<?>[] futures = new CompletableFuture[2];

        // the developer should be sure about the semantics
        futures[0] = CompletableFuture.runAsync( this.metadata.eventHandler );
        futures[1] = CompletableFuture.runAsync( this.metadata.scheduler );

    }


}
