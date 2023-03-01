package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.sdk.core.event.channel.VmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.example.InputEventExample1;
import dk.ku.di.dms.vms.sdk.core.example.MicroserviceExample2;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.facade.NetworkRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

/**
 * Test a varied set of configurations
 * to make sure the scheduler progresses
 * correctly according to the expectation.
 * Scenarios:
 * - failure
 * - simple and complex tasks
 * - submission of concurrent tasks
 * -
 * must plug a dumb storage?
 */
public class SchedulerTest {

    //
    @Test
    public void test() throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, InterruptedException {

        // what do I need to set up a vms transaction scheduler?
        ExecutorService readTaskPool = Executors.newSingleThreadExecutor();
        VmsInternalChannels vmsInternalChannels = VmsInternalChannels.getInstance();
        @SuppressWarnings("unchecked")
        Constructor<IVmsRepositoryFacade> constructor = (Constructor<IVmsRepositoryFacade>) NetworkRepositoryFacade.class.getConstructors()[0];
        VmsRuntimeMetadata vmsRuntimeMetadata = VmsMetadataLoader.load(new String[]{"dk.ku.di.dms.vms.sdk.core.example"}, constructor);

        VmsTransactionScheduler scheduler = new VmsTransactionScheduler(
                readTaskPool, vmsInternalChannels, vmsRuntimeMetadata.queueToVmsTransactionMap(), null, null);

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();

        // event producer that creates transactions simple and complex
        InputEventExample1 eventExample = new InputEventExample1(0);

        // scheduler needs to process the inbound event
        InboundEvent event = new InboundEvent(1,0,1,"in", eventExample, Collections.emptyMap());

        vmsInternalChannels.transactionInputQueue().add(event);

        // read from output queue and insert them all into scheduler again

        VmsTransactionResult out = vmsInternalChannels.transactionOutputQueue().take();

        // 1 - check if output queue contains two events
        // 2 - insert these two events in the input queue of the scheduler

        scheduler.stop(); // will stop the thread

        // tricky to simulate we have a scheduler in other microservice.... we need a new scheduler because of the tid
        // could reset the tid to 0, but would need to synchronize to avoid exceptions
        scheduler = new VmsTransactionScheduler(
                readTaskPool, vmsInternalChannels, vmsRuntimeMetadata.queueToVmsTransactionMap(), null, null);

        schedulerThread = new Thread(scheduler);
        schedulerThread.start();

        for(var res : out.resultTasks){
            // Class<?> clazz = vmsRuntimeMetadata.queueToEventMap().get( res.outputQueue() );
            InboundEvent payload_ = new InboundEvent(1,0,1, res.outputQueue(), res.output(), Collections.emptyMap());
            vmsInternalChannels.transactionInputQueue().add(payload_);
        }

        // 3 - check state of microservice 2 and see if the method was executed
        // there will be no output event since it is a void method
        sleep(2000); // just an upper bound. everything completes much earlier

        MicroserviceExample2 ms2 = (MicroserviceExample2) vmsRuntimeMetadata.loadedVmsInstances().get("dk.ku.di.dms.vms.sdk.core.example.MicroserviceExample2");

        assert ms2 != null;

        int count = ms2.getCount();

        // TODO test abort from the application. test 2

        assert count == 2;

    }

}
