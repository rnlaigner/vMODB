package dk.ku.di.dms.vms.event;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// TODO the idea in the future is to control the pace on
//  which we read from the message broker through configuring this class
//  the executor reads events in a pace different from what is requested by clients

/**
 * This class has the responsibility to ingest events from the external world
 * and delivering it reliably to virtual microservice executor.
 *
 * Responsibilities such as committing/acknowledging offsets and
 * making sure events received are durably stored prior to virtual
 * microservice execution are part of this class.
 *
 * @param <T> where T is the type read from the messaging system
 */
public abstract class EventHandler<T extends IEvent> {

    // TODO can use condition
    // https://baptiste-wicht.com/posts/2010/09/java-concurrency-part-5-monitors-locks-and-conditions.html
    // https://examples.javacodegeeks.com/core-java/util/concurrent/locks-concurrent/condition/java-util-concurrent-locks-condition-example/

    // TODO later on, one for each queue..
    //  then it becomes a map of queues,
    //  where the key is the queue id
    // Queue of requests to dispatch for vms execution
    // https://www.baeldung.com/java-queue-linkedblocking-concurrentlinked
    final protected Queue<IEvent> toDispatch;

    // final protected VirtualMicroserviceExecutor vms;

    // mapping of event string to an event type
    // final protected Map<String,Event> applicationEvents;

    // should be another type other than string... string is for simplicity now
    final protected Map<IEvent,String> fromOutputEventToQueueMap;

    // For queuing requests
    final private ExecutorService queueExecutor;

    final private EventRepository eventRepository;

    // TODO properties should be read from external file
    public EventHandler(
            final EventRepository eventRepository,
            final Map<IEvent,String> fromOutputEventToQueueMap){
        this.eventRepository = eventRepository;
        this.fromOutputEventToQueueMap = fromOutputEventToQueueMap;
        this.toDispatch = new ConcurrentLinkedQueue<>();
        // TODO adjust through configuration properties
        this.queueExecutor = Executors.newFixedThreadPool(2);
    }

    public void Init(){
        Dequeuer dequeuer = new Dequeuer();
        queueExecutor.submit(dequeuer);
        Enqueuer enqueuer = new Enqueuer();
        queueExecutor.submit(enqueuer);
    }

    // one thread to read from redis queue and one thread to queue to vms

    protected abstract void dequeue();
    protected abstract void queue(IEvent event, String queue);

    private class Dequeuer implements Runnable {
        @Override
        public void run() {
            // TODO for each queue
            while(true){
                dequeue();
                while(!toDispatch.isEmpty()) {
                    IEvent event = toDispatch.poll();
                    eventRepository.inputQueue.add(event);
                }
            }
        }
    }

    private class Enqueuer implements Runnable {
        @Override
        public void run() {
            // TODO for each queue
            while(true){
                // for each output event generated by the vms
                IEvent event = eventRepository.outputQueue.poll();
                if(event != null) {
                    String queue = fromOutputEventToQueueMap.get(event);
                    queue(event, queue);
                }
            }
        }
    }

}
