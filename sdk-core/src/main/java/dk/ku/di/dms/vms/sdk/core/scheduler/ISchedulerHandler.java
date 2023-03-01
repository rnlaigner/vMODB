package dk.ku.di.dms.vms.sdk.core.scheduler;

import java.util.concurrent.Future;

/**
 * Interface to attach cross-cutting functionalities to the scheduler,
 * such as checkpointing and applying state updates prior to transaction execution
 * It follows the idea of the  <a href="https://en.wikipedia.org/wiki/Decorator_pattern">decorator</a> design pattern
 */
public interface ISchedulerHandler {

    /**
     * Ideally should run in a caller's thread
     * @return nothing
     */
    Future<?> run();

    boolean conditionHolds();

}
