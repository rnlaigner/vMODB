package dk.ku.di.dms.vms.tpcc.proxy.workload;

import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.lang.System.Logger.Level.*;

final class Worker {

    private static final System.Logger LOGGER = System.getLogger(WorkloadUtils.class.getName());

    public static Map<Long, List<Long>> run(CountDownLatch allThreadsStart, CountDownLatch allThreadsAreDone, Semaphore semaphore, Tuple<Integer, String>[] txRatio, Map<String, Iterator<Object>> input, Function<Object, Long> inputResolverFunc, int runTime, int pipelineSize) {
        Map<Long, List<Long>> startTsMap = new HashMap<>();
        Map<String, Integer> histogram = new HashMap<>(txRatio.length);
        for (Tuple<Integer, String> integerStringTuple : txRatio) {
            histogram.put(integerStringTuple.t2, 0);
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long threadId = Thread.currentThread().threadId();
        LOGGER.log(INFO, "Worker run (Thread ID) " + threadId + " started");
        allThreadsStart.countDown();
        try {
            allThreadsStart.await();
        } catch (InterruptedException e) {
            LOGGER.log(ERROR, "Worker (Thread ID) " + threadId + " failed to await start");
            throw new RuntimeException(e);
        }
        String tx = null;
        int ratio;
        int numSubmitted = 0;
        long elapsedTime;
        final long initTs = System.currentTimeMillis();
        long currentTs = initTs;
        do {

            try {
                ratio = random.nextInt(1, 101);
                for (Tuple<Integer, String> txEntry : txRatio) {
                    if (ratio <= txEntry.t1) {
                        tx = txEntry.t2;
                        break;
                    }
                }

                if (!input.get(tx).hasNext()) {
                    LOGGER.log(WARNING, "Worker (Thread ID) " + threadId + ": Not enough transaction inputs for: " + tx + ". Closing submission loop earlier...");
                    break;
                }
                // rough estimate of the batch ID
                long batchId = inputResolverFunc.apply(input.get(tx).next());
                startTsMap.computeIfAbsent(batchId, _ -> new ArrayList<>()).add(currentTs);
                histogram.computeIfPresent(tx, (_, v) -> v + 1);
                /* only for local tests
                if(histogram.get(tx) == 200_000) {
                    LOGGER.log(WARNING,"200K transaction inputs for: "+tx+" hit. Closing submission loop earlier...");
                    Thread.sleep(runTime - (System.currentTimeMillis() - initTs));
                    break;
                }
                 */
                currentTs = System.currentTimeMillis();
                elapsedTime = currentTs - initTs;
                if(elapsedTime >= runTime) {
                    break;
                }
                numSubmitted++;
                if(numSubmitted == pipelineSize) {
                    semaphore.tryAcquire(runTime - elapsedTime, TimeUnit.MILLISECONDS);
                    numSubmitted = 0;
                }
                tx = null;
            } catch (Exception e) {
                LOGGER.log(ERROR, "Exception in Thread ID: " + (e.getMessage() == null ? "No message" : e.getMessage()));
                throw new RuntimeException(e);
            }
        } while (true);
        LOGGER.log(INFO, "Worker run (Thread ID) " + threadId + " finished");

        StringBuilder output = new StringBuilder("Worker run (Thread ID) " + threadId + " histogram:\n");
        for (var e : histogram.entrySet()) {
            output.append(e.getKey()).append(": ").append(e.getValue()).append("\n");
        }
        System.out.println(output);

        allThreadsAreDone.countDown();
        return startTsMap;
    }
}
