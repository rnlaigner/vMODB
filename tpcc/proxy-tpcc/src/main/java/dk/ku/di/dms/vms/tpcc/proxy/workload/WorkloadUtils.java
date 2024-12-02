package dk.ku.di.dms.vms.tpcc.proxy.workload;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.nuRand;
import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.randomNumber;
import static dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants.*;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class WorkloadUtils {

    private static final System.Logger LOGGER = System.getLogger(WorkloadUtils.class.getName());

    private static final Schema SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "itemIds", "supWares", "qty", "allLocal" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.INT_ARRAY,
                    DataType.INT_ARRAY, DataType.INT_ARRAY, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static void write(long pos, Object[] record) {
        long currAddress = pos;
        for (int index = 0; index < SCHEMA.columnOffset().length; index++) {
            DataType dt = SCHEMA.columnDataType(index);
            DataTypeUtils.callWriteFunction(currAddress, dt, record[index]);
            currAddress += dt.value;
        }
    }

    private static Object[] read(long address){
        Object[] record = new Object[SCHEMA.columnOffset().length];
        long currAddress = address;
        for(int i = 0; i < SCHEMA.columnOffset().length; i++) {
            DataType dt = SCHEMA.columnDataType(i);
            record[i] = DataTypeUtils.getValue(dt, currAddress);
            currAddress += dt.value;
        }
        return record;
    }

    /**
     * @param initTs Necessary to discard batches that complete after the end of the experiment
     * @param submitted Necessary to calculate the latency, throughput, and percentiles
     */
    public record WorkloadStats(long initTs, Map<Long, List<Long>>[] submitted){}

    @SuppressWarnings("unchecked")
    public static WorkloadStats submitWorkload(List<NewOrderWareIn> input, int numWorkers, int runTime, Function<NewOrderWareIn, Long> func) {
        List<List<NewOrderWareIn>> inputLists;
        if(numWorkers > 1) {
            inputLists = partition(input, numWorkers);
        } else {
            inputLists = List.of(input);
        }

        LOGGER.log(INFO, "Submitting "+input.size()+" transactions through "+numWorkers+" worker(s)");

        CountDownLatch allThreadsStart = new CountDownLatch(numWorkers+1);
        CountDownLatch allThreadsAreDone = new CountDownLatch(numWorkers);

        Map<Long, List<Long>>[] submittedArray = new Map[numWorkers];

        for(int i = 0; i < numWorkers; i++) {
            final List<NewOrderWareIn> workerInput = inputLists.get(i);
            int finalI = i;
            Thread thread = new Thread(()-> submittedArray[finalI] =
                            Worker.run(allThreadsStart, allThreadsAreDone, workerInput, runTime, func));
            thread.start();
        }

        allThreadsStart.countDown();
        long initTs;
        try {
            allThreadsStart.await();
            initTs = System.currentTimeMillis();

            LOGGER.log(INFO,"Experiment main going to wait for the workers to finish.");

            if (!allThreadsAreDone.await(runTime * 2L, TimeUnit.MILLISECONDS)) {
                LOGGER.log(ERROR,"Latch has not reached zero. Something wrong with the worker(s)");
            } else {
                LOGGER.log(INFO,"Experiment main woke up!");
            }
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }

        return new WorkloadStats(initTs, submittedArray);
    }

    private static final class Worker {

        public static Map<Long,List<Long>> run(CountDownLatch allThreadsStart,
                                         CountDownLatch allThreadsAreDone,
                                         List<NewOrderWareIn> input, int runTime,
                                         Function<NewOrderWareIn, Long> func) {
            Map<Long,List<Long>> startTsMap = new HashMap<>();
            long threadId = Thread.currentThread().threadId();
            LOGGER.log(INFO,"Thread ID " + threadId + " started");
            int idx = 0;
            allThreadsStart.countDown();
            try {
                allThreadsStart.await();
            } catch (InterruptedException e) {
                LOGGER.log(ERROR, "Thread ID "+threadId+" failed to await start");
                throw new RuntimeException(e);
            }
            long currentTs = System.currentTimeMillis();
            long endTs = System.currentTimeMillis() + runTime;
            do {
                try {
                    long batchId = func.apply(input.get(idx));
                    if(!startTsMap.containsKey(batchId)){
                        startTsMap.put(batchId, new ArrayList<>());
                    }
                    startTsMap.get(batchId).add(currentTs);
                    idx++;
                } catch (Exception e) {
                    idx = 0;
                    /*
                    LOGGER.log(ERROR,"Exception in Thread ID: " + (e.getMessage() == null ? "No message" : e.getMessage()));
                    if(idx >= input.size()){
                        allThreadsAreDone.countDown();
                        LOGGER.log(WARNING,"Number of input events "+input.size()+" are not enough for runtime "+runTime+" ms");
                        break;
                    }
                    */
                }
                currentTs = System.currentTimeMillis();
            } while (currentTs < endTs);

            allThreadsAreDone.countDown();
            return startTsMap;
        }
    }

    private static List<List<NewOrderWareIn>> partition(List<NewOrderWareIn> input, int numWorkers){
        List<List<NewOrderWareIn>> partitions = new ArrayList<>();
        int totalSize = input.size();
        int basePartitionSize = totalSize / numWorkers;
        int remainder = totalSize % numWorkers;
        int startIndex = 0;
        for (int i = 0; i < numWorkers; i++) {
            int endIndex = startIndex + basePartitionSize + (remainder-- > 0 ? 1 : 0);
            partitions.add(new ArrayList<>(input.subList(startIndex, Math.min(endIndex, totalSize))));
            startIndex = endIndex;
        }
        return partitions;
    }

    public static List<NewOrderWareIn> loadWorkloadData(){
        AppendOnlyBuffer buffer = EmbedMetadataLoader.loadAppendOnlyBufferUnknownSize("new_order_input");
        // calculate number of entries (i.e., transactions)
        int numTransactions = (int) buffer.size() / SCHEMA.getRecordSize();
        LOGGER.log(INFO, "Starting loading "+numTransactions+" from disk...");
        long initTs = System.currentTimeMillis();
        List<NewOrderWareIn> input = new ArrayList<>(numTransactions);
        for(int txIdx = 1; txIdx <= numTransactions; txIdx++) {
            Object[] newOrderInput = read(buffer.nextOffset());
            input.add(parseRecordIntoEntity(newOrderInput));
            buffer.forwardOffset(SCHEMA.getRecordSize());
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished loading "+numTransactions+" from disk in "+(endTs-initTs)+" ms");
        return input;
    }

    public static List<NewOrderWareIn> createWorkload(int numWare, int numTransactions){
        LOGGER.log(INFO, "Starting the generation of "+numTransactions+"...");
        long initTs = System.currentTimeMillis();
        List<NewOrderWareIn> input = new ArrayList<>(numTransactions);
        AppendOnlyBuffer buffer = EmbedMetadataLoader.loadAppendOnlyBuffer(numTransactions, SCHEMA.getRecordSize(),"new_order_input", true);
        for(int txIdx = 1; txIdx <= numTransactions; txIdx++) {
            int w_id = randomNumber(1, numWare);
            Object[] newOrderInput = generateNewOrder(w_id, numWare);
            input.add(parseRecordIntoEntity(newOrderInput));
            write(buffer.nextOffset(), newOrderInput);
            buffer.forwardOffset(SCHEMA.getRecordSize());
        }
        buffer.force();
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Finished generating "+numTransactions+" in "+(endTs-initTs)+" ms");
        return input;
    }

    private static NewOrderWareIn parseRecordIntoEntity(Object[] newOrderInput) {
        return new NewOrderWareIn(
                (int) newOrderInput[0],
                (int) newOrderInput[1],
                (int) newOrderInput[2],
                (int[]) newOrderInput[3],
                (int[]) newOrderInput[4],
                (int[]) newOrderInput[5],
                (boolean) newOrderInput[6]
        );
    }

    private static Object[] generateNewOrder(int w_id, int num_ware){
        int d_id;
        int c_id;
        int ol_cnt;
        int all_local = 1;
        int not_found = NUM_ITEMS + 1;
        int rbk;

        int max_num_items_per_order_ = Math.min(MAX_NUM_ITEMS_PER_ORDER, NUM_ITEMS);
        int min_num_items_per_order_ = Math.min(5, max_num_items_per_order_);

        d_id = randomNumber(1, NUM_DIST_PER_WARE);
        c_id = nuRand(1023, 1, NUM_CUST_PER_DIST);

        ol_cnt = randomNumber(min_num_items_per_order_, max_num_items_per_order_);
        rbk = randomNumber(1, 100);

        int[] itemIds = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty = new int[ol_cnt];

        for (int i = 0; i < ol_cnt; i++) {
            int item_ = nuRand(8191, 1, NUM_ITEMS);

            // avoid duplicate items
            while(foundItem(itemIds, i, item_)){
                item_ = nuRand(8191, 1, NUM_ITEMS);
            }
            itemIds[i] = item_;

            if(FORCE_ABORTS) {
                if ((i == ol_cnt - 1) && (rbk == 1)) {
                    // this can lead to exception and then abort in app code
                    itemIds[i] = not_found;
                }
            }

            if (ALLOW_MULTI_WAREHOUSE_TX) {
                if (randomNumber(1, 100) != 1) {
                    supWares[i] = w_id;
                } else {
                    supWares[i] = otherWare(num_ware, w_id);
                    all_local = 0;
                }
            } else {
                supWares[i] = w_id;
            }
            qty[i] = randomNumber(1, 10);
        }

        return new Object[]{ w_id, d_id, c_id, itemIds, supWares, qty, all_local == 1 };
    }

    private static boolean foundItem(int[] itemIds, int length, int value){
        if(length == 0) return false;
        for(int i = 0; i < length; i++){
            if(itemIds[i] == value) return true;
        }
        return false;
    }

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Driver.java#L310">AgilData</a>
     */
    private static int otherWare(int num_ware, int home_ware) {
        int tmp;
        if (num_ware == 1) return home_ware;
        do {
            tmp = randomNumber(1, num_ware);
        } while (tmp == home_ware);
        return tmp;
    }

}
