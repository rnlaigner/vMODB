package dk.ku.di.dms.vms.tpcc.proxy.workload;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBoundedBuffer;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyUnboundedBuffer;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Stream;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.nuRand;
import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.randomNumber;
import static dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants.*;
import static java.lang.System.Logger.Level.*;

public final class WorkloadUtils {

    private static final System.Logger LOGGER = System.getLogger(WorkloadUtils.class.getName());

    private static final String NEW_ORDER_INPUT_BASE_FILE_NAME = "new_order_input_";

    private static final String PAYMENT_INPUT_BASE_FILE_NAME = "payment_input_";

    private static final String ORDER_STATUS_INPUT_BASE_FILE_NAME = "order_status_input_";

    private static final Schema NEW_ORDER_SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "itemIds", "supWares", "qty", "allLocal" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.INT_ARRAY, DataType.INT_ARRAY, DataType.INT_ARRAY, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static final Schema PAYMENT_SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "c_w_id", "c_d_id", "amount", "c_last", "by_name" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.FLOAT, DataType.STRING, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static final Schema ORDER_STATUS_SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "c_last", "by_name" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.STRING, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static void writeRecordInMemoryPos(long pos, Object[] record, Schema schema) {
        long currAddress = pos;
        for (int index = 0; index < schema.columnOffset().length; index++) {
            DataType dt = schema.columnDataType(index);
            DataTypeUtils.callWriteFunction(currAddress, dt, record[index]);
            currAddress += dt.value;
        }
    }

    private static Object[] readRecordFromMemoryPos(long address, Schema schema){
        Object[] record = new Object[schema.columnOffset().length];
        long currAddress = address;
        for(int i = 0; i < schema.columnOffset().length; i++) {
            DataType dt = schema.columnDataType(i);
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
    public static WorkloadStats submitWorkload(Tuple<Integer, String>[] txRatio, List<Map<String, Iterator<Object>>> input, Function<Object, Long> func, int runtime) {
        int numWorkers = input.size();
        LOGGER.log(INFO, "Submitting transactions through "+numWorkers+" worker(s)");
        CountDownLatch allThreadsStart = new CountDownLatch(numWorkers+1);
        CountDownLatch allThreadsAreDone = new CountDownLatch(numWorkers);
        Map<Long, List<Long>>[] submittedArray = new Map[numWorkers];

        for(int i = 0; i < numWorkers; i++) {
            final Map<String, Iterator<Object>> workerInput = input.get(i);
            int finalI = i;
            Thread thread = new Thread(()-> submittedArray[finalI] =
                            Worker.run(allThreadsStart, allThreadsAreDone, txRatio, workerInput, func, runtime));
            thread.start();
        }

        allThreadsStart.countDown();
        long initTs = System.currentTimeMillis();
        try {
            allThreadsStart.await();
            LOGGER.log(INFO,"Experiment main going to wait for the workers to finish.");
            allThreadsAreDone.await();
            LOGGER.log(INFO,"Experiment main woke up!");
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }

        return new WorkloadStats(initTs, submittedArray);
    }

    private static final class Worker {

        public static Map<Long, List<Long>> run(CountDownLatch allThreadsStart, CountDownLatch allThreadsAreDone, Tuple<Integer, String>[] txRatio, Map<String, Iterator<Object>> input, Function<Object, Long> func, int runTime) {
            Map<Long, List<Long>> startTsMap = new HashMap<>();
            Map<String, Integer> histogram = new HashMap<>(txRatio.length);
            for (Tuple<Integer, String> integerStringTuple : txRatio) {
                histogram.put(integerStringTuple.t2, 0);
            }
            ThreadLocalRandom random = ThreadLocalRandom.current();
            long threadId = Thread.currentThread().threadId();
            LOGGER.log(INFO,"Worker run (Thread ID) " + threadId + " started");
            allThreadsStart.countDown();
            try {
                allThreadsStart.await();
            } catch (InterruptedException e) {
                LOGGER.log(ERROR, "Thread ID "+threadId+" failed to await start");
                throw new RuntimeException(e);
            }
            String tx = null;
            int ratio;
            final long initTs = System.currentTimeMillis();
            long currentTs = initTs;
            do {

                ratio = random.nextInt(1,101);
                for (Tuple<Integer, String> txEntry : txRatio) {
                    if (ratio <= txEntry.t1) {
                        tx = txEntry.t2;
                        break;
                    }
                }

                try {
                    if(!input.get(tx).hasNext()){
                        LOGGER.log(WARNING,"Not enough transaction inputs for: "+tx+". Closing submission loop earlier...");
                        Thread.sleep(runTime - (System.currentTimeMillis() - initTs));
                        break;
                    }
                    long batchId = func.apply(input.get(tx).next());
                    if(!startTsMap.containsKey(batchId)){
                        startTsMap.put(batchId, new ArrayList<>());
                    }
                    startTsMap.get(batchId).add(currentTs);
                    histogram.computeIfPresent(tx, (_, v)-> v+1);
                    // only for local tests
//                    if(histogram.get(tx) == 200_000) {
//                        LOGGER.log(WARNING,"200K transaction inputs for: "+tx+" hit. Closing submission loop earlier...");
//                        Thread.sleep(runTime - (System.currentTimeMillis() - initTs));
//                        break;
//                    }
                } catch (Exception e) {
                    LOGGER.log(ERROR,"Exception in Thread ID: " + (e.getMessage() == null ? "No message" : e.getMessage()));
                    throw new RuntimeException(e);
                }
                currentTs = System.currentTimeMillis();
                tx = null;
            } while (currentTs - initTs < runTime);
            LOGGER.log(INFO,"Worker run (Thread ID) " + threadId + " finished");

            StringBuilder output = new StringBuilder("Worker run (Thread ID) " + threadId + " histogram:\n");
            for(var e : histogram.entrySet()){
                output.append(e.getKey()).append(": ").append(e.getValue()).append("\n");
            }
            System.out.println(output);

            allThreadsAreDone.countDown();
            return startTsMap;
        }
    }

    public static List<Map<String,Iterator<Object>>> mapWorkloadInputFiles(int numWare, Map<String, Integer> numTxInputPerType){
        LOGGER.log(INFO, "Mapping "+numWare+" warehouse input files from disk...");
        long initTs = System.currentTimeMillis();
        List<Map<String, Iterator<Object>>> input = new ArrayList<>(numWare);
        int numTransactions;
        for(int i = 0; i < numWare; i++){

            Map<String, Iterator<Object>> wareInput = new HashMap<>(3);

            // new order
            if(numTxInputPerType.containsKey("new_order") && numTxInputPerType.get("new_order") > 0) {
                AppendOnlyBoundedBuffer newOrderBuffer = StorageUtils.loadAppendOnlyBuffer("proxy", NEW_ORDER_INPUT_BASE_FILE_NAME + (i + 1));
                // calculate number of entries (i.e., transaction requests)
                numTransactions = (int) newOrderBuffer.size() / NEW_ORDER_SCHEMA.getRecordSize();
                wareInput.put("new_order", createNewOrderInputIterator(newOrderBuffer, numTransactions));
            }

            // payment
            if(numTxInputPerType.containsKey("payment") && numTxInputPerType.get("payment") > 0) {
                AppendOnlyBoundedBuffer paymentBuffer = StorageUtils.loadAppendOnlyBuffer("proxy", PAYMENT_INPUT_BASE_FILE_NAME + (i + 1));
                numTransactions = (int) paymentBuffer.size() / PAYMENT_SCHEMA.getRecordSize();
                wareInput.put("payment", createPaymentInputIterator(paymentBuffer, numTransactions));
            }

            // order status
            if(numTxInputPerType.containsKey("order_status") && numTxInputPerType.get("order_status") > 0) {
                AppendOnlyBoundedBuffer orderStatusBuffer = StorageUtils.loadAppendOnlyBuffer("proxy", ORDER_STATUS_INPUT_BASE_FILE_NAME + (i + 1));
                numTransactions = (int) orderStatusBuffer.size() / ORDER_STATUS_SCHEMA.getRecordSize();
                wareInput.put("order_status", createOrderStatusInputIterator(orderStatusBuffer, numTransactions));
            }

            input.add(wareInput);
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Mapped input files for "+numWare+" warehouse(s) from disk in "+(endTs-initTs)+" ms");
        return input;
    }

    private static Iterator<Object> createPaymentInputIterator(AppendOnlyBoundedBuffer buffer, int numTransactions){
        return new Iterator<>() {
            int txIdx = 1;
            @Override
            public boolean hasNext() {
                return this.txIdx <= numTransactions;
            }
            @Override
            public PaymentIn next() {
                Object[] paymentInput = readRecordFromMemoryPos(buffer.nextOffset(), PAYMENT_SCHEMA);
                buffer.forwardOffset(PAYMENT_SCHEMA.getRecordSize());
                this.txIdx++;
                return parsePaymentRecordIntoEntity(paymentInput);
            }
        };
    }

    private static Iterator<Object> createOrderStatusInputIterator(AppendOnlyBoundedBuffer buffer, int numTransactions){
        return new Iterator<>() {
            int txIdx = 1;
            @Override
            public boolean hasNext() {
                return this.txIdx <= numTransactions;
            }
            @Override
            public OrderStatusIn next() {
                Object[] orderStatusInput = readRecordFromMemoryPos(buffer.nextOffset(), ORDER_STATUS_SCHEMA);
                buffer.forwardOffset(ORDER_STATUS_SCHEMA.getRecordSize());
                this.txIdx++;
                return parseOrderStatusRecordIntoEntity(orderStatusInput);
            }
        };
    }

    private static Iterator<Object> createNewOrderInputIterator(AppendOnlyBoundedBuffer buffer, int numTransactions){
        return new Iterator<>() {
            int txIdx = 1;
            @Override
            public boolean hasNext() {
                return this.txIdx <= numTransactions;
            }
            @Override
            public NewOrderWareIn next() {
                Object[] newOrderInput = readRecordFromMemoryPos(buffer.nextOffset(), NEW_ORDER_SCHEMA);
                buffer.forwardOffset(NEW_ORDER_SCHEMA.getRecordSize());
                this.txIdx++;
                return parseNewOrderRecordIntoEntity(newOrderInput);
            }
        };
    }

    public static void createWorkload(int numWare, boolean allowMultiWarehouses, Map<String, Integer> numTxPerType) throws IOException {
        deleteWorkloadInputFiles();
        LOGGER.log(INFO, "Starting transaction generation per warehouse/worker");
        long initTs = System.currentTimeMillis();

        ByteBuffer newOrderNativeBuffer = ByteBuffer.allocateDirect(NEW_ORDER_SCHEMA.getRecordSize());
        long newOrderBufferAddress = MemoryUtils.getByteBufferAddress(newOrderNativeBuffer);

        ByteBuffer paymentNativeBuffer = ByteBuffer.allocateDirect(PAYMENT_SCHEMA.getRecordSize());
        long paymentBufferAddress = MemoryUtils.getByteBufferAddress(paymentNativeBuffer);

        ByteBuffer orderStatusNativeBuffer = ByteBuffer.allocateDirect(ORDER_STATUS_SCHEMA.getRecordSize());
        long orderStatusBufferAddress = MemoryUtils.getByteBufferAddress(orderStatusNativeBuffer);

        for (int ware = 1; ware <= numWare; ware++) {
            LOGGER.log(INFO, "Warehouse "+ware+" started");
            for(var entry : numTxPerType.entrySet()) {
                if(entry.getValue() <= 0) continue;
                switch (entry.getKey()){
                    case "new_order" -> {
                        String newOrderInputFileName = NEW_ORDER_INPUT_BASE_FILE_NAME + ware;
                        AppendOnlyUnboundedBuffer newOrderBuffer = StorageUtils.loadAppendOnlyUnboundedBuffer("proxy", newOrderInputFileName);
                        for (int i = 1; i <= entry.getValue(); i++) {
                            Object[] newOrderInput = generateNewOrder(ware, numWare, allowMultiWarehouses);
                            writeRecordInMemoryPos(newOrderBufferAddress, newOrderInput, NEW_ORDER_SCHEMA);
                            newOrderBuffer.append(newOrderNativeBuffer);
                            newOrderNativeBuffer.clear();
                        }
                        newOrderBuffer.force();
                        LOGGER.log(INFO, "Generated "+entry.getValue()+" new order inputs");
                    }
                    case "payment" -> {
                        String paymentInputFileName = PAYMENT_INPUT_BASE_FILE_NAME + ware;
                        AppendOnlyUnboundedBuffer paymentBuffer = StorageUtils.loadAppendOnlyUnboundedBuffer("proxy", paymentInputFileName);
                        for (int i = 1; i <= entry.getValue(); i++) {
                            Object[] paymentInput = generatePayment(ware, numWare);
                            writeRecordInMemoryPos(paymentBufferAddress, paymentInput, PAYMENT_SCHEMA);
                            paymentBuffer.append(paymentNativeBuffer);
                            paymentNativeBuffer.clear();
                        }
                        paymentBuffer.force();
                        LOGGER.log(INFO, "Generated "+entry.getValue()+" payment inputs");
                    }
                    case "order_status" -> {
                        String orderStatusInputFileName = ORDER_STATUS_INPUT_BASE_FILE_NAME + ware;
                        AppendOnlyUnboundedBuffer orderStatusBuffer = StorageUtils.loadAppendOnlyUnboundedBuffer("proxy", orderStatusInputFileName);
                        for (int i = 1; i <= entry.getValue(); i++) {
                            Object[] orderStatusInput = generateOrderStatus(ware);
                            writeRecordInMemoryPos(orderStatusBufferAddress, orderStatusInput, ORDER_STATUS_SCHEMA);
                            orderStatusBuffer.append(orderStatusNativeBuffer);
                            orderStatusNativeBuffer.clear();
                        }
                        orderStatusBuffer.force();
                        LOGGER.log(INFO, "Generated "+entry.getValue()+" order status inputs");
                    }
                }
            }
            LOGGER.log(INFO, "Warehouse "+ware+" done");
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Transaction generation finished in "+(endTs-initTs)+" ms");
    }
    
    public static void deleteWorkloadInputFiles(){
        String basePathStr = StorageUtils.getBasePath("proxy");
        Path basePath = Paths.get(basePathStr);
        try(var paths = Files.walk(basePath)){
            var newOrderInputFiles = paths.filter(path -> path.toString().contains(NEW_ORDER_INPUT_BASE_FILE_NAME) || path.toString().contains(ORDER_STATUS_INPUT_BASE_FILE_NAME)).toList();
            for (var path : newOrderInputFiles){
                if(!path.toFile().delete()){
                    LOGGER.log(ERROR, "Could not dele file path: \n"+path);
                }
            }
        } catch (IOException e){
            LOGGER.log(ERROR, "Error captured while trying to access base path: \n"+e);
        }
    }

    public static int getNumWorkloadInputFiles(Map<String, Integer> numTxPerType){
        String basePathStr = StorageUtils.getBasePath("proxy");
        Path basePath = Paths.get(basePathStr);
        try(Stream<Path> paths = Files.walk(basePath)){
            final String fileName = numTxPerType.containsKey("new_order") ? NEW_ORDER_INPUT_BASE_FILE_NAME : ORDER_STATUS_INPUT_BASE_FILE_NAME;
            List<Path> workloadInputFiles = paths.filter(path -> path.toString().contains(fileName)).toList();
            return workloadInputFiles.size();
        } catch (IOException e){
            LOGGER.log(ERROR, "Error captured while trying to access base path: \n"+e);
            return 0;
        }
    }

    private static NewOrderWareIn parseNewOrderRecordIntoEntity(Object[] newOrderInput) {
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

    private static PaymentIn parsePaymentRecordIntoEntity(Object[] paymentInput) {
        return new PaymentIn(
                (int) paymentInput[0],
                (int) paymentInput[1],
                (int) paymentInput[2],
                (int) paymentInput[3],
                (int) paymentInput[4],
                (float) paymentInput[5],
                (String) paymentInput[6],
                (boolean) paymentInput[7]
        );
    }

    private static OrderStatusIn parseOrderStatusRecordIntoEntity(Object[] orderStatusInput) {
        return new OrderStatusIn(
                (int) orderStatusInput[0],
                (int) orderStatusInput[1],
                (int) orderStatusInput[2],
                (String) orderStatusInput[3],
                (boolean) orderStatusInput[4]
        );
    }

    private static Object[] generateNewOrder(int w_id, int num_ware, boolean allowMultiWarehouses){
        int d_id;
        int c_id;
        int ol_cnt;
        int all_local = 1;
        int not_found = NUM_ITEMS + 1;
        int rbk;

        d_id = randomNumber(1, NUM_DIST_PER_WARE);
        c_id = nuRand(1023, 259, 1, NUM_CUST_PER_DIST);

        ol_cnt = randomNumber(MIN_NUM_ITEMS_PER_ORDER, MAX_NUM_ITEMS_PER_ORDER);
        rbk = randomNumber(1, 100);

        int[] itemIds = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty = new int[ol_cnt];

        for (int i = 0; i < ol_cnt; i++) {
            int item_ = nuRand(8191, 7911, 1, NUM_ITEMS);

            // avoid duplicate items
            while(foundItem(itemIds, i, item_)){
                item_ = nuRand(8191, 7911, 1, NUM_ITEMS);
            }
            itemIds[i] = item_;

            if(FORCE_ABORTS) {
                if ((i == ol_cnt - 1) && (rbk == 1)) {
                    // this can lead to exception and then abort in app code
                    itemIds[i] = not_found;
                }
            }

            if (allowMultiWarehouses) {
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

    private static Object[] generateOrderStatus(int w_id){
        int d_id = randomNumber(1, NUM_DIST_PER_WARE);
        int c_id = nuRand(1023, 259, 1, NUM_CUST_PER_DIST);
        String c_last = DataGenUtils.lastName(nuRand(255, 223, 0,999));
        boolean by_name = randomNumber(1, 100) <= 60;
        return new Object[]{ w_id, d_id, c_id, c_last, by_name };
    }

    private static Object[] generatePayment(int w_id, int num_ware){
        int d_id = randomNumber(1, NUM_DIST_PER_WARE);
        int c_id = nuRand(1023, 259, 1, NUM_CUST_PER_DIST);
        float amount = (float) (randomNumber(100, 500000) / 100.0);
        int c_d_id = d_id;
        int c_w_id = w_id;
        if (randomNumber(1, 100) > 85) {
            c_d_id = randomNumber(1, NUM_DIST_PER_WARE);
            c_w_id = otherWare(num_ware, w_id);
        }
        String c_last = DataGenUtils.lastName(nuRand(255, 223, 0,999));
        boolean by_name = randomNumber(1, 100) <= 60;
        return new Object[]{ w_id, d_id, c_id, c_w_id, c_d_id, amount, c_last, by_name };
    }

}
