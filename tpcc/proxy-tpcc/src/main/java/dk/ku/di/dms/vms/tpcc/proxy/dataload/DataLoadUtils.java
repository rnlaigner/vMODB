package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;

import static java.lang.System.Logger.Level.*;

public final class DataLoadUtils {

    private static final System.Logger LOGGER = System.getLogger(DataLoadUtils.class.getName());

    public static Map<String, String> mapVmsToHost(Properties properties) {
        Map<String, String> vmsToHostMap = new HashMap<>();
        vmsToHostMap.put("warehouse_host", properties.getProperty("warehouse_host"));
        vmsToHostMap.put("inventory_host", properties.getProperty("inventory_host"));
        vmsToHostMap.put("order_host", properties.getProperty("order_host"));
        return vmsToHostMap;
    }

    @SuppressWarnings("rawtypes")
    public static Map<String, QueueTableIterator> mapTablesFromDisk(Map<String, UniqueHashBufferIndex> tableToIndexMap,
                                                                    Map<String, EntityHandler> entityHandlerMap) {
        LOGGER.log(INFO, "Mapping tables from disk starting...");
        long init = System.currentTimeMillis();
        Map<String, QueueTableIterator> tableInputMap = new HashMap<>();
        try {
            for(var idx : tableToIndexMap.entrySet()){
                if (idx.getKey().contains("stock")){
                    tableInputMap.put(idx.getKey(), new QueueTableIterator(idx.getValue(), entityHandlerMap.get("stock")));
                } else if (idx.getKey().contains("customer")) {
                    tableInputMap.put(idx.getKey(), new QueueTableIterator(idx.getValue(), entityHandlerMap.get("customer")));
                } else {
                    tableInputMap.put(idx.getKey(), new QueueTableIterator(idx.getValue(), entityHandlerMap.get(idx.getKey())));
                }
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            long end = System.currentTimeMillis();
            LOGGER.log(INFO, "Mapping tables from disk finished in "+(end-init)+" ms");
        }
        return tableInputMap;
    }

    /**
     * In case the services have been restarted, the cached connections won't work anymore
     * Calling this method is a conservative way to avoid errors on ingesting again in the same experiment session
     */
    private static void releaseAllConnections(){
        for(var entries : IngestionWorker.CONNECTION_POOL.values()){
            for(var conn : entries){
                conn.close();
            }
        }
        IngestionWorker.CONNECTION_POOL.clear();
    }

    public static void ingestData(Map<String, QueueTableIterator> tableInputMap, Map<String, String> vmsToHostMap, int numCpus) {
        releaseAllConnections();
        ExecutorService threadPool = Executors.newFixedThreadPool(numCpus);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(numCpus);
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);
        LOGGER.log(INFO, "Table ingestion starting...");
        long init = System.currentTimeMillis();

        Map<String, QueueTableIterator> tableInputMapPriority = new LinkedHashMap<>();
        tableInputMapPriority.put("warehouse", tableInputMap.remove("warehouse"));
        tableInputMapPriority.put("item", tableInputMap.remove("item"));
        tableInputMapPriority.put("district", tableInputMap.remove("district"));

        try {
            for (int i = 1; i <= numCpus; i++) {
                service.submit(new IngestionWorker(tableInputMapPriority, vmsToHostMap), null);
            }
            for (int i = 1; i <= numCpus; i++) {
                completionQueue.take();
            }
            for (int i = 1; i <= numCpus; i++) {
                service.submit(new IngestionWorker(tableInputMap, vmsToHostMap), null);
            }
            for (int i = 1; i <= numCpus; i++) {
                completionQueue.take();
            }
        } catch(InterruptedException e){
            threadPool.shutdownNow();
            e.printStackTrace(System.err);
        } finally{
            long end = System.currentTimeMillis();
            LOGGER.log(INFO, "Table ingestion finished in " + (end - init) + "ms");
        }
    }

    private static class IngestionWorker implements Runnable {

        private static final Map<String, ConcurrentLinkedDeque<MinimalHttpClient>> CONNECTION_POOL = new ConcurrentHashMap<>();

        private static final BiFunction<String, String, MinimalHttpClient> HTTP_CLIENT_SUPPLIER = (table, host) -> {
            String vms = TPCcConstants.TABLE_TO_VMS_MAP.get(table);
            if(vms != null){
                var clientPool = CONNECTION_POOL.computeIfAbsent(vms, (ignored)-> new ConcurrentLinkedDeque<>());
                if (!clientPool.isEmpty()) {
                    MinimalHttpClient client = clientPool.poll();
                    if (client != null) return client;
                }
            } else {
                LOGGER.log(ERROR, table+" not found! Set it correctly in TPCcConstants.TABLE_TO_VMS_MAP");
            }
            try {
                int port = TPCcConstants.VMS_TO_PORT_MAP.get(vms);
                return new MinimalHttpClient(host, port);
            } catch (Exception e) {
                throw new RuntimeException("Exception captured for VMS "+vms+" table "+table+" \n"+ e);
            }
        };

        private static void returnConnection(String table, MinimalHttpClient client){
            // return to pool for reuse
            String service = TPCcConstants.TABLE_TO_VMS_MAP.get(table);
            CONNECTION_POOL.get(service).add(client);
        }

        private final Map<String, QueueTableIterator> tableInputMap;
        private final Map<String, String> vmsToHostMap;

        private IngestionWorker(Map<String, QueueTableIterator> tableInputMap, Map<String, String> vmsToHostMap) {
            this.tableInputMap = tableInputMap;
            this.vmsToHostMap = vmsToHostMap;
        }

        /**
         * Must ensure data dependencies are fulfilled to avoid errors
         * Warehouse -> District -> Customer
         * Item -> Stock
         */
        @Override
        public void run() {
            try {
                for(Map.Entry<String, QueueTableIterator> table : this.tableInputMap.entrySet()) {
                    run_(table.getKey(), table.getValue());
                }
            } catch (Exception e){
                e.printStackTrace(System.err);
            }
        }

        private void run_(String table, QueueTableIterator iterator) throws IOException {
            String actualTable = table.contains("stock") ? "stock" : table;
            actualTable = table.contains("customer") ? "customer" : actualTable;
            MinimalHttpClient client = HTTP_CLIENT_SUPPLIER.apply(
                    actualTable,
                    this.vmsToHostMap.get(TPCcConstants.TABLE_TO_VMS_MAP.get(actualTable) + "_host" ));
            String entity;
            int count = 0;
            LOGGER.log(INFO, "Thread "+Thread.currentThread().threadId()+" start loading data to table "+ table);
            List<String> errors = new ArrayList<>();
            while ((entity = iterator.poll()) != null) {
                if(client.sendRequest("POST", entity, actualTable) != 200){
                    errors.add(entity);
                    continue;
                }
                count++;
            }

            if(!errors.isEmpty()) {
                LOGGER.log(WARNING, "Thread " + Thread.currentThread().threadId() + " trying to resend " + errors.size() + " failed entities...");
                int numEntities = errors.size();
                while (numEntities > 0) {
                    numEntities--;
                    entity = errors.removeFirst();
                    if (client.sendRequest("POST", entity, actualTable) != 200) {
                        continue;
                    }
                    count++;
                }
            }

            if(!errors.isEmpty()){
                LOGGER.log(WARNING, "Thread "+Thread.currentThread().threadId()+" finished with table "+ table+": "+count+" records sent and "+errors.size()+ " errors.");
            } else {
                LOGGER.log(INFO, "Thread "+Thread.currentThread().threadId()+" finished with table "+ table+": "+count+" records sent.");
            }
            returnConnection(actualTable, client);
        }
    }

}
