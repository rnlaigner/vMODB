package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.TPCcConstants;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Supplier;

public final class DataLoader {

    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_TYPE_VAL = "application/json";

    private static final ConcurrentLinkedQueue<HttpClient> CLIENT_POOL = new ConcurrentLinkedQueue<>();

    private static final Supplier<HttpClient> HTTP_CLIENT_SUPPLIER = () -> {
        if (!CLIENT_POOL.isEmpty()) {
            HttpClient client = CLIENT_POOL.poll();
            if (client != null) return client;
        }
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    };

    @SuppressWarnings({"rawtypes"})
    public static void load(Map<String, UniqueHashBufferIndex> tableToIndexMap,
                            Map<String, EntityHandler> entityHandlerMap) {

        int cpus = Runtime.getRuntime().availableProcessors();
        var threadPool = Executors.newFixedThreadPool(cpus);
        BlockingQueue<Future<Void>> completionQueue = new ArrayBlockingQueue<>(tableToIndexMap.size());
        CompletionService<Void> service = new ExecutorCompletionService<>(threadPool, completionQueue);

        // iterate over tables, for each, create a set of threads to ingest data
        for(var idx : tableToIndexMap.entrySet()){
            if(!idx.getKey().contentEquals("warehouse")) continue;
            service.submit(new IngestionWorker(idx.getKey(), idx.getValue(), entityHandlerMap.get(idx.getKey())), null);
        }

        long init = System.currentTimeMillis();
        try {
            for (int i = 0; i < tableToIndexMap.size(); i++) {
                completionQueue.poll(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e){
            threadPool.shutdownNow();
            e.printStackTrace(System.err);
        } finally {
            long end = System.currentTimeMillis();
            System.out.println("Loading tables (ms): "+(end-init));
        }

    }

    @SuppressWarnings({"rawtypes"})
    private static class IngestionWorker implements Runnable {

        private final String table;
        private final UniqueHashBufferIndex index;
        private final EntityHandler entityHandler;

        private IngestionWorker(String table, UniqueHashBufferIndex index, EntityHandler entityHandler) {
            this.table = table;
            this.index = index;
            this.entityHandler = entityHandler;
        }

        @Override
        public void run() {
            HttpClient httpClient = HTTP_CLIENT_SUPPLIER.get();
            try {

                IRecordIterator<IKey> iterator = index.iterator();

                String vms = TPCcConstants.TABLE_TO_VMS_MAP.get(table);
                int port = TPCcConstants.VMS_TO_PORT_MAP.get(vms);

                Properties properties = ConfigUtils.loadProperties();
                String host = properties.getProperty(table+"_host");

                String url = "http://"+ host+":"+port+"/"+table;

                while(iterator.hasNext()){
                    IKey key = iterator.next();
                    Object[] record = index.record(key);

                    var entity = entityHandler.parseObjectIntoEntity(record);

                    HttpRequest httpReq = HttpRequest.newBuilder()
                            .uri(URI.create(url))
                            .header(CONTENT_TYPE, CONTENT_TYPE_VAL)
                            .POST(HttpRequest.BodyPublishers.ofString(entity.toString()))
                            .build();

//                    var resp = httpClient.send(HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port)).GET().build(), HttpResponse.BodyHandlers.ofString());
//                    System.out.println(resp.body());
                    httpClient.send(httpReq, HttpResponse.BodyHandlers.discarding());
                }
            } catch (Exception e){
                e.printStackTrace(System.err);
            } finally {
                CLIENT_POOL.add(httpClient);
            }

        }
    }

}
