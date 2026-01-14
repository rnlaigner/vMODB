package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiFunction;

import static java.lang.System.Logger.Level.ERROR;

public final class DataLoadUtils {

    private static final System.Logger LOGGER = System.getLogger(DataLoadUtils.class.getName());

    public static Map<String, String> mapVmsToHost(Properties properties) {
        Map<String, String> vmsToHostMap = new HashMap<>();
        vmsToHostMap.put("warehouse_host", properties.getProperty("warehouse_host"));
        vmsToHostMap.put("inventory_host", properties.getProperty("inventory_host"));
        vmsToHostMap.put("order_host", properties.getProperty("order_host"));
        return vmsToHostMap;
    }

    /**
     * PORTS
     */
    public static final int WAREHOUSE_VMS_PORT = 8001;
    public static final int INVENTORY_VMS_PORT = 8002;
    public static final int ORDER_VMS_PORT = 8003;

    public static final Map<String, Integer> VMS_TO_PORT_MAP;

    public static final Map<String, String> TABLE_TO_VMS_MAP;

    static {
        VMS_TO_PORT_MAP = new HashMap<>(3);
        VMS_TO_PORT_MAP.put("warehouse", WAREHOUSE_VMS_PORT);
        VMS_TO_PORT_MAP.put("inventory", INVENTORY_VMS_PORT);
        VMS_TO_PORT_MAP.put("order", ORDER_VMS_PORT);

        TABLE_TO_VMS_MAP = new HashMap<>();
        TABLE_TO_VMS_MAP.put("warehouse", "warehouse");
        TABLE_TO_VMS_MAP.put("district", "warehouse");
        TABLE_TO_VMS_MAP.put("customer", "warehouse");

        TABLE_TO_VMS_MAP.put("item", "inventory");
        TABLE_TO_VMS_MAP.put("stock", "inventory");

        TABLE_TO_VMS_MAP.put("orders", "order");
        TABLE_TO_VMS_MAP.put("new_orders", "order");
        TABLE_TO_VMS_MAP.put("order_line", "order");
    }

    private static final Map<String, ConcurrentLinkedDeque<MinimalHttpClient>> CONNECTION_POOL = new ConcurrentHashMap<>();

    /**
     * In case the services have been restarted, the cached connections won't work anymore
     * Calling this method is a conservative way to avoid errors on ingesting again in the same experiment session
     */
    private static void releaseAllConnections(){
        for(var entries : CONNECTION_POOL.values()){
            for(var conn : entries){
                conn.close();
            }
        }
        CONNECTION_POOL.clear();
    }

    private static final BiFunction<String, String, MinimalHttpClient> HTTP_CLIENT_SUPPLIER = (table, host) -> {
        String vms = TABLE_TO_VMS_MAP.get(table);
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
            int port = VMS_TO_PORT_MAP.get(vms);
            return new MinimalHttpClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException("Exception captured for VMS "+vms+" table "+table+" \n"+ e);
        }
    };

    private static void returnConnection(String table, MinimalHttpClient client){
        // return to pool for reuse
        String service = TABLE_TO_VMS_MAP.get(table);
        CONNECTION_POOL.get(service).add(client);
    }

}
