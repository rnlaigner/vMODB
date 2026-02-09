package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public final class DataLoadUtils {

    private static final System.Logger LOGGER = System.getLogger(DataLoadUtils.class.getName());

    /**
     * PORTS
     */
    public static final int WAREHOUSE_VMS_PORT = 8001;
    public static final int INVENTORY_VMS_PORT = 8002;
    public static final int ORDER_VMS_PORT = 8003;

    public static final Map<String, Integer> VMS_TO_PORT_MAP;

    public static final Map<String, String> VMS_TO_HOST_MAP;

    static {
        Properties properties = ConfigUtils.loadProperties();
        VMS_TO_HOST_MAP = new HashMap<>(3);
        VMS_TO_HOST_MAP.put("warehouse", properties.getProperty("warehouse_host"));
        VMS_TO_HOST_MAP.put("inventory", properties.getProperty("inventory_host"));
        VMS_TO_HOST_MAP.put("order", properties.getProperty("order_host"));

        VMS_TO_PORT_MAP = new HashMap<>(3);
        VMS_TO_PORT_MAP.put("warehouse", WAREHOUSE_VMS_PORT);
        VMS_TO_PORT_MAP.put("inventory", INVENTORY_VMS_PORT);
        VMS_TO_PORT_MAP.put("order", ORDER_VMS_PORT);
    }

    private static final Map<String, ConcurrentLinkedDeque<MinimalHttpClient>> CONNECTION_POOL = new ConcurrentHashMap<>();

    /**
     * In case the services have been restarted, the cached connections won't work anymore
     * Calling this method is a conservative way to avoid errors on ingesting again in the same experiment session
     */
    public static void releaseAllConnections(){
        for(var entries : CONNECTION_POOL.values()){
            for(var conn : entries){
                conn.close();
            }
        }
        CONNECTION_POOL.clear();
    }

    public static MinimalHttpClient obtainHttpClient(String vms) {
        var clientPool = CONNECTION_POOL.computeIfAbsent(vms, (ignored)-> new ConcurrentLinkedDeque<>());
        if (!clientPool.isEmpty()) {
            MinimalHttpClient client = clientPool.poll();
            if (client != null) return client;
        }
        try {
            String host = VMS_TO_HOST_MAP.get(vms);
            int port = VMS_TO_PORT_MAP.get(vms);
            return new MinimalHttpClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException("Exception captured for VMS "+vms+":\n"+ e);
        }
    };

    public static void returnHttpClient(String vms, MinimalHttpClient client){
        // return to pool for reuse
        CONNECTION_POOL.get(vms).add(client);
    }

    public static void cleanup(boolean reset) {
        String param;
        Properties properties = ConfigUtils.loadProperties();
        if(reset) param = "reset"; else param = "cleanup";
        for(Map.Entry<String, Integer> vms : VMS_TO_PORT_MAP.entrySet()){
            String host = properties.getProperty(vms.getKey() + "_host");
            try(MinimalHttpClient client = new MinimalHttpClient(host, vms.getValue())){
                if(client.sendRequest("PATCH", "", param) != 200){
                    System.out.println("Error on "+param+" "+vms+" state!");
                }
            } catch (IOException e) {
                System.out.println("Exception on "+ param + " " +vms+" state: \n"+e);
            }
        }
    }

}
