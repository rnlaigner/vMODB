package dk.ku.di.dms.vms.sdk.embed.client;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.util.Properties;

public final class VmsApplicationOptions {

    private final String host;

    private final int port;

    private final String[] packages;

    private final int networkBufferSize;

    private final int networkThreadPoolSize;

    private final int numVmsWorkers;

    private final int vmsThreadPoolSize;

    private final int networkSendTimeout;

    private final int soBufferSize;

    private final int maxSleep;

    private final boolean logging;

    private final boolean checkpointing;

    private final boolean truncating;

    private final int maxRecords;

    public static VmsApplicationOptions build(String host, int port, String[] packages) {
        Properties properties = ConfigUtils.loadProperties();
        return build(properties, host, port, packages);
    }

    public static VmsApplicationOptions build(Properties properties, String host, int port, String[] packages) {

        System.out.println("Properties: \n" + properties.toString());

        int networkBufferSize = Integer.parseInt(properties.getProperty("network_buffer_size"));
        int soBufferSize = Integer.parseInt(properties.getProperty("so_buffer_size"));
        int networkSendTimeout = Integer.parseInt(properties.getProperty("network_send_timeout"));
        int networkThreadPoolSize = Integer.parseInt(properties.getProperty("network_thread_pool_size"));
        int vmsThreadPoolSize = Integer.parseInt(properties.getProperty("vms_thread_pool_size"));
        int numVmsWorkers = Integer.parseInt(properties.getProperty("num_vms_workers"));

        int maxSleep = 0;
        String maxSleepStr = properties.getProperty("max_sleep");
        if(maxSleepStr != null) {
            maxSleep = Integer.parseInt(maxSleepStr);
        }

        boolean logging = Boolean.parseBoolean(properties.getProperty("logging"));
        boolean checkpointing = Boolean.parseBoolean(properties.getProperty("checkpointing"));

        boolean truncating = true;
        String truncatingStr = properties.getProperty("checkpointing_truncate");
        if(truncatingStr != null){
            truncating = Boolean.parseBoolean(truncatingStr);
        }

        int maxRecords = 0;
        if (properties.containsKey("max_records")) {
            maxRecords = Integer.parseInt(properties.getProperty("max_records"));
        }

        return new VmsApplicationOptions(
                host,
                port,
                packages,
                networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize,
                networkThreadPoolSize,
                numVmsWorkers,
                vmsThreadPoolSize,
                networkSendTimeout,
                soBufferSize,
                logging,
                checkpointing,
                truncating,
                maxRecords == 0 ? 100000 : maxRecords,
                maxSleep);
    }

    private VmsApplicationOptions(String host, int port, String[] packages,
                                  int networkBufferSize, int networkThreadPoolSize, int numVmsWorkers,
                                  int vmsThreadPoolSize, int networkSendTimeout, int soBufferSize,
                                  boolean logging, boolean checkpointing, boolean truncating, int maxRecords, int maxSleep) {
        this.host = host;
        this.port = port;
        this.packages = packages;
        this.networkBufferSize = networkBufferSize;
        this.networkThreadPoolSize = networkThreadPoolSize;
        this.numVmsWorkers = numVmsWorkers;
        this.vmsThreadPoolSize = vmsThreadPoolSize;
        this.networkSendTimeout = networkSendTimeout;
        this.soBufferSize = soBufferSize;
        this.logging = logging;
        this.checkpointing = checkpointing;
        this.truncating = truncating;
        this.maxRecords = maxRecords;
        this.maxSleep = maxSleep;
    }

    public String host() {
        return this.host;
    }

    public int networkBufferSize() {
        return this.networkBufferSize;
    }

    public int networkThreadPoolSize() {
        return this.networkThreadPoolSize;
    }

    public int networkSendTimeout(){
        return this.networkSendTimeout;
    }

    public String[] packages() {
        return this.packages;
    }

    public int port() {
        return this.port;
    }

    public int vmsThreadPoolSize() {
        return this.vmsThreadPoolSize;
    }

    public int soBufferSize() {
        return this.soBufferSize;
    }

    public int numVmsWorkers() {
        return this.numVmsWorkers;
    }

    public int maxSleep() {
        return this.maxSleep;
    }

    public boolean isLogging() {
        return this.logging;
    }

    public boolean isCheckpointing() {
        return this.checkpointing;
    }

    public boolean isTruncating() {
        return this.truncating;
    }

    public int getMaxRecords() {
        return this.maxRecords;
    }
}
