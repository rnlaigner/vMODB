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

    private final String networkThreadPoolType;

    private final int vmsThreadPoolSize;

    private final int networkSendTimeout;

    private final int osBufferSize;

    private final int maxSleep;

    private final boolean compressing;

    private final boolean logging;

    private final boolean checkpointing;

    private final int maxRecords;

    public static VmsApplicationOptions build(String host, int port, String[] packages) {
        Properties properties = ConfigUtils.loadProperties();
        return build(properties, host, port, packages);
    }

    public static VmsApplicationOptions build(Properties properties, String host, int port, String[] packages) {

        System.out.println("Properties: \n" + properties.toString());

        int networkBufferSize = Integer.parseInt(properties.getProperty("network_buffer_size"));
        int osBufferSize = Integer.parseInt(properties.getProperty("os_buffer_size"));
        int networkSendTimeout = Integer.parseInt(properties.getProperty("network_send_timeout"));
        String networkThreadPoolType = properties.getProperty("network_thread_pool_type");
        int networkThreadPoolSize = Integer.parseInt(properties.getProperty("network_thread_pool_size"));
        int vmsThreadPoolSize = Integer.parseInt(properties.getProperty("vms_thread_pool_size"));
        int numVmsWorkers = Integer.parseInt(properties.getProperty("num_vms_workers"));
        int maxSleep = Integer.parseInt(properties.getProperty("max_sleep"));
        boolean logging = Boolean.parseBoolean(properties.getProperty("logging"));
        boolean checkpointing = Boolean.parseBoolean(properties.getProperty("checkpointing"));
        int maxRecords = Integer.parseInt(properties.getProperty("max_records"));
        boolean compression = Boolean.parseBoolean(properties.getProperty("compression"));

        return new VmsApplicationOptions(
                host,
                port,
                packages,
                networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize,
                networkThreadPoolType == null ? "default" : networkThreadPoolType,
                networkThreadPoolSize,
                numVmsWorkers,
                vmsThreadPoolSize,
                networkSendTimeout,
                osBufferSize,
                compression,
                logging,
                checkpointing,
                maxRecords == 0 ? MemoryUtils.DEFAULT_NUM_RECORDS : maxRecords,
                maxSleep);
    }

    private VmsApplicationOptions(String host, int port, String[] packages,
                                  int networkBufferSize, String networkThreadPoolType,
                                  int networkThreadPoolSize, int numVmsWorkers,
                                  int vmsThreadPoolSize, int networkSendTimeout, int osBufferSize, boolean compressing,
                                  boolean logging, boolean checkpointing, int maxRecords, int maxSleep) {
        this.host = host;
        this.port = port;
        this.packages = packages;
        this.networkBufferSize = networkBufferSize;
        this.networkThreadPoolType = networkThreadPoolType;
        this.networkThreadPoolSize = networkThreadPoolSize;
        this.numVmsWorkers = numVmsWorkers;
        this.vmsThreadPoolSize = vmsThreadPoolSize;
        this.networkSendTimeout = networkSendTimeout;
        this.osBufferSize = osBufferSize;
        this.compressing = compressing;
        this.logging = logging;
        this.checkpointing = checkpointing;
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

    public int osBufferSize() {
        return this.osBufferSize;
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

    public int getMaxRecords() {
        return this.maxRecords;
    }

    public String networkThreadPoolType() {
        return this.networkThreadPoolType;
    }

    public boolean isCompressing() {
        return this.compressing;
    }

}
