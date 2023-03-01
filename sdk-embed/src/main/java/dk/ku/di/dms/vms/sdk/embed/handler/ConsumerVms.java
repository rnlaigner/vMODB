package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.replication.Subscription;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Identification of a VMS that is ought to receive any sort of events
 * Contains only the necessary information for that
 * Attributes do not form an identification of the VMS, but rather
 * make it easier to manage metadata about each. I.e., metadata that
 * must be shared across threads (e.g., transactionEventsPerBatch)
 */
public class ConsumerVms extends NetworkAddress {

    public transient final SortedMap<Long, BlockingDeque<TransactionEvent.Payload>> transactionEventsPerBatch;

    /**
     * Timer for writing to each VMS connection
     * Read happens asynchronously anyway, so no need to set up timer for that
     * Only used by event handler {e.g., EmbedVmsEventHandler} to spawn periodical send of batch of events
      */
    public transient Timer timer;

    // table and columns to be replicated
    // public final Map<String, List<String>> replicationConfig = new HashMap<>();
    // Subscription
    public final Set<String> subscribedTables;

    public ConsumerVms(String host, int port) {
        super(host, port);
        this.transactionEventsPerBatch = new ConcurrentSkipListMap<>();
        this.subscribedTables = new HashSet<>();
    }

    public ConsumerVms(NetworkAddress address, Timer timer) {
        super(address.host, address.port);
        this.timer = timer;
        this.transactionEventsPerBatch = new ConcurrentSkipListMap<>();
        this.subscribedTables = new HashSet<>();
    }

}