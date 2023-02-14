package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.Map;
import java.util.Timer;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Identification of a VMS that is ought to receive any sort of events
 * Contains only the necessary information for that
 * Attributes do not form an identification of the VMS, but rather
 * make it easier to manage metadata about each. I.e., metadata that
 * must be shared across threads (e.g., transactionEventsPerBatch)
 */
public class ConsumerVms extends NetworkAddress {

    public transient final Map<Long, BlockingDeque<TransactionEvent.Payload>> transactionEventsPerBatch;

    /**
     * Timer for writing to each VMS connection
     * Read happens asynchronously anyway, so no need to set up timer for that
     * Only used by event handler {e.g., EmbedVmsEventHandler} to spawn periodical send of batch of events
      */
    public transient Timer timer;

    public ConsumerVms(String host, int port) {
        super(host, port);
        this.transactionEventsPerBatch = new ConcurrentHashMap<>();
    }

    public ConsumerVms(NetworkAddress address, Timer timer) {
        super(address.host, address.port);
        this.timer = timer;
        this.transactionEventsPerBatch = new ConcurrentHashMap<>();
    }

}