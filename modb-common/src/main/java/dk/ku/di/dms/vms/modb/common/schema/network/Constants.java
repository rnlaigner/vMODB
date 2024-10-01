package dk.ku.di.dms.vms.modb.common.schema.network;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;

public final class Constants {

    /**
     * Message identifiers
     */

    // from and to server nodes
    public static final byte HEARTBEAT = 0;

    public static final byte PRESENTATION = 1;

    public static final byte CONSUMER_SET = 2;

    /**
     * Transaction-related Events
     */
    public static final byte EVENT = 3;

    /**
     * Batch of events
     */
    public static final byte BATCH_OF_EVENTS = 4;

    /**
     * Batch of compressed events
     */
    public static final byte COMPRESSED_BATCH_OF_EVENTS = 14;


    // coming from one or more VMSs in the same transaction
    public static final byte TX_ABORT = 5;

    /*
     * Batch-commit-related events.
     * A batch never aborts.
     * Only individual transactions of the batch may abort.
     * So no need for 2-PC.
     * It works similarly as a snapshotting process in Flink
     */

    /**
     * all terminal VMSs that have participated in a batch must send this event
     * to coordinator in order to complete a batch.
     * Then after the coordinator send the batch commit request to all other VMSs
     * {@link BatchComplete}
    */
    public static final byte BATCH_COMPLETE = 6;

    /**
     * Batch commit info is appended to a batch of events
     * or sent individually to terminal nodes part of a batch
     */
    public static final byte BATCH_COMMIT_INFO = 7;

    /**
     * all terminal VMSs that have participated in a batch must send this event to
     * coordinator in order to complete a batch
     * {@link BatchAbortRequest}
     */
    public static final byte BATCH_REPLICATION = 8;

    public static final byte BATCH_REPLICATION_ACK = 9;

    /**
     * Sent to non-terminal VMSs participating in a batch
     * that whenever and end of batch is observed
     * the batch commit ack can be sent to the coordinator.
     * VMSs after receiving this message snapshot (log) their states
     */
    public static final byte BATCH_COMMIT_COMMAND = 10;

    // a commit response can indicate whether a leadership no longer holds
    // after network problems(e.g., partitions or increased latency) and subsequent normalization

    /**
     * VMSs respond the batch commit with this message... but can be avoided for decreased overhead
     * We assume a  service will eventually respond, even though there is a failure.
     * In other words, VMSs are sending this message but coordinator is not waiting for ACK, only for
     * BATCH_COMPLETE (only the necessary given the DAG formation).
     */
     public static final byte BATCH_COMMIT_ACK = 11;

    /**
     *  This message is sent by a new elected leader to roll back all
     *  changes previously made by the previous ongoing batch
     *  {@link BatchAbortRequest}
     */
    public static final byte BATCH_ABORT_REQUEST = 12;

}
