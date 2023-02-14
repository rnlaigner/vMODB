package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.utils.BatchUtils;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * This thread encapsulates the batch of events sending task
 * that should occur periodically. Once set up, it schedules itself
 * after each run, thus avoiding duplicate runs of the same task.
 * -
 * I could get the connection from the vms...
 * But in the future, one output event will no longer map to a single vms
 * So it is better to make the event sender task complete enough
 * FIXME what happens if the connection fails? then put the list of events
 *  in the batch to resend or return to the original location. let the
 *  main loop schedule the timer again. set the network node to off
 */
final class ConsumerVmsWorker extends TimerTask {

    private final Logger logger;
    private final ConsumerVms consumerVms;

    private final AsynchronousSocketChannel channel;

    private final ByteBuffer writeBuffer;

    public ConsumerVmsWorker(ConsumerVms consumerVms,
                             AsynchronousSocketChannel channel,
                             ByteBuffer writeBuffer){
        this.consumerVms = consumerVms;
        this.channel = channel;
        this.writeBuffer = writeBuffer;
        this.logger = Logger.getLogger("vms-worker-"+consumerVms.hashCode());
        this.logger.setUseParentHandlers(true);
    }

    @Override
    public void run() {

        this.logger.info("VMS worker scheduled at: "+System.currentTimeMillis());

        // find the smallest batch. to avoid synchronizing with main thread
        long batchToSend = Long.MAX_VALUE;
        for(long batchId : this.consumerVms.transactionEventsPerBatch.keySet()){
            if(batchId < batchToSend) batchToSend = batchId;
        }

        if(this.consumerVms.transactionEventsPerBatch.get(batchToSend) == null){
            return;
        }

        // there will always be a batch if this point of code is run
        List<TransactionEvent.Payload> events = new ArrayList<>(this.consumerVms.transactionEventsPerBatch.get(batchToSend).size());
        this.consumerVms.transactionEventsPerBatch.get(batchToSend).drainTo(events);

        int remaining = events.size();

        while(remaining > 0){
            this.logger.info("VMS worker submitting batch: "+batchToSend);
            remaining = BatchUtils.assembleBatchPayload( remaining, events, this.writeBuffer);
            this.writeBuffer.flip();
            try {
                int result = this.channel.write(this.writeBuffer).get();
                this.logger.info("Batch has been sent. Result: " + result);
                this.writeBuffer.clear();
            } catch (InterruptedException | ExecutionException e) {
                this.logger.warning("Error submitting batch");
                // return non-processed events to original location or what?
                if (!this.channel.isOpen()) {
                    this.logger.warning("The VMS is offline");
                }
                this.writeBuffer.clear();

                // return events to the deque
                for (TransactionEvent.Payload event : events) {
                    this.consumerVms.transactionEventsPerBatch.get(batchToSend).offerFirst(event);
                }

            }

        }

    }

}