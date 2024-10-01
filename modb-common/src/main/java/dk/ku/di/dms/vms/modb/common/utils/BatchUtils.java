package dk.ku.di.dms.vms.modb.common.utils;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.nio.ByteBuffer;
import java.util.List;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;

public final class BatchUtils {

    public static final int HEADER = 1 + (2 * Integer.BYTES);

    public static int assembleBatchOfEvents(int remaining, List<TransactionEvent.PayloadRaw> events, ByteBuffer writeBuffer){
        return assembleBatchOfEvents(remaining, events, writeBuffer, HEADER);
    }

    public static int assembleBatchOfEvents(int remaining, List<TransactionEvent.PayloadRaw> events, ByteBuffer writeBuffer, int header){
        int remainingBytes = writeBuffer.remaining();

        writeBuffer.put(BATCH_OF_EVENTS);
        // jump 2 integers
        writeBuffer.position(header);
        remainingBytes = remainingBytes - header;

        // batch them all in the buffer,
        // until buffer capacity is reached or elements are all sent
        int count = 0;
        int idx = events.size() - remaining;
        while(idx < events.size() && remainingBytes > events.get(idx).totalSize()){
            TransactionEvent.writeWithinBatch( writeBuffer, events.get(idx) );
            remainingBytes = remainingBytes - events.get(idx).totalSize();
            idx++;
            count++;
        }

        // buffer size
        int position = writeBuffer.position();
        writeBuffer.putInt(1, position);
        // number of events
        writeBuffer.putInt(5, count);
        // return to original limit position
        writeBuffer.position(position);

        return remaining - count;
    }

}
