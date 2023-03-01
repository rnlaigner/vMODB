package dk.ku.di.dms.vms.modb.common.utils;

import dk.ku.di.dms.vms.modb.common.schema.transaction.TransactionEvent;

import java.nio.ByteBuffer;
import java.util.List;

import static dk.ku.di.dms.vms.modb.common.schema.Constants.BATCH_OF_TRANSACTION_EVENTS;

public final class BatchUtils {

    public static int assembleBatchPayload(int remaining, List<TransactionEvent.Payload> events, ByteBuffer writeBuffer){
        int remainingBytes = writeBuffer.remaining();

        writeBuffer.clear();
        writeBuffer.put(BATCH_OF_TRANSACTION_EVENTS);
        writeBuffer.position(5);

        // batch them all in the buffer,
        // until buffer capacity is reached or elements are all sent
        int count = 0;
        remainingBytes = remainingBytes - 1 - Integer.BYTES;
        int idx = remaining - 1;

        while(idx >= 0 && remainingBytes > events.get(idx).totalSize()){
            TransactionEvent.write( writeBuffer, events.get(idx) );
            remainingBytes = remainingBytes - events.get(idx).totalSize();
            idx--;
            count++;
        }

        writeBuffer.mark();
        writeBuffer.putInt(1, count);
        writeBuffer.reset();

        return remaining - count;
    }

}
