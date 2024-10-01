package dk.ku.di.dms.vms.modb.multiversion;

import dk.ku.di.dms.vms.modb.transaction.internal.OneWriterMultiReadersLIFO;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FifoTest {

    private static ExecutorService threadPool;

    @BeforeClass
    public static void before(){
        threadPool = Executors.newWorkStealingPool();
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {

        OneWriterMultiReadersLIFO<TransactionId,Integer> fifo = new OneWriterMultiReadersLIFO<>();

        fifo.put( new TransactionId(1,0), 1 );
        fifo.put( new TransactionId(2,0), 2 );
        fifo.put( new TransactionId(4,0), 4 );

        Future<Boolean> res1 = threadPool.submit( () -> Objects.requireNonNull(fifo.floorEntry(new TransactionId(3, 0))).val() == 2 );
        Future<Boolean> res2 = threadPool.submit( () -> Objects.requireNonNull(fifo.floorEntry(new TransactionId(2, 0))).val() == 2 );
        Future<Boolean> res3 = threadPool.submit( () -> Objects.requireNonNull(fifo.floorEntry(new TransactionId(1, 0))).val() == 1 );
        Future<Boolean> res4 = threadPool.submit( () -> fifo.floorEntry(new TransactionId(0,0)) == null );

        Future<Boolean> res5 = threadPool.submit( () -> {
            fifo.put(new TransactionId(5,0), 5);
            return Objects.requireNonNull(fifo.floorEntry(new TransactionId(5, 0))).val() == 5;
        });

        Future<Boolean> res6 = threadPool.submit( () -> {
            fifo.removeUpToEntry(new TransactionId(1,0));
            return fifo.floorEntry(new TransactionId(1,0)) != null;
        });

        Future<Boolean> res7 = threadPool.submit( () -> {
            fifo.removeUpToEntry(new TransactionId(2,0));
            return fifo.floorEntry(new TransactionId(2,0)) != null &&
                    fifo.floorEntry(new TransactionId(1,0)) == null;
        });

        assert res1.get();
        assert res2.get();
        assert res3.get();
        assert res4.get();
        assert res5.get();
        assert res6.get();
        assert res7.get();

        System.out.println(fifo);

    }

}
