package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.row.IKey;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * A hash join where its dependencies are not fulfilled from the start
 * For example, when it is necessary to perform a sequential scan
 * before fulfilling the join
 * TODO generalize for left, right, outer join
 */

public class HashConsumerJoin extends AbstractJoin
        implements
        BiConsumer<CompletableFuture<OperatorResult>,CompletableFuture<OperatorResult>>,
        Supplier<OperatorResult>
{

    private CompletableFuture<OperatorResult> operatorResultFutureLeft;
    private CompletableFuture<OperatorResult> operatorResultFutureRight;

    public HashConsumerJoin(AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        super(innerIndex, outerIndex);
    }

    @Override
    public JoinTypeEnum getType() {
        return JoinTypeEnum.HASH;
    }

    @Override
    public void accept(CompletableFuture<OperatorResult> operatorResultFutureLeft, CompletableFuture<OperatorResult> operatorResultFutureRight) {
        this.operatorResultFutureLeft = operatorResultFutureLeft;
        this.operatorResultFutureRight = operatorResultFutureRight;
    }

    @Override
    public OperatorResult get() {

        // CompletableFuture.allOf( operatorResultFutureLeft, operatorResultFutureRight ).
        try {
            OperatorResult left = operatorResultFutureLeft.get();
            OperatorResult right = operatorResultFutureRight.get();

            // TODO continue

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return null;

    }
}
