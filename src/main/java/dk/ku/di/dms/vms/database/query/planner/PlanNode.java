package dk.ku.di.dms.vms.database.query.planner;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PlanNode {

    // applies only to projections to data classes
    // public Consumer<Object> projector;

    // applies to any execution
    public Supplier<OperatorResult> supplier;

    // single thread execution
    public Consumer<OperatorResult> consumer;

    // parallel execution
    public Consumer<CompletableFuture<OperatorResult>> consumerFuture;
    public BiConsumer<CompletableFuture<OperatorResult>,CompletableFuture<OperatorResult>> biConsumerFuture;

    public PlanNode father;
    public PlanNode left;
    public PlanNode right;

    public boolean isLeaf;

    // https://www.interdb.jp/pg/pgsql03.html

    public PlanNode(Supplier<OperatorResult> supplier){
        this.supplier = supplier;
    }

    public PlanNode(){}

    public PlanNode(Supplier<OperatorResult> supplier,
                    Consumer<CompletableFuture<OperatorResult>> consumerFuture,
                    BiConsumer<CompletableFuture<OperatorResult>,CompletableFuture<OperatorResult>> biConsumerFuture,
                    PlanNode father,
                    PlanNode left,
                    PlanNode right,
                    boolean isLeaf) {
        this.supplier = supplier;
        this.consumerFuture = consumerFuture;
        this.biConsumerFuture = biConsumerFuture;
        this.father = father;
        this.left = left;
        this.right = right;
        this.isLeaf = isLeaf;
    }
}
