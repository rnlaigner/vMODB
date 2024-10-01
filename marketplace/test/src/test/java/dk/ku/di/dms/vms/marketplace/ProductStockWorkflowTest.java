package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdateProduct;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static dk.ku.di.dms.vms.marketplace.common.Constants.UPDATE_PRICE;
import static dk.ku.di.dms.vms.marketplace.common.Constants.UPDATE_PRODUCT;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

public final class ProductStockWorkflowTest extends AbstractWorkflowTest {

    @Test
    public void testLargeBatchWithTwoVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.product.Main.main(null);
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);

        ingestDataIntoProductVms();
        insertItemsInStockVms();

        // initialize coordinator
        Coordinator coordinator = loadCoordinator();

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        int maxSleep = 3;
        do {
            sleep(5000);
            if(coordinator.getConnectedVMSs().size() == 2) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 2) throw new RuntimeException("VMSs did not connect to coordinator on time");

        Thread thread = new Thread(new Producer(coordinator));
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 2);

        Assert.assertEquals(2, coordinator.getBatchOffsetPendingCommit());
        Assert.assertEquals(9, coordinator.getNumTIDsCommitted());
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8080 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG updatePriceDag =  TransactionBootstrap.name(UPDATE_PRICE)
                .input( "a", "product", UPDATE_PRICE)
                .terminal("b", "product", "a")
                .build();

        TransactionDAG updateProductDag =  TransactionBootstrap.name(UPDATE_PRODUCT)
                .input( "a", "product", UPDATE_PRODUCT)
                .terminal("b", "stock", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);
        transactionMap.put(updateProductDag.name, updateProductDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        IdentifiableNode productAddress = new IdentifiableNode("product", "localhost", 8081);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", 8082);

        Map<String, IdentifiableNode> VMSs = new HashMap<>(2);
        VMSs.put(productAddress.identifier, productAddress);
        VMSs.put(stockAddress.identifier, stockAddress);

        return Coordinator.build(
                serverMap,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(BATCH_WINDOW_INTERVAL),
                1,
                1,  ignored -> new IHttpHandler() { },
                serdes
        );
    }

    private static class Producer implements Runnable {

        Coordinator coordinator;

        public Producer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val < 10) {
                UpdateProduct updateProduct = new UpdateProduct(
                        1,1,"test","test","test","test",10.0F,10.0F,"test", String.valueOf(val)
                );

                String payload = serdes.serializeAsString(updateProduct, UpdateProduct.class);

                TransactionInput.Event eventPayload = new TransactionInput.Event("update_product", payload);

                TransactionInput txInput = new TransactionInput("update_product", eventPayload);

                LOGGER.log(INFO, "[Producer] Adding "+val);

                coordinator.queueTransactionInput(txInput);

                val++;
            }
            LOGGER.log(INFO, "Producer going to bed definitely... ");
        }
    }

}
