package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.inputs.PriceUpdate;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdateProduct;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

public final class CartProductStockWorkflowTest extends AbstractWorkflowTest {

    private static final int WAIT_TIME = 5000;

    @Test
    public void testBasicCartProductStockWorkflow() throws Exception {
        dk.ku.di.dms.vms.marketplace.product.Main.main(null);
        dk.ku.di.dms.vms.marketplace.cart.Main.main(null);
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);

        ingestDataIntoProductVms();
        insertItemsInStockVms();

        // initialize coordinator
        Properties properties = ConfigUtils.loadProperties();
        Coordinator coordinator = loadCoordinator(properties);

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        int maxSleep = 3;
        do {
            sleep(WAIT_TIME);
            if(coordinator.getConnectedVMSs().size() == 3) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 3) throw new RuntimeException("VMSs did not connect to coordinator on time");

        Thread thread = new Thread(new ProductUpdateProducer(coordinator));
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 2);

        Assert.assertEquals(20, coordinator.getNumTIDsCommitted());
        Assert.assertEquals(2, coordinator.getBatchOffsetPendingCommit());
    }

    private Coordinator loadCoordinator(Properties properties) throws IOException {
        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG updatePriceDag =  TransactionBootstrap.name(UPDATE_PRICE)
                .input("a", "product", UPDATE_PRICE)
                .terminal("b", "cart", "a")
                .build();

        TransactionDAG updateProductDag =  TransactionBootstrap.name(UPDATE_PRODUCT)
                .input( "a", "product", UPDATE_PRODUCT)
                .terminal("b", "stock", "a")
                .terminal("c", "cart", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);
        transactionMap.put(updateProductDag.name, updateProductDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        String productHost = properties.getProperty("product_host");
        String cartHost = properties.getProperty("cart_host");
        String stockHost = properties.getProperty("stock_host");

        Map<String, IdentifiableNode> VMSs = getIdentifiableNodeMap(productHost, cartHost, stockHost);

        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int batchSendRate = Integer.parseInt( properties.getProperty("batch_window_ms") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );

        return Coordinator.build(
                serverMap,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withBatchWindow(batchSendRate)
                        .withNetworkThreadPoolSize(groupPoolSize)
                        .withNetworkBufferSize(networkBufferSize),
                1,
                1,  ignored -> new IHttpHandler() { },
                serdes
        );
    }

    private static Map<String, IdentifiableNode> getIdentifiableNodeMap(String productHost, String cartHost, String stockHost) {
        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, PRODUCT_VMS_PORT);
        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, CART_VMS_PORT);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", stockHost, STOCK_VMS_PORT);
        Map<String, IdentifiableNode> VMSs = new HashMap<>();
        VMSs.put(productAddress.identifier, productAddress);
        VMSs.put(cartAddress.identifier, cartAddress);
        VMSs.put(stockAddress.identifier, stockAddress);
        return VMSs;
    }

    private static class ProductUpdateProducer implements Runnable {

        Coordinator coordinator;

        public ProductUpdateProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val <= 10) {
                produceProductUpdate(val, serdes);
                producePriceUpdate(val, serdes);
                val++;
            }
            LOGGER.log(INFO, "Producer going to bed definitely... ");
        }

        private void produceProductUpdate(int val, IVmsSerdesProxy serdes) {
            UpdateProduct updateProduct = new UpdateProduct(
                    1,1,"test","test","test","test",10.0F,10.0F,"test", String.valueOf(val)
            );
            String payload = serdes.serializeAsString(updateProduct, UpdateProduct.class);
            TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRODUCT, payload);
            TransactionInput txInput = new TransactionInput(UPDATE_PRODUCT, eventPayload);
            LOGGER.log(INFO, "[Producer] New product version: "+ val);
            coordinator.queueTransactionInput(txInput);
        }

        private void producePriceUpdate(int val, IVmsSerdesProxy serdes) {
            PriceUpdate priceUpdate = new PriceUpdate(1,1,10.0F, String.valueOf(val), String.valueOf(val));
            String payload = serdes.serializeAsString(priceUpdate, PriceUpdate.class);
            TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRICE, payload);
            TransactionInput txInput = new TransactionInput(UPDATE_PRICE, eventPayload);
            LOGGER.log(INFO, "[Producer] New product price: "+ val);
            coordinator.queueTransactionInput(txInput);
        }
    }

}
