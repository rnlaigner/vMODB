package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.inputs.PriceUpdate;
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

public class CartProductWorkflowTest extends AbstractWorkflowTest {
    
    @Test
    public final void testBasicCartProductWorkflow() throws Exception {
        this.initCartAndProduct();
        ingestDataIntoProductVms();

        // initialize coordinator
        Properties properties = ConfigUtils.loadProperties();
        Coordinator coordinator = loadCoordinator(properties);

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        int maxSleep = 3;
        do {
            sleep(5000);
            if(coordinator.getConnectedVMSs().size() == 2) break;
            maxSleep--;
        } while (maxSleep > 0);

        if(coordinator.getConnectedVMSs().size() < 2) throw new RuntimeException("VMSs did not connect to coordinator on time");

        Thread thread = new Thread(new PriceUpdateProducer(coordinator));
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 3);

        Assert.assertEquals(9, coordinator.getNumTIDsCommitted());

        this.additionalAssertions();
    }

    protected void additionalAssertions() { }

    protected void initCartAndProduct() throws Exception {
        dk.ku.di.dms.vms.marketplace.product.Main.main(null);
        dk.ku.di.dms.vms.marketplace.cart.Main.main(null);
    }

    private Coordinator loadCoordinator(Properties properties) throws IOException {
        int tcpPort = Integer.parseInt( properties.getProperty("tcp_port") );
        ServerNode serverIdentifier = new ServerNode( "localhost", tcpPort );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG updatePriceDag =  TransactionBootstrap.name(UPDATE_PRICE)
                .input( "a", "product", UPDATE_PRICE )
                .terminal("b", "cart", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(updatePriceDag.name, updatePriceDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        String productHost = properties.getProperty("product_host");
        String cartHost = properties.getProperty("cart_host");

        IdentifiableNode productAddress = new IdentifiableNode("product", productHost, PRODUCT_VMS_PORT);
        IdentifiableNode cartAddress = new IdentifiableNode("cart", cartHost, CART_VMS_PORT);

        Map<String, IdentifiableNode> starterVMSs = new HashMap<>(2);
        starterVMSs.put(productAddress.identifier, productAddress);
        starterVMSs.put(cartAddress.identifier, cartAddress);

        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int batchSendRate = Integer.parseInt( properties.getProperty("batch_window_ms") );
        int groupPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );
        boolean logging = false;
        String loggingVal = System.getProperty("logging");
        if(loggingVal != null) logging = Boolean.parseBoolean(loggingVal);

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions()
                        .withBatchWindow(batchSendRate)
                        .withNetworkThreadPoolSize(groupPoolSize)
                        .withNetworkBufferSize(networkBufferSize)
                        .withLogging(logging),
                1,
                1,  ignored -> new IHttpHandler() { },
                serdes
        );
    }

    private static class PriceUpdateProducer implements Runnable {

        private final Coordinator coordinator;

        public PriceUpdateProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );
            int val = 1;
            while(val < 10) {
                PriceUpdate priceUpdate = new PriceUpdate(
                        val,1,10.0F, "1", String.valueOf(val) );
                String payload = serdes.serializeAsString(priceUpdate, PriceUpdate.class);
                TransactionInput.Event eventPayload = new TransactionInput.Event(UPDATE_PRICE, payload);
                TransactionInput txInput = new TransactionInput(UPDATE_PRICE, eventPayload);
                LOGGER.log(INFO, "[Producer] Adding "+val);
                this.coordinator.queueTransactionInput(txInput);
                val++;
            }
            LOGGER.log(INFO, "Producer going to bed definitely... ");
        }
    }

}
