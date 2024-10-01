package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
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

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

/**
 * This class tests the full checkout workflow
 * That is, all VMSs and events part of checkout
 */
public sealed class CheckoutWorkflowTest extends AbstractWorkflowTest permits UpdateShipmentWorkflowTest {

    private void initVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.cart.Main.main(null);
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);
        dk.ku.di.dms.vms.marketplace.seller.Main.main(null);
        insertItemsInStockVms();
        insertCustomersInCustomerVms();
    }

    @Test
    public void testCheckout() throws Exception {
        this.initVMSs();
        Coordinator coordinator = this.loadCoordinator();
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();
        this.triggerCheckoutWorkflow(coordinator);
        sleep(BATCH_WINDOW_INTERVAL * 5);
        Assert.assertEquals(2, coordinator.getBatchOffsetPendingCommit());
        Assert.assertEquals(10, coordinator.getNumTIDsCommitted());
    }

    @SuppressWarnings("BusyWait")
    protected void triggerCheckoutWorkflow(Coordinator coordinator) throws Exception {
        int numStarterVMSs = coordinator.getStarterVMSs().size();
        do{
            sleep(2000);
        } while (coordinator.getConnectedVMSs().size() < numStarterVMSs);
        new CustomerCheckoutProducer(coordinator).run();
    }

    private static class CustomerCheckoutProducer implements Runnable {

        private final String NAME = CustomerCheckoutProducer.class.getSimpleName();

        private final Coordinator coordinator;

        public CustomerCheckoutProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {
            LOGGER.log(INFO, "["+this.NAME +"] Starting...");
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
            int val = 1;
            while(val <= 10) {
                // add item
                try {
                    insertCartItemInCartVms(val);
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // customer checkout
                var custCheckout = CUSTOMER_CHECKOUT_FUNCTION.apply( val );

                String checkoutPayload = serdes.serializeAsString(custCheckout, CustomerCheckout.class);
                TransactionInput.Event eventPayload_ = new TransactionInput.Event(CUSTOMER_CHECKOUT, checkoutPayload);

                TransactionInput txInput_ = new TransactionInput(CUSTOMER_CHECKOUT, eventPayload_);
                LOGGER.log(INFO, "["+this.NAME +"] New customer checkout event with version: "+val);
                coordinator.queueTransactionInput(txInput_);
                val++;
            }
            LOGGER.log(INFO, "["+this.NAME +"] Going to bed definitely...");
        }
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8091 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG checkoutDag =  TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                .input( "a", "cart", CUSTOMER_CHECKOUT)
                .internal( "b", "stock", RESERVE_STOCK, "a")
                .internal("c", "order", STOCK_CONFIRMED, "b")
                .internal("d", "payment", INVOICE_ISSUED, "c")
                .terminal("e", "seller", "c")
                .terminal( "f", "customer", "d" )
                .terminal( "g", "shipment",  "d" )
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(checkoutDag.name, checkoutDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<String, IdentifiableNode> starterVMSs = getStaterVMSs();

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(BATCH_WINDOW_INTERVAL),
                1,
                1,
                ignored -> new IHttpHandler() { },
                serdes
        );
    }

    private static Map<String, IdentifiableNode> getStaterVMSs() {
        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();
        IdentifiableNode cartAddress = new IdentifiableNode("cart", "localhost", CART_VMS_PORT);
        starterVMSs.put(cartAddress.identifier, cartAddress);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", STOCK_VMS_PORT);
        starterVMSs.put(stockAddress.identifier, stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", ORDER_VMS_PORT);
        starterVMSs.put(orderAddress.identifier, orderAddress);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", "localhost", PAYMENT_VMS_PORT);
        starterVMSs.put(paymentAddress.identifier, paymentAddress);
        IdentifiableNode sellerAddress = new IdentifiableNode("seller", "localhost", SELLER_VMS_PORT);
        starterVMSs.put(sellerAddress.identifier, sellerAddress);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", "localhost", CUSTOMER_VMS_PORT);
        starterVMSs.put(customerAddress.identifier, customerAddress);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment", "localhost", SHIPMENT_VMS_PORT);
        starterVMSs.put(shipmentAddress.identifier, shipmentAddress);
        return starterVMSs;
    }

}
