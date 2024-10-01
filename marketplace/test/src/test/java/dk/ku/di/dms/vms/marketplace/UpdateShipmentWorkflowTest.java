package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.inputs.UpdateDelivery;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

/**
 * This test composes with the checkout workflow.
 * Why? Because it is necessary to have orders. These
 * eventually leads to shipments and associated packages.
 * The packages are then progressively updated
 */
public non-sealed class UpdateShipmentWorkflowTest extends CheckoutWorkflowTest {

    /**
     * Overriding because this test only intends to inherit the behavior of checkout workflow, not testing it
     */
    @Override
    public void testCheckout() {
        assert true;
    }

    @Test
    public void testUpdateShipment() throws Exception {

        this.initVMSs();

        Coordinator coordinator = this.loadCoordinator();
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        LOGGER.log(INFO, "Triggering checkout workflow...");
        this.triggerCheckoutWorkflow(coordinator);

        LOGGER.log(INFO, "Waiting batch window interval...");
        sleep(BATCH_WINDOW_INTERVAL * 2);

        LOGGER.log(INFO, "Sending update shipment event...");
        // now send the update shipment event
        new UpdateShipmentProducer(coordinator).run();

        sleep(BATCH_WINDOW_INTERVAL * 2);

        Assert.assertEquals(3, coordinator.getBatchOffsetPendingCommit());
        Assert.assertEquals(11, coordinator.getNumTIDsCommitted());
    }

    @Override
    @SuppressWarnings("BusyWait")
    protected void triggerCheckoutWorkflow(Coordinator coordinator) throws Exception {
        int numStarterVMSs = coordinator.getStarterVMSs().size();
        do{
            sleep(2000);
        } while (coordinator.getConnectedVMSs().size() < numStarterVMSs);
        new ReserveStockProducer(coordinator).run();
    }

    private static final Random RANDOM = new Random();

    private static class ReserveStockProducer implements Runnable {

        private final String NAME = ReserveStockProducer.class.getSimpleName();

        Coordinator coordinator;

        public ReserveStockProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {
            LOGGER.log(INFO, "["+ NAME +"] Starting...");
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
            int val = 1;
            while(val <= 10) {
                // reserve stock
                ReserveStock reserveStockEvent = new ReserveStock(
                        new Date(), CUSTOMER_CHECKOUT_FUNCTION.apply( RANDOM.nextInt(1,MAX_CUSTOMERS+1) ),
                        List.of(
                                new CartItem(val,1,"test",
                                        1.0f, 1.0f, 1, 1.0f, "1")
                        ),
                        String.valueOf(val)
                );
                String payload_ = serdes.serializeAsString(reserveStockEvent, ReserveStock.class);
                TransactionInput.Event eventPayload_ = new TransactionInput.Event("reserve_stock", payload_);
                TransactionInput txInput_ = new TransactionInput("customer_checkout", eventPayload_);
                LOGGER.log(INFO, "["+ NAME +"] New reserve stock event with version: "+val);
                coordinator.queueTransactionInput(txInput_);
                val++;
            }
            LOGGER.log(INFO, "["+ NAME +"] Going to bed definitely...");
        }
    }

    private static class UpdateShipmentProducer implements Runnable {

        private final String NAME = UpdateShipmentProducer.class.getSimpleName();

        Coordinator coordinator;

        public UpdateShipmentProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
            LOGGER.log(INFO, "["+ NAME +"] Starting...");
            UpdateDelivery updateDelivery = new UpdateDelivery("11");
            String payload_ = serdes.serializeAsString(updateDelivery, UpdateDelivery.class);
            // event name
            TransactionInput.Event eventPayload_ = new TransactionInput.Event(UPDATE_DELIVERY, payload_);

            // transaction name
            TransactionInput txInput_ = new TransactionInput(UPDATE_DELIVERY, eventPayload_);

            LOGGER.log(INFO, "["+ NAME +"] New update shipment event with version 11");
            coordinator.queueTransactionInput(txInput_);

            LOGGER.log(INFO, "["+ NAME +"] Going to bed definitely... ");
        }
    }

    private void initVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);
        insertItemsInStockVms();
        insertCustomersInCustomerVms();
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8091 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG checkoutDag =  TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                .input( "a", "stock", RESERVE_STOCK )
                .internal("b", "order", STOCK_CONFIRMED, "a")
                .internal("c", "payment", INVOICE_ISSUED, "b")
                .terminal( "d", "customer", "c" )
                .terminal( "e", "shipment",  "c" )
                .build();

        TransactionDAG updateShipmentDag =  TransactionBootstrap.name(UPDATE_DELIVERY)
                .input( "a", "shipment", UPDATE_DELIVERY)
                // .terminal("b", "seller", "a")
                .terminal( "c", "customer", "a" )
                .terminal( "d", "order",  "a" )
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(checkoutDag.name, checkoutDag);
        transactionMap.put(updateShipmentDag.name, updateShipmentDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<String, IdentifiableNode> starterVMSs = getIdentifiableNodeMap();

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(BATCH_WINDOW_INTERVAL),
                1,
                1,  ignored -> new IHttpHandler() { },
                serdes
        );
    }

    private static Map<String, IdentifiableNode> getIdentifiableNodeMap() {
        Map<String, IdentifiableNode> starterVMSs = new HashMap<>(10);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", Constants.STOCK_VMS_PORT);
        starterVMSs.put(stockAddress.identifier, stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", Constants.ORDER_VMS_PORT);
        starterVMSs.put(orderAddress.identifier, orderAddress);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", "localhost", Constants.PAYMENT_VMS_PORT);
        starterVMSs.put(paymentAddress.identifier, paymentAddress);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment","localhost", Constants.SHIPMENT_VMS_PORT);
        starterVMSs.put(shipmentAddress.identifier, shipmentAddress);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", "localhost", Constants.CUSTOMER_VMS_PORT);
        starterVMSs.put(customerAddress.identifier, customerAddress);
        return starterVMSs;
    }

}
