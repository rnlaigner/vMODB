package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
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
 * This class tests a subset of the checkout workflow
 */
public class StockOrderPaymentCustomerShipmentTest extends AbstractWorkflowTest {

    private void initVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);
        dk.ku.di.dms.vms.marketplace.payment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.shipment.Main.main(null);
        dk.ku.di.dms.vms.marketplace.customer.Main.main(null);
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

    /**  // Another way to define the DAG
         TransactionDAG checkoutDag =  TransactionBootstrap.name("customer_checkout")
         .input( "a", "stock", "reserve_stock" )
         .internal("b", "order", "stock_confirmed", "a")
         .internal("c", "seller", "invoice_issued", "b")
         .internal("c", "payment", "invoice_issued", "b")
         .internal("c", "seller", "payment_confirmed", "c")
         .internal( "e", "shipment",  "c" )
         .terminal("c", "seller", "shipment_notification", "e")
         .terminal( "d", "customer", "c" )
         .build();
     */
    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8080 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG checkoutDag =  TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                .input( "a", "stock", RESERVE_STOCK)
                .internal("b", "order", STOCK_CONFIRMED, "a")
                .internal("c", "payment", INVOICE_ISSUED, "b")
                .terminal( "d", "customer", "c" )
                .terminal( "e", "shipment",  "c" )
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
                new CoordinatorOptions().withBatchWindow(3000),
                1,
                1,
                c -> new IHttpHandler() { },
                serdes
        );
    }

    private static Map<String, IdentifiableNode> getStaterVMSs() {
        Map<String, IdentifiableNode> starterVMSs = new HashMap<>();
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", STOCK_VMS_PORT);
        starterVMSs.put(stockAddress.identifier, stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", ORDER_VMS_PORT);
        starterVMSs.put(orderAddress.identifier, orderAddress);
        IdentifiableNode paymentAddress = new IdentifiableNode("payment", "localhost", PAYMENT_VMS_PORT);
        starterVMSs.put(paymentAddress.identifier, paymentAddress);
        IdentifiableNode shipmentAddress = new IdentifiableNode("shipment", "localhost", SHIPMENT_VMS_PORT);
        starterVMSs.put(shipmentAddress.identifier, shipmentAddress);
        IdentifiableNode customerAddress = new IdentifiableNode("customer", "localhost", CUSTOMER_VMS_PORT);
        starterVMSs.put(customerAddress.identifier, customerAddress);
        return starterVMSs;
    }

}
