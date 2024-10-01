package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.events.ReserveStock;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dk.ku.di.dms.vms.marketplace.common.Constants.CUSTOMER_CHECKOUT;
import static dk.ku.di.dms.vms.marketplace.common.Constants.RESERVE_STOCK;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.Thread.sleep;

public final class StockOrderWorkflowTest extends AbstractWorkflowTest {

    private static final CustomerCheckout customerCheckout = new CustomerCheckout();

    @Test
    @SuppressWarnings("BusyWait")
    public void testTopologyWithTwoVMSs() throws Exception {
        dk.ku.di.dms.vms.marketplace.stock.Main.main(null);
        dk.ku.di.dms.vms.marketplace.order.Main.main(null);

        insertItemsInStockVms();

        Coordinator coordinator = loadCoordinator();

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        do{
            sleep(5000);
        } while (coordinator.getConnectedVMSs().size() < 2);

        Thread thread = new Thread(new InputProducer(coordinator));
        thread.start();

        sleep(BATCH_WINDOW_INTERVAL * 2);

        Assert.assertEquals(2, coordinator.getBatchOffsetPendingCommit());
        Assert.assertEquals(20, coordinator.getNumTIDsCommitted());
    }

    private static class InputProducer implements Runnable {

        private final Coordinator coordinator;

        public InputProducer(Coordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void run() {
            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );
            int val = 1;
            while(val < 21) {
                // reserve stock
                ReserveStock reserveStockEvent = new ReserveStock(
                        new Date(), customerCheckout,
                        List.of( new CartItem(1,1,"test",1.0f, 1.0f, 1, 1.0f, "test") ),
                        String.valueOf(val)
                );
                String payload_ = serdes.serializeAsString(reserveStockEvent, ReserveStock.class);
                TransactionInput.Event eventPayload_ = new TransactionInput.Event(RESERVE_STOCK, payload_);
                TransactionInput txInput_ = new TransactionInput(CUSTOMER_CHECKOUT, eventPayload_);
                LOGGER.log(INFO, "[CheckoutProducer] New reserve stock event with version: "+val);
                coordinator.queueTransactionInput(txInput_);
                val++;
            }
            LOGGER.log(INFO, "InputProducer going to bed definitely... ");
        }
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8080 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG checkoutDag =  TransactionBootstrap.name(CUSTOMER_CHECKOUT)
                .input( "a", "stock", RESERVE_STOCK)
                .terminal("b", "order", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(checkoutDag.name, checkoutDag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Map<String, IdentifiableNode> VMSs = new HashMap<>(3);
        IdentifiableNode stockAddress = new IdentifiableNode("stock", "localhost", 8082);
        VMSs.put(stockAddress.identifier, stockAddress);
        IdentifiableNode orderAddress = new IdentifiableNode("order", "localhost", 8083);
        VMSs.put(orderAddress.identifier, orderAddress);

        return Coordinator.build(
                serverMap,
                VMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(3000),
                1,
                1,  ignored -> new IHttpHandler() { },
                serdes
        );
    }

}
