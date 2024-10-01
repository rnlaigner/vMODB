package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.marketplace.common.entities.OrderItem;
import dk.ku.di.dms.vms.marketplace.common.events.InvoiceIssued;
import dk.ku.di.dms.vms.marketplace.common.inputs.CustomerCheckout;
import dk.ku.di.dms.vms.marketplace.seller.entities.Seller;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static dk.ku.di.dms.vms.marketplace.common.Constants.*;
import static java.lang.Thread.sleep;

public final class SellerViewTest extends AbstractWorkflowTest {

    @Test
    public void test() throws Exception {

        dk.ku.di.dms.vms.marketplace.seller.Main.main(null);
        this.insertSellersInSellerVms();

        Coordinator coordinator = this.loadCoordinator();
        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        for(int i = 1; i <= MAX_SELLERS; i++) {
            CustomerCheckout customerCheckout = CUSTOMER_CHECKOUT_FUNCTION.apply(1);
            InvoiceIssued invoiceIssued = new InvoiceIssued( customerCheckout, i,  "test", new Date(), 100,
                    List.of(new OrderItem(i,1,1, "name",
                            i, 1.0f, new Date(), 1.0f, 1, 1.0f, 1.0f, 0.0f) )
                    , String.valueOf(i));

            String payload_ = serdes.serializeAsString(invoiceIssued, InvoiceIssued.class);
            TransactionInput.Event eventPayload_ = new TransactionInput.Event(INVOICE_ISSUED, payload_);

            TransactionInput txInput_ = new TransactionInput(UPDATE_DELIVERY, eventPayload_);

            coordinator.queueTransactionInput(txInput_);
        }

        // wait for commit
        sleep(BATCH_WINDOW_INTERVAL * 3);

        try (HttpClient client = HttpClient.newHttpClient()) {
            var request = HTTP_REQUEST_DASHBOARD_SUPPLIER.apply(1);
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Assert.assertEquals(200, response.statusCode());
        }
    }

    private Coordinator loadCoordinator() throws IOException {
        ServerNode serverIdentifier = new ServerNode( "localhost", 8080 );

        Map<Integer, ServerNode> serverMap = new HashMap<>(2);
        serverMap.put(serverIdentifier.hashCode(), serverIdentifier);

        TransactionDAG simpleDAG =  TransactionBootstrap.name(UPDATE_DELIVERY)
                .input( "a", "seller", INVOICE_ISSUED)
                .terminal("b", "seller", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>();
        transactionMap.put(simpleDAG.name, simpleDAG);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        Map<String, IdentifiableNode> starterVMSs = new HashMap<>(3);
        IdentifiableNode sellerAddress = new IdentifiableNode("seller", "localhost", SELLER_VMS_PORT);
        starterVMSs.put(sellerAddress.identifier, sellerAddress);

        return Coordinator.build(
                serverMap,
                starterVMSs,
                transactionMap,
                serverIdentifier,
                new CoordinatorOptions().withBatchWindow(3000),
                1,
                1,  ignored -> new IHttpHandler() { },
                serdes
        );
    }

    private static final Function<Integer, HttpRequest> HTTP_REQUEST_DASHBOARD_SUPPLIER = sellerId -> HttpRequest.newBuilder(
                    URI.create( "http://localhost:8007/seller/dashboard/"+sellerId ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .GET()
            .build();

    private static final Function<String, HttpRequest> HTTP_REQUEST_SELLER_SUPPLIER = str -> HttpRequest.newBuilder(
                    URI.create( "http://localhost:8007/seller" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

    private void insertSellersInSellerVms() throws IOException, InterruptedException {
        try (HttpClient client = HttpClient.newHttpClient()) {
            String str;
            for (int i = 1; i <= MAX_SELLERS; i++) {
                str = new Seller(i, "test", "test", "test",
                        "test", "test", "test", "test",
                        "test", "test", "test", "test", "test").toString();
                HttpRequest sellerReq = HTTP_REQUEST_SELLER_SUPPLIER.apply(str);
                client.send(sellerReq, HttpResponse.BodyHandlers.ofString());
            }
        }
    }

}
