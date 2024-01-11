package dk.ku.di.dms.vms.marketplace;

import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.marketplace.product.Product;
import dk.ku.di.dms.vms.marketplace.stock.Stock;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;
import java.util.logging.Logger;

public class AbstractWorkflowTest {

    protected static final int batchWindowInterval = 3000;

    protected static final Logger logger = Logger.getLogger(ProductStockWorkflowTest.class.getCanonicalName());

    protected final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    private static final Function<String, HttpRequest> httpRequestProductSupplier = str -> HttpRequest.newBuilder( URI.create( "http://localhost:8001/product" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

    private static final Function<String, HttpRequest> httpRequestStockSupplier = str -> HttpRequest.newBuilder( URI.create( "http://localhost:8002/stock" ) )
            .header("Content-Type", "application/json").timeout(Duration.ofMinutes(10))
            .version(HttpClient.Version.HTTP_2)
            .POST(HttpRequest.BodyPublishers.ofString( str ))
            .build();

    private static final int MAX_ITEMS = 10;

    protected void ingestDataIntoVMSs() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        String str1;
        String str2;
        for(int i = 1; i <= MAX_ITEMS; i++){
            str1 = new Product( 1, i, "test", "test", "test", "test", 1.0f, 1.0f,  "test", "test" ).toString();
            HttpRequest prodReq = httpRequestProductSupplier.apply(str1);
            client.send(prodReq, HttpResponse.BodyHandlers.ofString());

            str2 = new Stock( 1, i, 100, 0, 0, 0,  "test", "test" ).toString();
            HttpRequest stockReq = httpRequestStockSupplier.apply(str2);
            client.send(stockReq, HttpResponse.BodyHandlers.ofString());
        }
    }

}
