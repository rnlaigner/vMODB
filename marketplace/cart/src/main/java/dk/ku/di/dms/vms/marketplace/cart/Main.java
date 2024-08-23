package dk.ku.di.dms.vms.marketplace.cart;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.cart.infra.CartHttpServerVertx;
import dk.ku.di.dms.vms.marketplace.cart.infra.CartUtils;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.CompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ForkJoinPool;

import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] ignoredArgs) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "localhost",
                Constants.CART_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.cart",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        VmsApplication vms = VmsApplication.build(options);
        vms.start();
        // initHttpServerJdk(vms);
        initHttpServerVertx(vms);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            vms.stop();
            LOGGER.log(INFO, "Cart terminating ...");
        }));
    }

    private static void initHttpServerVertx(VmsApplication vms){
         CartHttpServerVertx.init(vms, 4, true);
    }

    private static void initHttpServerJdk(VmsApplication vms) throws IOException {
        // initialize HTTP server for data ingestion
        System.setProperty("sun.net.httpserver.nodelay","true");
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", Constants.CART_HTTP_PORT), 0);
        httpServer.createContext("/cart", new CartHttpHandlerJdk(vms));
        httpServer.setExecutor(ForkJoinPool.commonPool());
        httpServer.start();
        LOGGER.log(INFO, "Cart HTTP Server initialized");
    }

    private static class CartHttpHandlerJdk implements HttpHandler {

        private final Table table;
        private final AbstractProxyRepository<CartItem.CartItemId, CartItem> repository;
        private static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();
        private final VmsApplication vms;

        @SuppressWarnings("unchecked")
        public CartHttpHandlerJdk(VmsApplication vms){
            this.vms = vms;
            this.table = vms.getTable("cart_items");
            this.repository = (AbstractProxyRepository<CartItem.CartItemId, CartItem>) vms.getRepositoryProxy("cart_items");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()) {
                case "GET": {
                    String[] split = exchange.getRequestURI().toString().split("/");
                    int customerId = Integer.parseInt(split[split.length - 3]);
                    int sellerId = Integer.parseInt(split[split.length - 2]);
                    int productId = Integer.parseInt(split[split.length - 1]);

                    CartItem entity = getCartItem(sellerId, productId, customerId);
                    if (entity == null) {
                        returnFailed(exchange);
                        return;
                    }

                    try {
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                        outputStream.write( entity.toString().getBytes(StandardCharsets.UTF_8) );
                        outputStream.close();
                    } catch(Exception e) {
                        returnFailed(exchange);
                    }
                    break;
                }
                case "PATCH": {
                    String[] split = exchange.getRequestURI().toString().split("/");

                    try {
                        int customerId = Integer.parseInt(split[split.length - 2]);
                        String str = new String(exchange.getRequestBody().readAllBytes());
                        dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI =
                                SERDES.deserialize(str, dk.ku.di.dms.vms.marketplace.common.entities.CartItem.class);
                        this.processAddCartItem(customerId, cartItemAPI);

                        // response
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(200, 0);
                        outputStream.close();
                    } catch(Exception e){
                        returnFailed(exchange);
                    }
                    break;
                }
                default: {
                    returnFailed(exchange);
                }
            }
        }

        private CartItem getCartItem(int sellerId, int productId, int customerId) {
            Object[] obj = new Object[3];
            obj[0] = sellerId;
            obj[1] = productId;
            obj[2] = customerId;

            IKey key = CompositeKey.of( obj );

            long tid = this.vms.lastTidFinished();
            try(var txCtx = (TransactionContext) this.vms.getTransactionManager().beginTransaction( tid, 0, tid,true )) {
                Object[] record = this.table.primaryKeyIndex().lookupByKey(txCtx, key);
                if (record == null) {
                    return null;
                }
                return this.repository.parseObjectIntoEntity(record);
            }
        }

        private void processAddCartItem(int customerId,
                                        dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI) {
            // get last tid executed to bypass transaction scheduler
            long tid = this.vms.lastTidFinished();
            // can ask the transactional handler
            try(var txCtx = (TransactionContext) this.vms.getTransactionManager().beginTransaction(tid, 0, tid,false)) {
                this.repository.insert(CartUtils.convertCartItemAPI(customerId, cartItemAPI));
                this.vms.getTransactionManager().commit();
            }
        }

        private static void returnFailed(HttpExchange exchange) throws IOException {
            // failed response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
            outputStream.close();
        }

    }

}
