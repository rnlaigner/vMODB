package dk.ku.di.dms.vms.marketplace.seller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.seller.dtos.OrderSellerView;
import dk.ku.di.dms.vms.marketplace.seller.dtos.SellerDashboard;
import dk.ku.di.dms.vms.marketplace.seller.entities.OrderEntry;
import dk.ku.di.dms.vms.marketplace.seller.entities.Seller;
import dk.ku.di.dms.vms.marketplace.seller.infra.SellerHttpServerVertx;
import dk.ku.di.dms.vms.marketplace.seller.repositories.IOrderEntryRepository;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.web_common.IHttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import static dk.ku.di.dms.vms.marketplace.seller.SellerService.EMPTY_DASHBOARD;
import static dk.ku.di.dms.vms.marketplace.seller.SellerService.SELLER_VIEW_BASE;
import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VmsApplication vms = initVms(properties);
//        initHttpServer(properties, vms);
    }

    private static VmsApplication initVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.SELLER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        VmsApplication vms = VmsApplication.build(options,
                (x,z) -> new SellerHttpHandlerJdk2(x, (IOrderEntryRepository) z.apply("order_entries")));
        vms.start();
        return vms;
    }

    private static class SellerHttpHandlerJdk2 implements IHttpHandler {
        private final ITransactionManager transactionManager;
        private final IOrderEntryRepository repository;

        public SellerHttpHandlerJdk2(ITransactionManager transactionManager,
                                    IOrderEntryRepository repository){
            this.transactionManager = transactionManager;
            this.repository = repository;
        }

        @Override
        public void patch(String uri, String body) {
            this.transactionManager.reset();
        }

        @Override
        public String getAsJson(String uri) throws Exception {
            String[] uriSplit = uri.split("/");
            int sellerId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
            try (var txCtx = this.transactionManager.beginTransaction(0, 0, 0, true)) {
                List<OrderEntry> orderEntries = this.repository.getOrderEntriesBySellerId(sellerId);
                if(orderEntries.isEmpty()) return EMPTY_DASHBOARD.toString();
                LOGGER.log(INFO, "APP: Seller "+sellerId+" has "+orderEntries.size()+" entries in seller dashboard");
                OrderSellerView view = this.repository.fetchOne(SELLER_VIEW_BASE.setParam(sellerId), OrderSellerView.class);
                return new SellerDashboard(view, orderEntries).toString();
            }
        }
    }

    private static void initHttpServer(Properties properties, VmsApplication vms) throws IOException {
        String httpServer = properties.getProperty("http_server");
        if(httpServer == null || httpServer.isEmpty()){
            throw new RuntimeException("http_server property is missing");
        }
        if(httpServer.equalsIgnoreCase("vertx")){
            int httpThreadPoolSize = Integer.parseInt( properties.getProperty("http_thread_pool_size") );
            int numVertices = Integer.parseInt( properties.getProperty("num_vertices") );
            boolean nativeTransport = Boolean.parseBoolean( properties.getProperty("native_transport") );
            SellerHttpServerVertx.init(vms, numVertices, httpThreadPoolSize, nativeTransport);
            LOGGER.log(INFO,"Seller: Vertx HTTP Server started");
            return;
        }
        if(httpServer.equalsIgnoreCase("jdk")){
            int backlog = Integer.parseInt( properties.getProperty("backlog") );
            initHttpServerJdk(vms, backlog);
            LOGGER.log(INFO,"Seller: JDK HTTP Server started");
            return;
        }
        throw new RuntimeException("http_server property is unknown: "+ httpServer);
    }

    private static void initHttpServerJdk(VmsApplication vms, int backlog) throws IOException {
        // initialize HTTP server to serve seller dashboard online requests
        System.setProperty("sun.net.httpserver.nodelay","true");
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", Constants.SELLER_HTTP_PORT), backlog);
        httpServer.createContext("/seller", new SellerHttpHandler(vms));
        httpServer.start();
    }

    private static class SellerHttpHandler implements HttpHandler {

        private final Table sellerTable;

        private final AbstractProxyRepository<Integer, Seller> sellerRepository;

        private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        private final SellerService sellerService;

        private final VmsApplication vms;

        @SuppressWarnings("unchecked")
        private SellerHttpHandler(VmsApplication vms) {
            this.vms = vms;
            var optService = vms.<SellerService>getService();
            if(optService.isEmpty()){
                throw new RuntimeException("Could not find service for SellerService");
            }
            this.sellerService = optService.get();
            this.sellerTable = vms.getTable("sellers");
            this.sellerRepository = (AbstractProxyRepository<Integer, Seller>) vms.getRepositoryProxy("sellers");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()){
                case "GET": {
                    //  seller dashboard. send fetch and fetchMany directly to transaction manager?
                    //  one way: the tx manager assigns the last finished tid to thread, thus obtaining the freshest snapshot possible
                    // another way: reentrant locks in the application code
                    String[] split = exchange.getRequestURI().toString().split("/");
                    if(split[2].contentEquals("dashboard")){
                        int sellerId = Integer.parseInt(split[split.length - 1]);
                        // register a transaction with the last tid finished
                        // this allows to get the freshest view, bypassing the scheduler
                        long lastTid = this.vms.lastTidFinished();
                        try(var txCtx = this.vms.getTransactionManager().beginTransaction(lastTid, 0, lastTid, true)) {
                            SellerDashboard view = this.sellerService.queryDashboard(sellerId);
                            // parse and return result
                            OutputStream outputStream = exchange.getResponseBody();
                            exchange.sendResponseHeaders(200, 0);
                            outputStream.write( view.toString().getBytes(StandardCharsets.UTF_8) );
                            outputStream.close();
                        }
                    } else {
                        int sellerId = Integer.parseInt(split[split.length - 1]);
                        IKey key = SimpleKey.of( sellerId );
                        // bypass transaction manager
                        Object[] record = this.sellerTable.underlyingPrimaryKeyIndex().lookupByKey(key);
                        var entity = this.sellerRepository.parseObjectIntoEntity(record);
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(200, 0);
                        outputStream.write( entity.toString().getBytes(StandardCharsets.UTF_8) );
                        outputStream.flush();
                        outputStream.close();
                        return;
                    }
                }
                case "POST": {
                    String str = new String( exchange.getRequestBody().readAllBytes() );
                    Seller seller = serdes.deserialize(str, Seller.class);

                    Object[] obj = this.sellerRepository.extractFieldValuesFromEntityObject(seller);
                    IKey key = KeyUtils.buildRecordKey( sellerTable.schema().getPrimaryKeyColumns(), obj );
                    this.sellerTable.underlyingPrimaryKeyIndex().insert(key, obj);

                    // response
                    OutputStream outputStream = exchange.getResponseBody();
                    exchange.sendResponseHeaders(200, 0);
                    outputStream.flush();
                    outputStream.close();
                    return;
                }
            }
            // failed response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(404, 0);
            outputStream.flush();
            outputStream.close();
        }
    }

}
