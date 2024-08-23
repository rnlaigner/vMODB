package dk.ku.di.dms.vms.marketplace.seller.infra;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.seller.SellerService;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class SellerHttpServerVertx extends AbstractVerticle {

    static VmsApplication SELLER_VMS;
    static SellerService SELLER_SERVICE;

    public static void init(VmsApplication sellerVms, int numVertices, boolean nativeTransport){
        SELLER_VMS = sellerVms;
        var optService = SELLER_VMS.<SellerService>getService();
        if(optService.isEmpty()){
            throw new RuntimeException("Could not find service for SellerService");
        }
        SELLER_SERVICE = optService.get();
        Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(nativeTransport));
        boolean usingNative = vertx.isNativeTransportEnabled();
        System.out.println("Vertx is running with native: " + usingNative);
        DeploymentOptions deploymentOptions = new DeploymentOptions()
                .setThreadingModel(ThreadingModel.EVENT_LOOP)
                .setInstances(numVertices);

        try {
            vertx.deployVerticle(SellerHttpServerVertx.class,
                            deploymentOptions
                    )
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start(Promise<Void> startPromise) {
        HttpServerOptions options = new HttpServerOptions();
        options.setPort(Constants.SELLER_HTTP_PORT);
        options.setHost("localhost");
        options.setTcpKeepAlive(true);
        HttpServer server = this.vertx.createHttpServer(options);
        server.requestHandler(new VertxHandler());
        server.listen(res -> {
            if (res.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(res.cause());
            }
        });
    }

    public static class VertxHandler implements Handler<HttpServerRequest> {
        @Override
        public void handle(HttpServerRequest exchange) {
            String[] uriSplit = exchange.uri().split("/");
            exchange.bodyHandler(buff -> {
                assert uriSplit[1].equals("seller");
                assert (exchange.method().name().equals("GET"));
                String[] split = exchange.uri().split("/");
                assert split[2].contentEquals("dashboard");
                int sellerId = Integer.parseInt(split[split.length - 1]);
                long tid = SELLER_VMS.lastTidFinished();
                try (var txCtx = SELLER_VMS.getTransactionManager().beginTransaction(tid, 0, tid, true)) {
                    var view = SELLER_SERVICE.queryDashboardNoApp(sellerId);
                    exchange.response().setChunked(true);
                    exchange.response().setStatusCode(200);
                    exchange.response().write(view.toString());
                    exchange.response().end();
                } catch (IOException e) {
                    exchange.response().setChunked(true);
                    exchange.response().setStatusCode(500);
                    exchange.response().write(e.getMessage());
                    exchange.response().end();
                }
            });
        }
    }

}
