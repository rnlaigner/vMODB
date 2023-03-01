package dk.ku.di.dms.vms.sdk.embed.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 *
 */
public final class VmsHttpServer {

    private final Logger logger = LoggerFactory.getLogger("httpServer");

    private final Map<String, IVmsRepositoryFacade> repositoryFacades;
    private final Map<String, Class<?>> tableNameToEntityClazzMap;
    private final IVmsSerdesProxy serdes;

    private final HttpServer server;

    public VmsHttpServer(String host, int port,
                        Map<String, IVmsRepositoryFacade> repositoryFacades,
                        Map<String, Class<?>> tableNameToEntityClazzMap,
                        IVmsSerdesProxy serdes) throws IOException {
        this.repositoryFacades = repositoryFacades;
        this.tableNameToEntityClazzMap = tableNameToEntityClazzMap;
        this.serdes = serdes;
        this.server = HttpServer.create(new InetSocketAddress(host, port), 10);
        this.server.createContext("/", new VmsRequestHandler());
        this.server.setExecutor(Executors.newSingleThreadExecutor());
    }

    public void start(){
        this.server.start();
    }

    public void stop(){
        this.server.stop(0);
    }

    public class VmsRequestHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {

            // find out the table
            String table = exchange.getRequestURI().getPath();



            switch (exchange.getRequestMethod()){

                case "POST": {

                    logger.info("POST request arrived");
                    System.out.println(new String( exchange.getRequestBody().readAllBytes() ));
                    exchange.sendResponseHeaders(200, 0);

                }
                case "GET": {

                    logger.info("GET request arrived");
                    System.out.println(new String( exchange.getRequestBody().readAllBytes() ));
                    exchange.sendResponseHeaders(200, 0);

                }
            }

            exchange.close();
        }
    }

}
