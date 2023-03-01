package dk.ku.di.dms.vms.coordinator;

import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.http.EdgeHttpServerBuilder;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Main
{

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public record StartupConfig (
            boolean runElection,
            NetworkAddress me,
            List<NetworkAddress> starterVMSs,
            Map<String, TransactionDAG> transactionMap,
            CoordinatorOptions options
    ){}

    private static final StartupConfig defaultConfig = new StartupConfig(false, new NetworkAddress("127.0.0.1", 8080), Collections.emptyList(), new HashMap<>(), new CoordinatorOptions());

    public static void main( String[] args ) throws InterruptedException, IOException {

        // https://stackoverflow.com/questions/42426206/httpserver-very-slow-with-keepalive
        System.setProperty("sun.net.httpserver.nodelay", "true");

        IVmsSerdesProxy serdesProxy = VmsSerdesProxyBuilder.build();
        StartupConfig startupConfig;
        if(args.length == 1){
            startupConfig = serdesProxy.deserialize(args[0], StartupConfig.class);
        } else {
            startupConfig = defaultConfig;
        }

        BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();
        Map<String, TransactionDAG> transactionMap = new ConcurrentHashMap<>();

        if(!startupConfig.runElection()){

            // just set up the http server then
            try {
                HttpServer httpServer = EdgeHttpServerBuilder.start(serdesProxy, parsedTransactionRequests, transactionMap,
                        new InetSocketAddress(InetAddress.getByName("localhost"), 8001));
                logger.info("Http server initialized");
            } catch (IOException e){
                logger.warn("ERROR: "+e.getMessage());
                return;
            }
        }

        /*
        HttpClient client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder().uri(URI.create("http://localhost:8001/data")).POST(HttpRequest.BodyPublishers.ofString("TESTE")).build();
        client.send( request, HttpResponse.BodyHandlers.discarding());
        */
        Thread.sleep(Long.MAX_VALUE);

        // the input a new-order transaction
//        a payload containing these 4 events {
//
//
//        "a", "customer", "customer-new-order-in" )
//                .input("b", "item","item-new-order-in" )
//                .input( "c", "stock","stock-new-order-in" )
//                .input( "d", "warehouse", "waredist-new-order-in" )
//    }
        // assemble this into a new new-order tx, giving a unique id

        // the coordinator receives many events ... it maintains these received events in main memory, and then it forwards to the respective dbms proxies

        // at some point we do a checkpoint

        // a checkpoint is a batch of received events (transactions), the dbms proxies advances in time at every checkpoint

        // between checkpoints there is no guarantee, if failure, the dbms proxies they have to restore the old snpashot (checkpoint)

        // when failure, after coming back, the coordinator asks the dbms proxies about the current global state

        // deterministic execution, no 2-phase commit
        // in the coordinator, we batch the events, try to commit in batches, one at a time

        // we rollback the dbms proxies to the last checkpointed state (state + events applied so far at that time)

        // write down the protocol carefully
        // coordinators are stateless
        // if the coordinator is stateless, how can it ensure correctness?

        // the batch commit would require 2-PC?

        // write the protocol... just rely on the proxy, why do we need the coordinator? to sequence the events... and forward the events to other vms
        //  how can it be stateless if we need to remember the sequence of events?
        //  the sequence of events is stored together with the batch commit in each dbms proxy

        // we fix the problem of durability first
        // batch logging is fine, still need to logged it!

        // can we combine the proxies and the coordinator?
        // is that some microservice may require a lot of data processing.
        // and increasing the number of tasks (sequencing) in the proxy, may be an overkill for the application,
        // but you can have multiple proxies doing the work, one dbms proxy
        // separate functionality among the proxies

        /*
        ElectionWorker electionManager = null;

        while(true) {

            // TODO initiate election
            // depending on the result, setup the correct thread

            // will block until the election protocol has finished

            electionManager.getResult();

            if (electionManager.getState() == LEADER) {
                // setup leader
                // Leader leader = new Leader();
                // setup http server for incoming transactions. followers must redirect the request to the leader for now
            } else {
                // setup follower
            }


            // this block of code returns because the heartbeat from the leader did not come in time
            // so time for a new election
            // we set up a new election worker and repeat the process

        }
         */

    }
}
