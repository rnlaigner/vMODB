package dk.ku.di.dms.vms.playground.scenario1;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.playground.app.EventExample;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbeddedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbeddedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 *
 * Scenario 1: One VMS and a coordinator. One transaction, running in the vms
 * -
 * "Unix-based systems declare ports below 1024 as privileged"
 * <a href="https://stackoverflow.com/questions/25544849/java-net-bindexception-permission-denied-when-creating-a-serversocket-on-mac-os">...</a>
 *
 */
public class App 
{

    static final Logger logger = LoggerFactory.getLogger(App.class);

    // input transactions
    private static final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    public static void main( String[] args ) throws Exception {

        loadMicroservice();

        loadCoordinator();

        Thread producerThread = new Thread(new Producer());
        producerThread.start();

    }

    private static class Producer implements Runnable {

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(true) {

                if(val < 3) {

                    EventExample eventExample = new EventExample(val);

                    String payload = serdes.serialize(eventExample, EventExample.class);

                    TransactionInput.Event eventPayload = new TransactionInput.Event("in", payload);

                    TransactionInput txInput = new TransactionInput("example", eventPayload);

                    logger.info("[Producer] Adding " + val);

                    parsedTransactionRequests.add(txInput);

                }

                try {
                    //logger.info("Producer going to bed... ");
                    Thread.sleep(10000);
                    //logger.info("Producer woke up! Time to insert one more ");
                } catch (InterruptedException ignored) { }

                val++;

            }
        }
    }

    private static void loadCoordinator() throws IOException {

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 1081 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverEm1.hashCode(), serverEm1);

        NetworkAddress vms = new NetworkAddress("localhost", 1080);

        Map<Integer, NetworkAddress> VMSs = new HashMap<>(1);
        VMSs.put(vms.hashCode(), vms);

        TransactionDAG dag =  TransactionBootstrap.name("example")
                .input( "a", "example", "in" )
                // bad way to do it for single-microservice transactions
                .terminal("t", "example", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>(1);
        transactionMap.put("example", dag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Coordinator coordinator = Coordinator.buildDefault(
                serverMap,
                null,
                VMSs,
                transactionMap,
                serverEm1,
                new CoordinatorOptions(),
                1,
                1,
                App.parsedTransactionRequests,
                serdes
        );

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

    }

    /**
     * Load one microservice at first and perform several transactions and batch commit
     */
    private static void loadMicroservice() throws Exception {

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata("dk.ku.di.dms.vms.playground.app");

        TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacadeAndInjectIntoRepositories(vmsMetadata);

        ExecutorService vmsAppLogicTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        VmsEmbeddedInternalChannels vmsInternalPubSubService = new VmsEmbeddedInternalChannels();

        VmsTransactionScheduler scheduler =
                new VmsTransactionScheduler(
                        vmsAppLogicTaskPool,
                        vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(),
                        null, null);

        VmsNode vmsIdentifier = new VmsNode(
                "localhost", 1080, "example",
                0, 0,0,
                vmsMetadata.tableSchema(), vmsMetadata.replicatedTableSchema(),
                vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        EmbeddedVmsEventHandler eventHandler = EmbeddedVmsEventHandler.buildWithDefaults(
                vmsIdentifier, null, transactionFacade, transactionFacade, vmsInternalPubSubService, vmsMetadata, serdes, socketPool );

        Thread eventHandlerThread = new Thread(eventHandler);
        eventHandlerThread.start();

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();

    }

}
