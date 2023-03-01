package dk.ku.di.dms.vms.playground.scenario2;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.meta.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.node.NetworkNode;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 *
 * Scenario 2: Two VMSs and a coordinator.
 * Two transactions:
 * A. Run in the vms 1
 * B. Start at vms 1 and finishes at vms 2
 * -
 * That means vms 2 may receive not sequential tid.
 * Keeping track of the previous tid in the scheduler must occur.
 * -
 * "Unix-based systems declare ports below 1024 as privileged"
 * <a href="https://stackoverflow.com/questions/25544849/java-net-bindexception-permission-denied-when-creating-a-serversocket-on-mac-os">...</a>
 */
public class App 
{

    static final Logger logger = LoggerFactory.getLogger(App.class);

    private static final String transactionName = "tx_example";

    // input transactions
    private static final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    public static void main( String[] args ) throws Exception {

        // the reflections framework is scanning all the packages, not respecting the package passed
        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        loadMicroservice( new NetworkNode("localhost", 1080),
                "example",
                "dk.ku.di.dms.vms.playground.app",
                inToDiscard,
                outToDiscard,
                inToSwap);

        inToSwap = inToDiscard;
        inToDiscard = List.of("in");
        outToDiscard = List.of("out");

        loadMicroservice(
                new NetworkNode("localhost", 1081),
                "example2",
                "dk.ku.di.dms.vms.playground.scenario2",
                inToDiscard,
                outToDiscard,
                inToSwap);

        loadCoordinator();

        Thread producerThread = new Thread(new Producer());
        producerThread.start();

    }

    private static class Producer implements Runnable {

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val < 3) {

                EventExample eventExample = new EventExample(val);

                String payload = serdes.serialize(eventExample, EventExample.class);

                TransactionInput.Event eventPayload = new TransactionInput.Event("in", payload);

                TransactionInput txInput = new TransactionInput(transactionName, eventPayload);

                logger.info("[Producer] Adding "+val);

                parsedTransactionRequests.add(txInput);

                try {
                    //logger.info("Producer going to bed... ");
                    Thread.sleep(10000);
                    //logger.info("Producer woke up! Time to insert one more ");
                } catch (InterruptedException ignored) { }

                val++;

            }

            logger.info("Producer going to bed definitely... ");
        }
    }

    private static void loadCoordinator() throws IOException {

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 1082 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 1083 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverEm1.hashCode(), serverEm1);
        serverMap.put(serverEm2.hashCode(), serverEm2);

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        NetworkAddress vms1 = new NetworkAddress("localhost", 1080);
        NetworkAddress vms2 = new NetworkAddress("localhost", 1081);

        Map<Integer, NetworkAddress> VMSs = new HashMap<>(2);
        VMSs.put(vms1.hashCode(), vms1);
        VMSs.put(vms2.hashCode(), vms2);

        TransactionDAG dag =  TransactionBootstrap.name(transactionName)
                .input( "a", "example", "in" )
                .terminal("b", "example", "a")
                .terminal("c", "example2", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>(1);
        transactionMap.put(dag.name, dag);

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

    private static void loadMicroservice(NetworkNode node, String vmsName, String packageName, List<String> inToDiscard, List<String> outToDiscard, List<String> inToSwap) throws Exception {

        VmsEmbeddedInternalChannels vmsInternalPubSubService = new VmsEmbeddedInternalChannels();

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata(packageName);

        // discard events
        for(String in : inToDiscard)
            vmsMetadata.inputEventSchema().remove(in);

        for(String out : outToDiscard)
            vmsMetadata.outputEventSchema().remove(out);

        for(String in : inToSwap) {
            VmsEventSchema eventSchema = vmsMetadata.inputEventSchema().remove(in);
            vmsMetadata.outputEventSchema().put(in, eventSchema);
        }

        TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacadeAndInjectIntoRepositories(vmsMetadata);

        ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        VmsTransactionScheduler scheduler =
                new VmsTransactionScheduler(
                        readTaskPool,
                        vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(),null,null
                        );

        VmsNode vmsIdentifier = new VmsNode(
                node.host, node.port, vmsName,0, 0,0,
                vmsMetadata.tableSchema(),   vmsMetadata.replicatedTableSchema(),
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
