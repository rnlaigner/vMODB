package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.io.IOException;
import java.util.*;

import static dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils.VMS_TO_PORT_MAP;

public final class Main {

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    public static void main(String[] args) throws Exception {
        String option;
        if(args.length == 0) {
            System.out.println("Select your deployment scheme: \n1 - Distributed \n2 - Local \nq - Quit\n\nYou can also set this automatically by passing 1 or 2 as an argument to the CLI.");
            option = new Scanner(System.in).nextLine();
        } else {
            option = args[0];
        }
        switch (option){
            case "1" -> loadMenu("Distributed Deployment Menu");
            case "2" -> loadLocalDeploymentMenu();
            default -> System.exit(0);
        }
    }

    private static void loadLocalDeploymentMenu() throws Exception {
        dk.ku.di.dms.vms.tpcc.warehouse.Main.main(null);
        dk.ku.di.dms.vms.tpcc.inventory.Main.main(null);
        dk.ku.di.dms.vms.tpcc.order.Main.main(null);
        loadMenu("Local Deployment Menu");
    }

    private static void loadMenu(String menuType) {
        Coordinator coordinator = null;
        final int numWare = Integer.parseInt(PROPERTIES.get("num_ware").toString());
        List<Map<String,Iterator<Object>>> input;

        Map<String, Integer> numTxInputPerType = new HashMap<>(3);
        numTxInputPerType.put("new_order", Integer.valueOf(PROPERTIES.get("new_order_input_size").toString()));
        numTxInputPerType.put("payment", Integer.valueOf(PROPERTIES.get("payment_input_size").toString()));
        numTxInputPerType.put("order_status", Integer.valueOf(PROPERTIES.get("order_status_input_size").toString()));

        Map<String, Integer> txRatioMap = buildTransactionRatioMap();
        Tuple<Integer, String>[] txRatio = buildTransactionRatio(txRatioMap);

        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        while (running) {
            printMenu(menuType);
            System.out.print("Enter your choice: ");
            String choice = scanner.nextLine();
            switch (choice) {
                case "1": {
                    String order_host = PROPERTIES.getProperty("order_host");
                    try(MinimalHttpClient client = new MinimalHttpClient(order_host, DataLoadUtils.ORDER_VMS_PORT)){
                        if(client.sendRequest("PUT", "", "") != 200){
                            System.out.println("Error on PUT endpoint of order!");
                        }
                    } catch (IOException e) {
                        System.out.println("Error on PUT endpoint of order!");
                        break;
                    }

                    String warehouse_host = PROPERTIES.getProperty("warehouse_host");
                    try(MinimalHttpClient client = new MinimalHttpClient(warehouse_host, DataLoadUtils.WAREHOUSE_VMS_PORT)){
                        if(client.sendRequest("PUT", "", "") != 200){
                            System.out.println("Error on PUT endpoint of warehouse!");
                        }
                    } catch (IOException e) {
                        System.out.println("Error on PUT endpoint of warehouse!");
                        break;
                    }

                    String inventory_host = PROPERTIES.getProperty("inventory_host");
                    try(MinimalHttpClient client = new MinimalHttpClient(inventory_host, DataLoadUtils.INVENTORY_VMS_PORT)){
                        if(client.sendRequest("PUT", "", "") != 200){
                            System.out.println("Error on PUT endpoint of inventory!");
                        }
                    } catch (IOException e) {
                        System.out.println("Error on PUT endpoint of inventory!");
                    }
                    break;
                }
                case "3":
                    System.out.println("Option 3: \"Create workload\" selected.");
                    System.out.println("Number of warehouses: "+numWare);

                    try {
                        WorkloadUtils.createWorkload(numWare, Boolean.getBoolean( PROPERTIES.get("multi_ware").toString() ), numTxInputPerType);
                    } catch (IOException e){
                        System.out.println("ERROR:\n"+e);
                    }
                    break;
                case "4":
                    System.out.println("Option 4: \"Submit workload\" selected.");

                    // check if workload files exist
                    int numFiles = WorkloadUtils.getNumWorkloadInputFiles(numTxInputPerType);

                    if(numWare != numFiles){
                        System.out.println("Number of warehouses ("+numWare+") != Number of input files ("+numFiles+")");
                        System.out.println("Do you want to proceed? [y/n]");
                        String resp = scanner.nextLine();
                        if(resp.equalsIgnoreCase("n")){
                            break;
                        }
                    }

                    int batchWindow = Integer.parseInt(PROPERTIES.getProperty("batch_window_ms"));
                    int runTime;

                    while(true) {
                        System.out.print("Enter duration (ms): [press 0 for 10s] ");
                        runTime = Integer.parseInt(scanner.nextLine());
                        if (runTime == 0) runTime = 10000;
                        if(runTime < (batchWindow * 2)){
                            System.out.print("Duration must be at least 2 * "+batchWindow+" (ms)\n");
                            continue;
                        }
                        break;
                    }
                    int warmUp;
                    while(true) {
                        System.out.println("Enter warm up period (ms): [press 0 for 2s] ");
                        warmUp = Integer.parseInt(scanner.nextLine());
                        if (warmUp <= 0) warmUp = 2000;
                        if(warmUp > runTime){
                            System.out.print("Warm up must be lower than run time "+runTime+" (ms)\n");
                            continue;
                        }
                        break;
                    }

                    // reload iterators
                    input = WorkloadUtils.mapWorkloadInputFiles(numWare, txRatioMap);

                    // load coordinator
                    if(coordinator == null){
                        coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
                        // wait for all starter VMSes to connect
                        int numConnected;
                        do {
                            numConnected = coordinator.getConnectedVMSs().size();
                        } while (numConnected < 3);
                    }

                    // prevent log pollution, i.e., interleaving of handshaking and experiment messages
                    try { Thread.sleep(100); } catch (InterruptedException _) { }

                    ExperimentUtils.ExperimentStats expStats = ExperimentUtils.runExperiment(coordinator, txRatio, input, runTime, warmUp);
                    ExperimentUtils.writeResultsToFile(numWare, expStats, runTime, warmUp,
                            coordinator.getOptions().getNumTransactionWorkers(), coordinator.getOptions().getBatchWindow(), coordinator.getOptions().getMaxTransactionsPerBatch(), txRatio);
                    break;
                case "5":
                    System.out.println("Option 5: \"Cleanup VMS states\" selected.");
                    // has to wait for all submitted transactions to commit in order to send the reset
                    if (checkCompleteness(coordinator, scanner)) break;
                    // cleanup VMS states
                    cleanup(false);
                    System.out.println("VMS states cleaned.");
                    break;
                case "6":
                    System.out.println("Option 5: \"Reset VMS states\" selected.");
                    // has to wait for all submitted transactions to commit in order to send the reset
                    if (checkCompleteness(coordinator, scanner)) break;
                    cleanup(true);
                    System.out.println("VMS states reset.");
                    break;
                case "q":
                    System.out.println("Exiting the application...");
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
        scanner.close();
        System.exit(0);
    }

    private static boolean checkCompleteness(Coordinator coordinator, Scanner scanner) {
        if(coordinator != null){
            long numTIDsCommitted = coordinator.getNumTIDsCommitted();
            long numTIDsSubmitted = coordinator.getNumTIDsSubmitted();
            if(numTIDsCommitted != numTIDsSubmitted){
                System.out.println("There are ongoing batches executing! Cannot reset states now. \n Number of TIDs committed: "+numTIDsCommitted+"\n Number of TIDs submitted: "+numTIDsSubmitted);
                System.out.println("Do you want to proceed? [y/n]");
                String resp = scanner.nextLine();
                return resp.equalsIgnoreCase("n");
            }
        }
        return false;
    }

    private static void cleanup(boolean reset) {
        String param;
        if(reset) param = "reset"; else param = "cleanup";
        for(Map.Entry<String, Integer> vms : VMS_TO_PORT_MAP.entrySet()){
            String host = PROPERTIES.getProperty(vms.getKey() + "_host");
            try(MinimalHttpClient client = new MinimalHttpClient(host, vms.getValue())){
                if(client.sendRequest("PATCH", "", param) != 200){
                    System.out.println("Error on resetting "+vms+" state!");
                }
            } catch (IOException e) {
                System.out.println("Exception on resetting "+vms+" state: \n"+e);
            }
        }
    }

    public static Map<String, Integer> buildTransactionRatioMap(){
        Map<String, Integer> txRatioMap = new TreeMap<>();
        boolean seen_100 = false;
        if(!PROPERTIES.get("new_order").toString().equals("0")) {
            txRatioMap.put("new_order", Integer.valueOf(PROPERTIES.get("new_order").toString()));
            if(txRatioMap.get("new_order") == 100) seen_100 = true;
        }
        if(!PROPERTIES.get("payment").toString().equals("0")) {
            txRatioMap.put("payment", Integer.valueOf(PROPERTIES.get("payment").toString()));
            if(txRatioMap.get("payment") == 100) seen_100 = true;
        }
        if(!PROPERTIES.get("order_status").toString().equals("0")) {
            txRatioMap.put("order_status", Integer.valueOf(PROPERTIES.get("order_status").toString()));
            if(txRatioMap.get("order_status") == 100) seen_100 = true;
        }
        if(!seen_100) throw new RuntimeException("No transaction defined as 100 in app.properties!");
        return txRatioMap;
    }

    @SuppressWarnings("unchecked")
    private static Tuple<Integer, String>[] buildTransactionRatio(Map<String, Integer> txRatioMap) {
        Tuple<Integer, String>[] txRatio = new Tuple[txRatioMap.size()];
        int i = 0;
        for(var entry : txRatioMap.entrySet()) {
            txRatio[i] = Tuple.of(entry.getValue(), entry.getKey());
            i++;
        }
        return txRatio;
    }

    private static void printMenu(String menuType) {
        System.out.println("\n=== "+menuType+" ===");
        System.out.println("1. Populate tables in VMSes");
        System.out.println("2. Check data correctness in VMSes");
        System.out.println("3. Create workload");
        System.out.println("4. Submit workload");
        System.out.println("5. Cleanup VMS states");
        System.out.println("6. Reset VMS states");
        System.out.println("q. Quit program");
    }

}
