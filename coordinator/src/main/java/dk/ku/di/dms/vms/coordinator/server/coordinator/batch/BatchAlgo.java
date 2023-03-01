package dk.ku.di.dms.vms.coordinator.server.coordinator.batch;

import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsContext;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;

import java.util.*;

/**
 * Algorithms related to the formation of a batch of transactions
 */
public final class BatchAlgo {

    /**
     * A map of vms and corresponding precedent TID for a given tid
     * Build precedence for the downstream events of an (input) event
     */
    public static Map<String, Long> buildPrecedenceMap(EventIdentifier inputEvent, TransactionDAG transactionDAG, Map<String, VmsContext> vmsMetadata) {
        return buildPrecedenceRecursive(inputEvent, transactionDAG, vmsMetadata);
    }

    public static Map<String, Long> buildPrecedenceMap(TransactionDAG transactionDAG, Map<String, VmsContext> vmsMetadata) {
        List<Map<String, Long>> listOfMapPerInputEvent = new ArrayList<>(transactionDAG.inputEvents.size());
        for(EventIdentifier inputEvent : transactionDAG.inputEvents.values()) {
            listOfMapPerInputEvent.add( buildPrecedenceRecursive(inputEvent, transactionDAG, vmsMetadata) );
        }
        Map<String, Long> merged = new HashMap<>(listOfMapPerInputEvent.size());
        for(Map<String, Long> map : listOfMapPerInputEvent){
            merged.putAll( map );
        }
        return merged;
    }

    private static Map<String, Long> buildPrecedenceRecursive(EventIdentifier event,
                                                              TransactionDAG transactionDAG,
                                                              Map<String, VmsContext> vmsMetadata){
        Map<String, Long> listToBuildMap = new HashMap<>();

        // input and internal nodes first, since they have children
        if(transactionDAG.internalNodes.contains( event.targetVms ) || transactionDAG.inputEvents.get( event.getName() ) != null){
            listToBuildMap.put(event.targetVms, vmsMetadata.get(event.targetVms).getLastTidOfBatch());
            for(EventIdentifier child : event.children){
                listToBuildMap.putAll(buildPrecedenceRecursive(child, transactionDAG, vmsMetadata));
            }
        } else if(transactionDAG.terminalNodes.contains( event.targetVms )){
                listToBuildMap.put(event.targetVms, vmsMetadata.get(event.targetVms).getLastTidOfBatch());
        }

        return listToBuildMap;
    }

}
