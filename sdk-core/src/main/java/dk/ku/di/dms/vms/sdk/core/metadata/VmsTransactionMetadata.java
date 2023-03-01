package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.common.data_structure.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;

import java.util.ArrayList;
import java.util.List;

/**
 * As an event can trigger multiple methods, all methods are treated
 * as part of the same transaction.
 * This class contains the method signatures for a given transaction
 * as well as the contextual information about the execution of the tasks
 * Basically serves the skeleton of a {@link dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask}
 * in direction of the idea of the <a href="https://en.wikipedia.org/wiki/Prototype_pattern">Prototype</a> design pattern
 */
public class VmsTransactionMetadata {

    public int numReadTasks;
    public int numWriteTasks;
    public int numReadWriteTasks;

    public int numTasksWithMoreThanOneInput;

    public List<IdentifiableNode<VmsTransactionSignature>> signatures;

    public VmsTransactionMetadata(){
        this.numReadTasks = 0;
        this.numWriteTasks = 0;
        this.numReadWriteTasks = 0;
        this.numTasksWithMoreThanOneInput = 0;
        this.signatures = new ArrayList<>();
    }

}
