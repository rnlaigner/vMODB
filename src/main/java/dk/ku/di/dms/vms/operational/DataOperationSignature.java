package dk.ku.di.dms.vms.operational;

import java.lang.reflect.Method;

/**
 * A data class that stores the method, respective class,
 * and the name of the inbound events that trigger this method.
 *
 * This method can be reused so the scheduler can keep track
 * of the missing events.
 */
public class DataOperationSignature {

    public final Object methodClazz;

    // https://stackoverflow.com/questions/4685563/how-to-pass-a-function-as-a-parameter-in-java
    public final Method method;

    // The list must obey the order of the parameters? Can I use a set?
    public String[] inputQueues;

    // For fast search when the event is generated
    public String outputQueue;

    public DataOperationSignature(Object methodClazz, Method method, String[] inputQueues, String outputQueue) {
        this.methodClazz = methodClazz;
        this.method = method;
        this.inputQueues = inputQueues;
        this.outputQueue = outputQueue;
    }

    public DataOperationSignature(Object methodClazz, Method method) {
        this.methodClazz = methodClazz;
        this.method = method;
    }

}
