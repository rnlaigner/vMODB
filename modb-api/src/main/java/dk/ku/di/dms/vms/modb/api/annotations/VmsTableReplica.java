package dk.ku.di.dms.vms.modb.api.annotations;

import dk.ku.di.dms.vms.modb.api.enums.ReplicationStrategy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotate a class to act as a replicated table in this VMS
 */
@Target({TYPE})
@Retention(RUNTIME)
public @interface VmsTableReplica {

    // the ownerVMS
    String vms();

    // the table this class is representing
    String table();

    ReplicationStrategy strategy() default ReplicationStrategy.STRONG;

}