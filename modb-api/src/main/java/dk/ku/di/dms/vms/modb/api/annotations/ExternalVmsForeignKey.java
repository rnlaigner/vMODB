package dk.ku.di.dms.vms.modb.api.annotations;

import dk.ku.di.dms.vms.modb.api.enums.ReplicationStrategy;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Potentially this is used when developers are able to exchange information
 * about their microservices.
 * However, there may be cases where an event comes from outside
 * and vms and table does not make sense, so an event would be a better
 * abstraction. Let's start first with this abstraction...
 */
@Target({FIELD})
@Retention(RUNTIME)
public @interface ExternalVmsForeignKey {
    String vms();
    String table();
    String column();

    // cross-vms foreign key enforcement is a sub-problem of replication
    ReplicationStrategy strategy() default ReplicationStrategy.STRONG;
}