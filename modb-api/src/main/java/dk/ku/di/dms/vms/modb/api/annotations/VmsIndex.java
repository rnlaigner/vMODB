package dk.ku.di.dms.vms.modb.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({ElementType.FIELD})
@Retention(RUNTIME)
public @interface VmsIndex {

    /**
     * Inspired by {@link javax.persistence.Index}
     * (Required) The name of the index; defaults to a provider-generated name.
     */
    String name();

    /**
     * Inspired by {@link javax.persistence.Index}
     * (Optional) Whether the index is unique.
     */
    boolean unique() default false;

    boolean sorted() default false;

}
