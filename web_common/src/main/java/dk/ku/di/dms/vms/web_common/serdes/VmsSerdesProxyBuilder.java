package dk.ku.di.dms.vms.web_common.serdes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.util.function.Function;

/**
 * A builder of serialization and deserialization capabilities
 * The idea is to abstract in this class the procedures to transform objects,
 * so later we can change without disrupting the client classes, like VmsEventHandler
 *
 * https://www.overops.com/blog/the-ultimate-json-library-json-simple-vs-gson-vs-jackson-vs-json/
 * "If your environment primarily deals with lots of small JSON requests, such as in a micro services
 * or distributed architecture setup, then GSON is your library of interest. Jackson struggles the most with small files."
 */
public final class VmsSerdesProxyBuilder {

    public static IVmsSerdesProxy build(final Function<String,Class<? extends IApplicationEvent>> clazzResolver){

        GsonBuilder builder = new GsonBuilder();

        // register new type adapter here
        builder.registerTypeAdapter(TransactionalEvent.class, new TransactionalEventAdapter( clazzResolver ));

        builder.setPrettyPrinting();
        Gson gson1 = builder.create();

        return new DefaultVmsSerdes( gson1 );

    }

}
