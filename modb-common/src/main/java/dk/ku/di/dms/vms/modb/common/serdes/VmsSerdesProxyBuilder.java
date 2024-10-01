package dk.ku.di.dms.vms.modb.common.serdes;

/**
 * A builder of serialization and deserialization capabilities
 * The idea is to abstract in this class the procedures to transform objects,
 * so later we can change without disrupting the client classes, like VmsEventHandler
 */
public final class VmsSerdesProxyBuilder {

    public static IVmsSerdesProxy build(){
        IVmsSerdesProxy proxy;
        try {
            proxy = new JacksonVmsSerdes();
        } catch (NoClassDefFoundError | Exception e){
            System.out.println("Failed to load default proxy: \n"+e);
            proxy = new GsonVmsSerdes();
        }
        return proxy;
    }

}
