package dk.ku.di.dms.vms.modb.common.serdes;

/**
 * A builder of serialization and deserialization capabilities
 * The idea is to abstract in this class the procedures to transform objects,
 * so later we can change without disrupting the client classes, like VmsEventHandler
 */
public final class VmsSerdesProxyBuilder {

    private static IVmsSerdesProxy INSTANCE;

    public synchronized static IVmsSerdesProxy build(){
        if(INSTANCE != null){
            return INSTANCE;
        }
        try {
            INSTANCE = new JacksonVmsSerdes();
        } catch (NoClassDefFoundError | Exception e){
            System.out.println("Failed to load default proxy: \n"+e);
            INSTANCE = new GsonVmsSerdes();
        }
        return INSTANCE;
    }

}
