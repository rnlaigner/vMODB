package dk.ku.di.dms.vms.modb.common.utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.logging.LogManager;

public final class ConfigUtils {

    private static final String CONFIG_FILE = "app.properties";

    private static final String LOGGING_FILE = "logging.properties";

    private static final Properties PROPERTIES = new Properties();

    static {
        // Java does not automatically load logging.properties from the classpath by default.
        try (InputStream inputStream = ConfigUtils.class.getClassLoader().getResourceAsStream(LOGGING_FILE)) {
            if (inputStream != null) {
                LogManager.getLogManager().readConfiguration(inputStream);
            } else {
                System.out.println("Could not find logging file in the classpath. Resorting to default configuration.");
            }
        } catch (Exception e) {
            System.out.println("Error loading logging configuration. Resorting to default configuration. Error details:\n" + e.getMessage());
        }
        try {
            PROPERTIES.load(ConfigUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
        } catch (Exception e) {
            System.out.println("Error loading application configuration. Error details:\n" + e.getMessage());
        }
    }

    public static Properties loadProperties(){
        return PROPERTIES;
    }

    public static String getUserHome() {
        String userHome = System.getProperty("user.home");
        if(userHome == null){
            System.out.println("User home directory is not set in the environment. Resorting to /usr/local/lib");
            userHome = "/usr/local/lib";
        }
        return userHome;
    }

    public static String getOsName()
    {
        return System.getProperty("os.name");
    }

    public static boolean isWindows()
    {
        return getOsName().startsWith("Windows");
    }

    public static String getCallerPackage(){
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement element = stackTrace[i];
            if (isVmsApplicationBuild(element)) {

                // if next is also vms app build, skip
                if(isVmsApplicationBuild(stackTrace[i+1])) continue;

                try {
                    Class<?> mainClass = Class.forName(stackTrace[i+1].getClassName());
                    Package pkg = mainClass.getPackage();
                    return pkg != null ? pkg.getName() : "(default package)";
                } catch (ClassNotFoundException e) {
                    return null;
                }
            }
        }

        // then it is probably a static call at the init of the program
        // find the last "<clinit>".equals(methodName)
        int lastIdx = Thread.currentThread().getStackTrace().length;
        if(Thread.currentThread().getStackTrace()[lastIdx-1].getMethodName().equals("<clinit>")){
            try {
                Class<?> mainClass = Class.forName(Thread.currentThread().getStackTrace()[lastIdx-1].getClassName());
                Package pkg = mainClass.getPackage();
                return pkg != null ? pkg.getName() : "(default package)";
            } catch (ClassNotFoundException e) {
                return null;
            }
        }

        return null;
    }

    private static boolean isVmsApplicationBuild(StackTraceElement element) {
        return "build".equals(element.getMethodName()) && "VmsApplication.java".equals(element.getFileName());
    }

}
