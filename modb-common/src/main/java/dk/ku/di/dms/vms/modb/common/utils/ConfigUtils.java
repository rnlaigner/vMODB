package dk.ku.di.dms.vms.modb.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public final class ConfigUtils {

    private static final String CONFIG_FILE = "app.properties";

    public static Properties loadProperties(){
        Properties properties = new Properties();
        try {
            if (Files.exists(Paths.get(CONFIG_FILE))) {
                properties.load(new FileInputStream(CONFIG_FILE));
            } else {
                properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
            }
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
