package com.sitech.gmall.brc.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigManager {

    static Properties props = new Properties();

    static {

        try {
            InputStream in = ConfigManager.class.getClassLoader()
                    .getResourceAsStream("sys.properties");
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }

    public static Properties getAll() {
        return props;
    }
}
