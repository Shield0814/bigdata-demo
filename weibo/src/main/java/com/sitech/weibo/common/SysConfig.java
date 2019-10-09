package com.sitech.weibo.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SysConfig {

    public static final Properties sysConfig = new Properties();

    static {
        InputStream in = SysConfig.class.getClassLoader().getResourceAsStream("sys.properties");
        try {
            sysConfig.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
