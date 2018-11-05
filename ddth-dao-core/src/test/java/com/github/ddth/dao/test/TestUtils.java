package com.github.ddth.dao.test;

import java.util.Properties;

public class TestUtils {
    private static Properties sysProps = new Properties();
    static {
        System.getProperties().forEach((k, v) -> sysProps.put(k.toString().toUpperCase(), v));
    }

    /**
     * Check if a system property exists (case-insensitive).
     * 
     * @param key
     * @return
     */
    public static boolean hasSystemPropertyIgnoreCase(String key) {
        return sysProps.containsKey(key.toUpperCase());
    }

    /**
     * Get a system property (case-insensitive).
     * 
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getSystemPropertyIgnoreCase(String key, String defaultValue) {
        return sysProps.getProperty(key, defaultValue);
    }
}
