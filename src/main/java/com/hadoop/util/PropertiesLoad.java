package com.hadoop.util;

import java.util.Properties;

public class PropertiesLoad {

    private static Properties pros = new Properties();

    public static void init(Properties pro) {
        pros = pro;
    }

    public static String getProperties(String key) {
        return pros.getProperty(key);
    }

    public static void putProperties(String key, String value) {
        pros.put(key, value);
    }

    public static void main(String[] args) {
//        System.out.println(pros.getProperty("IpAddre"));
    }
}

