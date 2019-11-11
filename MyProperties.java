package com.zhiyou100.util;

import java.io.IOException;
import java.util.Properties;

public class MyProperties {
    private static String url;
    private static String user;
    private static String password;

    private static String zookeeper_name;
    private static String zookeeper_value;

    public static String getZookeeper_name() {
        return zookeeper_name;
    }

    public static void setZookeeper_name(String zookeeper_name) {
        MyProperties.zookeeper_name = zookeeper_name;
    }

    public static String getZookeeper_value() {
        return zookeeper_value;
    }

    public static void setZookeeper_value(String zookeeper_value) {
        MyProperties.zookeeper_value = zookeeper_value;
    }

    public static String getUrl() {
        return url;
    }

    public static void setUrl(String url) {
        MyProperties.url = url;
    }

    public static String getUser() {
        return user;
    }

    public static void setUser(String user) {
        MyProperties.user = user;
    }

    public static String getPassword() {
        return password;
    }

    public static void setPassword(String password) {
        MyProperties.password = password;
    }

    public MyProperties() {
    }

    static {
        try {
            Properties p = new Properties();
            p.load(DBUtil.class.getResourceAsStream("/db.properties"));
            url = p.getProperty("url");
            user = p.getProperty("user");
            password = p.getProperty("password");
            zookeeper_name = p.getProperty("zookeeper_name");
            zookeeper_value = p.getProperty("zookeeper_value");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
