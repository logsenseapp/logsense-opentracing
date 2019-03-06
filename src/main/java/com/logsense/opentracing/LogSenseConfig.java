package com.logsense.opentracing;

import java.util.Properties;

public class LogSenseConfig {
    public static final String CUSTOMER_TOKEN = "logsense.token";
    public static final String HOST = "logsense.host";
    public static final String PORT = "logsense.port";

    public static final String DEFAULT_HOST = "logs.logsense.com";
    public static final int DEFAULT_PORT = 32714;

    private final String customerToken;
    private final String host;
    private final int port;

    private static final LogSenseConfig INSTANCE = new LogSenseConfig();

    LogSenseConfig() {
        customerToken = getStringProperty(System.getProperties(), CUSTOMER_TOKEN, "");
        host = getStringProperty(System.getProperties(), HOST, DEFAULT_HOST);
        port = getIntegerProperty(System.getProperties(), PORT, DEFAULT_PORT);
    }

    LogSenseConfig(Properties prop, LogSenseConfig parent) {
        customerToken = getStringProperty(prop, CUSTOMER_TOKEN, parent.getCustomerToken());
        host = getStringProperty(prop, HOST, parent.getHost());
        port = getIntegerProperty(prop, PORT, parent.getPort());
    }

    public static LogSenseConfig get() {
        return INSTANCE;
    }

    public static LogSenseConfig get(final Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return INSTANCE;
        } else {
            return new LogSenseConfig(properties, INSTANCE);
        }
    }

    public int getPort() {
        return port;
    }

    public String getCustomerToken() {
        return customerToken;
    }

    public String getHost() {
        return host;
    }

    private static String getPropertyWithFallback(final Properties prop, final String name, final String defaultValue) {
        String valueMaybe = prop.getProperty(name, null);
        // Fallback to env but only if global properties are taken
        if (null == valueMaybe && prop == System.getProperties()) {
            valueMaybe = System.getenv(name);
        }

        if (valueMaybe == null || valueMaybe.isEmpty()) {
            return defaultValue;
        } else {
            return valueMaybe;
        }
    }

    private static String getStringProperty(final Properties prop, final String name, final String defaultValue) {
        return getPropertyWithFallback(prop, name, defaultValue);
    }

    private static int getIntegerProperty(final Properties prop, final String name, final int defaultValue) {
        try {
            return Integer.parseInt(getPropertyWithFallback(prop, name, Integer.toString(defaultValue)));
        } catch (NumberFormatException nfe) {
            return defaultValue;
        }
    }
}
