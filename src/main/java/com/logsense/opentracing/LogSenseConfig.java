package com.logsense.opentracing;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LogSenseConfig {
    private final static Logger log = Logger.getLogger(LogSenseConfig.class.getName());

    public static class Property {
        public Property(final String propertyName, final String envName, final String defaultValue) {
            this.propertyName = propertyName;
            this.envName = envName;
            this.defaultValue = defaultValue;
        }

        private final String propertyName;
        private final String envName;
        private final String defaultValue;

        private String getPropertyValue(final Properties prop) {
            return getPropertyValue(prop, null);
        }

        private String getPropertyValue(final Properties prop, String customDefaultValue) {
            String valueMaybe = prop.getProperty(propertyName, null);

            // Fallback to env but only if global properties are taken
            if (null == valueMaybe && prop == System.getProperties() && envName != null) {
                valueMaybe = System.getenv(envName);
            }

            if (valueMaybe == null || valueMaybe.trim().isEmpty()) {
                if (customDefaultValue != null) {
                    return customDefaultValue;
                } else {
                    return defaultValue;
                }
            } else {
                return valueMaybe;
            }
        }

        private Integer getIntegerValue(final Properties prop) {
            return getIntegerValue(prop, null);
        }


        private Integer getIntegerValue(final Properties prop, Integer customDefaultValue) {
            try {
                return Integer.parseInt(getPropertyValue(prop));
            } catch (NumberFormatException nfe) {
                if (customDefaultValue != null) {
                    return customDefaultValue;
                } else {
                    if (defaultValue == null || defaultValue.isEmpty()) {
                        return null;
                    } else {
                        return Integer.parseInt(defaultValue);
                    }
                }
            }
        }

        public String getEnvName() {
            return envName;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        @Override
        public String toString() {
            return String.format("%s property or %s env variable with default value of %s", propertyName, envName, defaultValue);
        }
    }

    public static final Property CUSTOMER_TOKEN = new Property("logsense.token", "LOGSENSE_TOKEN", "");
    public static final Property HOST = new Property("logsense.host", "LOGSENSE_HOST", "logs.logsense.com");
    public static final Property PORT = new Property("logsense.port", "LOGSENSE_PORT", "32714");
    public static final Property SERVICE_NAME = new Property("logsense.service.name", "LOGSENSE_SERVICE_NAME", "");
    public static final Property CONFIG_FILE = new Property("logsense.config", null, null);


    private String customerToken;
    private String host;
    private String serviceName;
    private int port;

    private static final LogSenseConfig INSTANCE = new LogSenseConfig();

    private boolean enabled;

    LogSenseConfig() {
        customerToken = CUSTOMER_TOKEN.getPropertyValue(System.getProperties());
        host = HOST.getPropertyValue(System.getProperties());
        port = PORT.getIntegerValue(System.getProperties());
        serviceName = SERVICE_NAME.getPropertyValue(System.getProperties());

        String config_file = CONFIG_FILE.getPropertyValue(System.getProperties());
        if (config_file != null && !config_file.isEmpty()) {
            Properties prop = attemptLoadingPropertyFile(config_file);
            merge(prop, this);
        }

        checkEnabled();
    }

    LogSenseConfig(Properties prop, LogSenseConfig parent) {
        merge(prop, parent);
    }

    private void merge(Properties prop, LogSenseConfig parent) {
        customerToken = CUSTOMER_TOKEN.getPropertyValue(prop, parent.getCustomerToken());
        host = HOST.getPropertyValue(prop, parent.getHost());
        port = PORT.getIntegerValue(prop, parent.getPort());
        serviceName = SERVICE_NAME.getPropertyValue(prop, parent.getServiceName());


        checkEnabled();
    }

    private void checkEnabled() {
        if (customerToken == null || "".equals(customerToken.trim())) {
            this.enabled = false;
            log.info("LogSense tracing disabled due to no valid token provided");
        } else {
            this.enabled = true;
            log.info("LogSense tracing enabled");
        }
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

    public boolean isEnabled() {
        return enabled;
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

    public String getServiceName() {
        return serviceName;
    }

    private Properties attemptLoadingPropertyFile(String path) {
        Properties prop = new Properties();
        InputStream fis=null;
        try {
            fis = new FileInputStream(path);
            prop.load(fis);
        } catch(Exception e) {
            log.log(Level.WARNING, String.format("Skipping loading LogSense properties from %s due to exception: %s", path, e.getMessage(), e));
        } finally {
            try {
                fis.close();;
            } catch (Exception e) {
                return null;
            }
        }

        return prop;
    }
}
