package ru.sap.po.mapping.config;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import java.io.IOException;
import java.io.InputStream;

public class RouterPropertiesHandler {

    private static final String PROPERTIES_FILENAME = "router.properties";

    private static RouterPropertiesHandler instance;
    private Properties properties;

    private RouterPropertiesHandler() {
        properties = loadPropertiesFromClasspath();
    }

    public static synchronized RouterPropertiesHandler getInstance() {
        if (instance == null) {
            instance = new RouterPropertiesHandler();
        }
        return instance;
    }

    private Properties loadPropertiesFromClasspath() {
        try (InputStream input = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILENAME)) {
            Properties prop = new Properties();
            prop.load(input);
            return prop;
        } catch (IOException | IllegalArgumentException ex) {
            return null;
        }
    }

    public String getPropertyValue(String key) {
        if (properties != null) {
            return properties.getProperty(key);
        }
        return null;
    }

    public List<String> getListPropertyValue(String key) {
        String propertyValue = this.getPropertyValue(key);
        if (propertyValue != null) {
            return Arrays.asList(propertyValue.split(","));
        }
        return null;
    }

}
