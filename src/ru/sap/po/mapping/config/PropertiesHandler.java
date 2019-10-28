package ru.sap.po.mapping.config;

import java.util.Properties;
import java.util.Optional;

import java.io.IOException;
import java.io.InputStream;

public class PropertiesHandler {

    private static PropertiesHandler instance;
    private Properties properties;

    private PropertiesHandler() {
    	properties = loadPropertiesFromClasspath();
    }

    public static synchronized PropertiesHandler getInstance() {
        if (instance == null) {
            instance = new PropertiesHandler();
        }
        return instance;
    }
    
    public Optional<String> getValue(String key) {
        if (properties != null) {
            return Optional.of(properties.getProperty(key));
        }
        return Optional.empty();
    }
    
    public String[] readPropertyValueToArray(String propertyValue) {
    	String[] values = propertyValue.split(",");
    	return values;
    }

    private Properties loadPropertiesFromClasspath() {
        try (InputStream input = this.getClass().getClassLoader().getResourceAsStream("mapping.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            return prop;
        } catch (IOException | IllegalArgumentException ex) {
            return null;
        }
    }

}
