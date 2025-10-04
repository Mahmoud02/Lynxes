package org.mahmoud.fastqueue.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration loader using Typesafe Config.
 * Loads configuration from HOCON files and maps them to Java objects using annotations.
 */
public class ConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
    
    /**
     * Loads configuration for the specified environment.
     * 
     * @param environment Environment name (dev, prod, etc.)
     * @return QueueConfig object with loaded configuration
     */
    public static QueueConfig loadConfig(String environment) {
        try {
            // Load base configuration
            Config baseConfig = ConfigFactory.load("application");
            
            // Load environment-specific configuration
            Config envConfig = ConfigFactory.load("application-" + environment);
            
            // Merge configurations (environment overrides base)
            Config finalConfig = envConfig.withFallback(baseConfig);
            
            logger.info("Loading configuration for environment: {}", environment);
            
            // Create QueueConfig and populate it using reflection
            QueueConfig queueConfig = new QueueConfig();
            populateConfigFromTypesafe(queueConfig, finalConfig);
            
            logger.info("Configuration loaded: {}", queueConfig);
            return queueConfig;
            
        } catch (Exception e) {
            logger.warn("Failed to load configuration for environment: {}, using defaults: {}", 
                       environment, e.getMessage());
            return new QueueConfig(); // Fallback to defaults
        }
    }
    
    /**
     * Populates a QueueConfig object using Typesafe Config and @ConfigProperty annotations.
     */
    private static void populateConfigFromTypesafe(QueueConfig config, Config typesafeConfig) {
        Field[] fields = QueueConfig.class.getDeclaredFields();
        
        for (Field field : fields) {
            ConfigProperty annotation = field.getAnnotation(ConfigProperty.class);
            if (annotation != null) {
                try {
                    field.setAccessible(true);
                    String configKey = annotation.value();
                    String defaultValue = annotation.defaultValue();
                    
                    // Get value from Typesafe Config
                    Object value = getConfigValue(typesafeConfig, configKey, field.getType(), defaultValue);
                    
                    if (value != null) {
                        field.set(config, value);
                        logger.debug("Set {} = {} from config key: {}", field.getName(), value, configKey);
                    }
                    
                } catch (Exception e) {
                    logger.warn("Failed to set field {}: {}", field.getName(), e.getMessage());
                }
            }
        }
    }
    
    /**
     * Gets a configuration value from Typesafe Config with proper type conversion.
     */
    private static Object getConfigValue(Config config, String key, Class<?> type, String defaultValue) {
        try {
            if (!config.hasPath(key)) {
                if (!defaultValue.isEmpty()) {
                    return convertValue(defaultValue, type);
                }
                return null;
            }
            
            if (type == String.class) {
                return config.getString(key);
            } else if (type == int.class || type == Integer.class) {
                return config.getInt(key);
            } else if (type == long.class || type == Long.class) {
                return config.getLong(key);
            } else if (type == boolean.class || type == Boolean.class) {
                return config.getBoolean(key);
            } else if (type == Path.class) {
                return Paths.get(config.getString(key));
            } else {
                return config.getAnyRef(key);
            }
            
        } catch (Exception e) {
            logger.warn("Failed to get config value for key {}: {}", key, e.getMessage());
            if (!defaultValue.isEmpty()) {
                return convertValue(defaultValue, type);
            }
            return null;
        }
    }
    
    /**
     * Converts a string value to the target type.
     */
    private static Object convertValue(String value, Class<?> type) {
        try {
            if (type == String.class) {
                return value;
            } else if (type == int.class || type == Integer.class) {
                return Integer.parseInt(value);
            } else if (type == long.class || type == Long.class) {
                return Long.parseLong(value);
            } else if (type == boolean.class || type == Boolean.class) {
                return Boolean.parseBoolean(value);
            } else if (type == Path.class) {
                return Paths.get(value);
            } else {
                return value;
            }
        } catch (Exception e) {
            logger.warn("Failed to convert value '{}' to type {}: {}", value, type.getSimpleName(), e.getMessage());
            return null;
        }
    }
}
