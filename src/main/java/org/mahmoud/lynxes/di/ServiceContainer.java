package org.mahmoud.lynxes.di;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service container that manages dependency injection for FastQueue2.
 * This is a singleton that provides access to all injected services.
 */
public class ServiceContainer {
    private static final Logger logger = LoggerFactory.getLogger(ServiceContainer.class);
    
    private static ServiceContainer instance;
    private final Injector injector;
    
    private ServiceContainer(String environment) {
        logger.info("Initializing ServiceContainer for environment: {}", environment);
        
        // Create Guice module
        Module module = new LynxesModule(environment);
        
        // Create injector
        this.injector = Guice.createInjector(module);
        
        logger.info("ServiceContainer initialized successfully");
    }
    
    /**
     * Gets the singleton instance of ServiceContainer.
     * Creates a new instance if one doesn't exist.
     */
    public static synchronized ServiceContainer getInstance(String environment) {
        if (instance == null) {
            instance = new ServiceContainer(environment);
        }
        return instance;
    }
    
    /**
     * Gets the singleton instance of ServiceContainer.
     * Uses "default" environment if no instance exists.
     */
    public static synchronized ServiceContainer getInstance() {
        if (instance == null) {
            instance = new ServiceContainer("default");
        }
        return instance;
    }
    
    /**
     * Gets a service instance by its class.
     * 
     * @param serviceClass The class of the service to retrieve
     * @param <T> The type of the service
     * @return The service instance
     */
    public <T> T getService(Class<T> serviceClass) {
        return injector.getInstance(serviceClass);
    }
    
    /**
     * Gets the Guice injector for advanced usage.
     * 
     * @return The Guice injector
     */
    public Injector getInjector() {
        return injector;
    }
    
    /**
     * Resets the singleton instance (useful for testing).
     */
    public static synchronized void reset() {
        instance = null;
        logger.info("ServiceContainer reset");
    }
}
