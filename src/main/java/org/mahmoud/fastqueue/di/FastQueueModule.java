package org.mahmoud.fastqueue.di;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.config.ConfigLoader;
import org.mahmoud.fastqueue.api.producer.Producer;
import org.mahmoud.fastqueue.api.consumer.Consumer;
import org.mahmoud.fastqueue.server.async.RequestChannel;
import org.mahmoud.fastqueue.server.async.AsyncProcessor;
import org.mahmoud.fastqueue.server.async.AsyncHttpServer;
import org.mahmoud.fastqueue.server.http.JettyHttpServer;

/**
 * Guice module for FastQueue2 dependency injection configuration.
 * This module defines how dependencies should be created and injected.
 */
public class FastQueueModule extends AbstractModule {
    
    private final String environment;
    
    public FastQueueModule(String environment) {
        this.environment = environment;
    }
    
    @Override
    protected void configure() {
        // Bind core services - Guice will automatically inject QueueConfig
        bind(Producer.class).in(Singleton.class);
        bind(Consumer.class).in(Singleton.class);
        
        // Don't bind RequestChannel here - it's provided by @Provides method
        // Don't bind AsyncProcessor and HTTP servers here to avoid circular dependencies
        // They will be created manually in the providers
    }
    
    /**
     * Provides a configured QueueConfig.
     */
    @Provides
    @Singleton
    public QueueConfig provideQueueConfig() {
        return ConfigLoader.loadConfig(environment);
    }
    
    /**
     * Provides a configured RequestChannel.
     */
    @Provides
    @Singleton
    public RequestChannel provideRequestChannel(QueueConfig config) {
        return new RequestChannel(1000); // Max 1000 queued requests
    }
}
