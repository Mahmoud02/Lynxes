package org.mahmoud.fastqueue.di;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.concurrent.ExecutorService;
import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.config.ConfigLoader;
import org.mahmoud.fastqueue.api.producer.Producer;
import org.mahmoud.fastqueue.api.consumer.Consumer;
import org.mahmoud.fastqueue.service.MessageService;
import org.mahmoud.fastqueue.service.HealthService;
import org.mahmoud.fastqueue.service.ObjectMapperService;
import org.mahmoud.fastqueue.service.ExecutorServiceProvider;
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
        // Bind storage layer services
        bind(Producer.class).in(Singleton.class);
        bind(Consumer.class).in(Singleton.class);
        
        // Bind service layer
        bind(MessageService.class).in(Singleton.class);
        bind(HealthService.class).in(Singleton.class);
        bind(ObjectMapperService.class).in(Singleton.class);
        
        // Bind infrastructure services
        bind(ExecutorService.class).toProvider(ExecutorServiceProvider.class).in(Singleton.class);
        
        // HTTP servers are provided by @Provides methods below
        
        // RequestChannel is provided by @Provides method below
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
    
    /**
     * Provides AsyncHttpServer with all dependencies injected.
     */
    @Provides
    @Singleton
    public AsyncHttpServer provideAsyncHttpServer(QueueConfig config, RequestChannel requestChannel, 
                                                 MessageService messageService, HealthService healthService,
                                                 ExecutorService executorService) {
        return new AsyncHttpServer(config, requestChannel, messageService, healthService, executorService);
    }
    
    /**
     * Provides JettyHttpServer with all dependencies injected.
     */
    @Provides
    @Singleton
    public JettyHttpServer provideJettyHttpServer(QueueConfig config, MessageService messageService, 
                                                 ObjectMapperService objectMapperService) {
        return new JettyHttpServer(config, messageService, objectMapperService);
    }
}
