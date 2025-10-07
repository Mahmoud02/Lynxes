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
import org.mahmoud.fastqueue.service.TopicService;
import org.mahmoud.fastqueue.service.ObjectMapperService;
import org.mahmoud.fastqueue.service.ConsumerGroupService;
import org.mahmoud.fastqueue.service.SimpleConsumerService;
import org.mahmoud.fastqueue.api.consumer.ConsumerGroupManager;
import org.mahmoud.fastqueue.service.ExecutorServiceProvider;
import org.mahmoud.fastqueue.server.async.RequestChannel;
import org.mahmoud.fastqueue.server.async.ResponseChannel;
import org.mahmoud.fastqueue.server.async.AsyncProcessor;
import org.mahmoud.fastqueue.server.async.ResponseProcessor;
import org.mahmoud.fastqueue.server.async.AsyncHttpServer;

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
        bind(TopicService.class).in(Singleton.class);
        bind(ObjectMapperService.class).in(Singleton.class);
        bind(ConsumerGroupManager.class).in(Singleton.class);
        // ConsumerGroupService is provided by @Provides method below
        
        // Bind infrastructure services
        bind(ExecutorService.class).toProvider(ExecutorServiceProvider.class);
        
        // HTTP servers are provided by @Provides methods below
        
        // RequestChannel and ResponseChannel are provided by @Provides methods below
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
     * Provides a configured ResponseChannel.
     */
    @Provides
    @Singleton
    public ResponseChannel provideResponseChannel(QueueConfig config) {
        return new ResponseChannel(1000); // Max 1000 queued responses
    }
    
    /**
     * Provides AsyncProcessor with all dependencies injected.
     */
    @Provides
    @Singleton
    public AsyncProcessor provideAsyncProcessor(RequestChannel requestChannel, ResponseChannel responseChannel,
                                               MessageService messageService, HealthService healthService,
                                               ObjectMapperService objectMapperService, ExecutorService executorService) {
        return new AsyncProcessor(requestChannel, responseChannel, messageService, healthService, objectMapperService, executorService);
    }
    
    /**
     * Provides ResponseProcessor with all dependencies injected.
     */
    @Provides
    @Singleton
    public ResponseProcessor provideResponseProcessor(ResponseChannel responseChannel, ExecutorService executorService) {
        return new ResponseProcessor(responseChannel, executorService);
    }
    
    /**
     * Provides ConsumerGroupService with dependencies injected.
     */
    @Provides
    @Singleton
    public ConsumerGroupService provideConsumerGroupService(ConsumerGroupManager consumerGroupManager,
                                                           org.mahmoud.fastqueue.api.topic.TopicRegistry topicRegistry,
                                                           QueueConfig config) {
        return new ConsumerGroupService(consumerGroupManager, topicRegistry, config);
    }
    
    @Provides
    @Singleton
    public SimpleConsumerService provideSimpleConsumerService(org.mahmoud.fastqueue.api.topic.TopicRegistry topicRegistry,
                                                             QueueConfig config) {
        return new SimpleConsumerService(topicRegistry, config);
    }
    
    /**
     * Provides AsyncHttpServer with all dependencies injected.
     */
    @Provides
    @Singleton
    public AsyncHttpServer provideAsyncHttpServer(QueueConfig config, RequestChannel requestChannel, 
                                                 ResponseChannel responseChannel, MessageService messageService, 
                                                 HealthService healthService, TopicService topicService,
                                                 ConsumerGroupService consumerGroupService, SimpleConsumerService simpleConsumerService, 
                                                 ObjectMapperService objectMapperService, AsyncProcessor asyncProcessor, ResponseProcessor responseProcessor, ExecutorService executorService) {
        return new AsyncHttpServer(config, requestChannel, responseChannel, messageService, healthService, topicService, consumerGroupService, simpleConsumerService, objectMapperService, asyncProcessor, responseProcessor, executorService);
    }
    
}
