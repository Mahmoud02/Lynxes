package org.mahmoud.lynxes.di;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.concurrent.ExecutorService;
import org.mahmoud.lynxes.config.QueueConfig;
import org.mahmoud.lynxes.config.ConfigLoader;
import org.mahmoud.lynxes.api.producer.Producer;
import org.mahmoud.lynxes.api.consumer.Consumer;
import org.mahmoud.lynxes.service.MessageService;
import org.mahmoud.lynxes.service.HealthService;
import org.mahmoud.lynxes.service.TopicService;
import org.mahmoud.lynxes.service.ObjectMapperService;
import org.mahmoud.lynxes.service.ConsumerGroupService;
import org.mahmoud.lynxes.service.SimpleConsumerService;
import org.mahmoud.lynxes.api.consumer.ConsumerGroupManager;
import org.mahmoud.lynxes.service.ExecutorServiceProvider;
import org.mahmoud.lynxes.server.async.RequestChannel;
import org.mahmoud.lynxes.server.async.ResponseChannel;
import org.mahmoud.lynxes.server.async.AsyncProcessor;
import org.mahmoud.lynxes.server.async.ResponseProcessor;
import org.mahmoud.lynxes.server.async.AsyncHttpServer;

/**
 * Guice module for Lynxes dependency injection configuration.
 * This module defines how dependencies should be created and injected.
 */
public class LynxesModule extends AbstractModule {
    
    private final String environment;
    
    public LynxesModule(String environment) {
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
                                                           org.mahmoud.lynxes.api.topic.TopicRegistry topicRegistry,
                                                           QueueConfig config) {
        return new ConsumerGroupService(consumerGroupManager, topicRegistry, config);
    }
    
    @Provides
    @Singleton
    public SimpleConsumerService provideSimpleConsumerService(org.mahmoud.lynxes.api.topic.TopicRegistry topicRegistry,
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
