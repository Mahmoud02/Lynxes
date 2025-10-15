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
import org.mahmoud.lynxes.server.pipeline.RequestChannel;
import org.mahmoud.lynxes.server.pipeline.ResponseChannel;
import org.mahmoud.lynxes.server.pipeline.AsyncProcessor;
import org.mahmoud.lynxes.server.pipeline.ResponseProcessor;
import org.mahmoud.lynxes.server.api.HealthRouteHandler;
import org.mahmoud.lynxes.server.api.TopicsRouteHandler;
import org.mahmoud.lynxes.server.api.TopicRouteHandler;
import org.mahmoud.lynxes.server.api.MetricsRouteHandler;
import org.mahmoud.lynxes.server.api.ConsumerGroupRouteHandler;
import org.mahmoud.lynxes.server.api.ConsumerRouteHandler;
import org.mahmoud.lynxes.server.ServletRouteMapper;
import org.mahmoud.lynxes.server.HttpServerConfigurator;
import org.mahmoud.lynxes.server.AsyncHttpServer;

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
        
        // Bind route handlers (no service dependencies)
        bind(HealthRouteHandler.class).in(Singleton.class);
        bind(TopicsRouteHandler.class).in(Singleton.class);
        bind(TopicRouteHandler.class).in(Singleton.class);
        bind(MetricsRouteHandler.class).in(Singleton.class);
        bind(ConsumerGroupRouteHandler.class).in(Singleton.class);
        bind(ConsumerRouteHandler.class).in(Singleton.class);
        
        // Bind server components
        bind(ServletRouteMapper.class).in(Singleton.class);
        bind(HttpServerConfigurator.class).in(Singleton.class);
        bind(AsyncHttpServer.class).in(Singleton.class);
        
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
                                               ConsumerGroupService consumerGroupService,
                                               SimpleConsumerService simpleConsumerService,
                                               ObjectMapperService objectMapperService, ExecutorService executorService) {
        return new AsyncProcessor(requestChannel, responseChannel, messageService, healthService, consumerGroupService, simpleConsumerService, objectMapperService, executorService);
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
                                                 ResponseChannel responseChannel, AsyncProcessor asyncProcessor, 
                                                 ResponseProcessor responseProcessor, ServletRouteMapper servletRouteMapper, 
                                                 HttpServerConfigurator serverConfigurator) {
        return new AsyncHttpServer(config, requestChannel, responseChannel, asyncProcessor, responseProcessor, servletRouteMapper, serverConfigurator);
    }
    
}
