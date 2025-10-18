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
import org.mahmoud.lynxes.service.ServerUptimeService;
import org.mahmoud.lynxes.api.consumer.ConsumerGroupManager;
import org.mahmoud.lynxes.service.ExecutorServiceProvider;
import org.mahmoud.lynxes.server.pipeline.channels.RequestChannel;
import org.mahmoud.lynxes.server.pipeline.channels.ResponseChannel;
import org.mahmoud.lynxes.server.pipeline.orchestration.AsyncRequestProcessorOrchestrator;
import org.mahmoud.lynxes.server.pipeline.orchestration.ResponseProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.HealthRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.TopicsRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.MetricsRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.PublishRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.ConsumeRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.DeleteTopicRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.DeleteAllTopicsRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.processors.CreateTopicRequestProcessor;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessorFactory;
import org.mahmoud.lynxes.server.api.HealthRouteHandler;
import org.mahmoud.lynxes.server.api.TopicsRouteHandler;
import org.mahmoud.lynxes.server.api.TopicRouteHandler;
import org.mahmoud.lynxes.server.api.MetricsRouteHandler;
import org.mahmoud.lynxes.server.api.ConsumerGroupRouteHandler;
import org.mahmoud.lynxes.server.api.ConsumerRouteHandler;
import org.mahmoud.lynxes.server.ServletRouteMapper;
import org.mahmoud.lynxes.server.HttpServerConfigurator;
import org.mahmoud.lynxes.server.AsyncHttpServer;
import java.util.List;
import java.util.Arrays;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;

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
        bind(ServerUptimeService.class).in(Singleton.class);
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
        
        // Bind request processors
        bind(HealthRequestProcessor.class).in(Singleton.class);
        bind(TopicsRequestProcessor.class).in(Singleton.class);
        bind(MetricsRequestProcessor.class).in(Singleton.class);
        bind(PublishRequestProcessor.class).in(Singleton.class);
        bind(ConsumeRequestProcessor.class).in(Singleton.class);
        bind(DeleteTopicRequestProcessor.class).in(Singleton.class);
        bind(DeleteAllTopicsRequestProcessor.class).in(Singleton.class);
        bind(CreateTopicRequestProcessor.class).in(Singleton.class);
        
        // Bind request processor factory
        bind(RequestProcessorFactory.class).toProvider(RequestProcessorFactoryProvider.class).in(Singleton.class);
        
        // Bind orchestrator
        bind(AsyncRequestProcessorOrchestrator.class).in(Singleton.class);
        
        // Bind server components
        bind(ServletRouteMapper.class).in(Singleton.class);
        bind(HttpServerConfigurator.class).in(Singleton.class);
        // AsyncHttpServer is provided by @Provides method below
        
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
        return new ConsumerGroupService(consumerGroupManager, config);
    }
    
    @Provides
    @Singleton
    public SimpleConsumerService provideSimpleConsumerService(org.mahmoud.lynxes.api.topic.TopicRegistry topicRegistry,
                                                             QueueConfig config) {
        return new SimpleConsumerService(config);
    }
    
    /**
     * Provides AsyncHttpServer with all dependencies injected.
     */
    @Provides
    @Singleton
    public AsyncHttpServer provideAsyncHttpServer(QueueConfig config, RequestChannel requestChannel, 
                                                 ResponseChannel responseChannel, AsyncRequestProcessorOrchestrator orchestrator, 
                                                 ResponseProcessor responseProcessor, ServletRouteMapper servletRouteMapper, 
                                                 HttpServerConfigurator serverConfigurator) {
        return new AsyncHttpServer(config, requestChannel, responseChannel, orchestrator, responseProcessor, servletRouteMapper, serverConfigurator);
    }
    
    /**
     * Provider for RequestProcessorFactory.
     */
    public static class RequestProcessorFactoryProvider implements com.google.inject.Provider<RequestProcessorFactory> {
        private final HealthRequestProcessor healthProcessor;
        private final TopicsRequestProcessor topicsProcessor;
        private final MetricsRequestProcessor metricsProcessor;
        private final PublishRequestProcessor publishProcessor;
        private final ConsumeRequestProcessor consumeProcessor;
        private final DeleteTopicRequestProcessor deleteTopicProcessor;
        private final DeleteAllTopicsRequestProcessor deleteAllTopicsProcessor;
        private final CreateTopicRequestProcessor createTopicProcessor;
        
        @com.google.inject.Inject
        public RequestProcessorFactoryProvider(HealthRequestProcessor healthProcessor,
                                             TopicsRequestProcessor topicsProcessor,
                                             MetricsRequestProcessor metricsProcessor,
                                             PublishRequestProcessor publishProcessor,
                                             ConsumeRequestProcessor consumeProcessor,
                                             DeleteTopicRequestProcessor deleteTopicProcessor,
                                             DeleteAllTopicsRequestProcessor deleteAllTopicsProcessor,
                                             CreateTopicRequestProcessor createTopicProcessor) {
            this.healthProcessor = healthProcessor;
            this.topicsProcessor = topicsProcessor;
            this.metricsProcessor = metricsProcessor;
            this.publishProcessor = publishProcessor;
            this.consumeProcessor = consumeProcessor;
            this.deleteTopicProcessor = deleteTopicProcessor;
            this.deleteAllTopicsProcessor = deleteAllTopicsProcessor;
            this.createTopicProcessor = createTopicProcessor;
        }
        
        @Override
        public RequestProcessorFactory get() {
            List<RequestProcessor> processors = Arrays.asList(
                healthProcessor,
                topicsProcessor,
                metricsProcessor,
                publishProcessor,
                consumeProcessor,
                deleteTopicProcessor,
                deleteAllTopicsProcessor,
                createTopicProcessor
            );
            return new RequestProcessorFactory(processors);
        }
    }
    
}
