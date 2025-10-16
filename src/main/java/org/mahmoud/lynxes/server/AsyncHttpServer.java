package org.mahmoud.lynxes.server;

import org.mahmoud.lynxes.config.QueueConfig;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.mahmoud.lynxes.server.pipeline.channels.RequestChannel;
import org.mahmoud.lynxes.server.pipeline.channels.ResponseChannel;
import org.mahmoud.lynxes.server.pipeline.orchestration.AsyncRequestProcessorOrchestrator;
import org.mahmoud.lynxes.server.pipeline.orchestration.ResponseProcessor;

/**
 * Async HTTP server that uses Jetty as network threads and async processing for I/O.
 * Implements Kafka-inspired architecture with request channel and async processing.
 * 
 * This class has been refactored to delegate servlet management to ServletRegistry
 * and server configuration to HttpServerConfigurator for better separation of concerns.
 */
public class AsyncHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(AsyncHttpServer.class);
    
    private final QueueConfig config;
    private final RequestChannel requestChannel;
    private final ResponseChannel responseChannel;
    private final AsyncRequestProcessorOrchestrator orchestrator;
    private final ResponseProcessor responseProcessor;
    private final ServletRouteMapper servletRouteMapper;
    private final HttpServerConfigurator serverConfigurator;
    private volatile boolean running;
    
    @Inject
    public AsyncHttpServer(QueueConfig config, RequestChannel requestChannel, ResponseChannel responseChannel,
                          AsyncRequestProcessorOrchestrator orchestrator, ResponseProcessor responseProcessor, 
                          ServletRouteMapper servletRouteMapper, HttpServerConfigurator serverConfigurator) {
        this.config = config;
        this.requestChannel = requestChannel;
        this.responseChannel = responseChannel;
        this.orchestrator = orchestrator;
        this.responseProcessor = responseProcessor;
        this.servletRouteMapper = servletRouteMapper;
        this.serverConfigurator = serverConfigurator;
        
        this.running = false;
        
        initializeServer();
        logger.info("AsyncHttpServer initialized on port {}", config.getServerPort());
    }
    
    /**
     * Initializes the server configuration and servlet registration.
     */
    private void initializeServer() {
        try {
            serverConfigurator.initializeServer(requestChannel);
            logger.info("Server initialized with endpoints: {}", String.join(", ", servletRouteMapper.getMappedRoutes()));
        } catch (Exception e) {
            logger.error("Failed to initialize server", e);
            throw new RuntimeException("Server initialization failed", e);
        }
    }
    
    /**
     * Starts the async HTTP server.
     */
    public void start() throws Exception {
        if (!running) {
            logger.info("Starting AsyncHttpServer...");
            
            // Start processors first
            orchestrator.start();
            responseProcessor.start();
            
            // Start Jetty server
            serverConfigurator.startServer();
            running = true;
            
            logger.info("AsyncHttpServer started successfully on port {}", config.getServerPort());
            logger.info("RequestChannel metrics: {}", requestChannel.toString());
            logger.info("ResponseChannel metrics: {}", responseChannel.toString());
            logger.info("Server info: {}", serverConfigurator.getServerInfo());
        } else {
            logger.warn("AsyncHttpServer is already running");
        }
    }
    
    /**
     * Stops the async HTTP server.
     */
    public void stop() throws Exception {
        if (running) {
            logger.info("Stopping AsyncHttpServer...");
            
            // Stop Jetty server
            serverConfigurator.stopServer();
            
            // Stop processors
            orchestrator.stop();
            responseProcessor.stop();
            
            running = false;
            logger.info("AsyncHttpServer stopped successfully");
        } else {
            logger.warn("AsyncHttpServer is not running");
        }
    }
    
    /**
     * Checks if the server is running.
     */
    public boolean isRunning() {
        return running && serverConfigurator.isServerRunning();
    }
    
    /**
     * Gets the server port.
     */
    public int getPort() {
        return config.getServerPort();
    }
}
