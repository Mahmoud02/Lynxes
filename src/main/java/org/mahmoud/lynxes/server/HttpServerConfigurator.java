package org.mahmoud.lynxes.server;

import org.mahmoud.lynxes.config.QueueConfig;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.mahmoud.lynxes.server.pipeline.RequestChannel;
import org.slf4j.LoggerFactory;

/**
 * Configures and manages the Jetty HTTP server for Lynxes.
 * This class handles server setup, configuration, and lifecycle management.
 */
public class HttpServerConfigurator {
    private static final Logger logger = LoggerFactory.getLogger(HttpServerConfigurator.class);
    
    private final QueueConfig config;
    private final ServletRouteMapper servletRouteMapper;
    private Server server;
    private ServletContextHandler contextHandler;
    
    // Server configuration constants
    private static final String CONTEXT_PATH = "/";
    private static final int DEFAULT_TIMEOUT_MS = 30000;
    
    /**
     * Constructs an HttpServerConfigurator with the given configuration and servlet route mapper.
     * 
     * @param config The queue configuration
     * @param servletRouteMapper The servlet route mapper for endpoint management
     */
    public HttpServerConfigurator(QueueConfig config, ServletRouteMapper servletRouteMapper) {
        this.config = config;
        this.servletRouteMapper = servletRouteMapper;
    }
    
    /**
     * Initializes and configures the Jetty server.
     * 
     * @param requestChannel The request channel to make available to servlets
     * @throws Exception if server initialization fails
     */
    public void initializeServer(RequestChannel requestChannel) throws Exception {
        logger.info("Initializing Jetty server on port {}", config.getServerPort());
        
        // Create and configure server
        this.server = new Server(config.getServerPort());
        
        // Create and configure servlet context
        this.contextHandler = createServletContextHandler(requestChannel);
        
        // Map routes to servlets
        servletRouteMapper.mapRoutes(contextHandler);
        
        // Set the handler
        server.setHandler(contextHandler);
        
        logger.info("Jetty server initialized successfully");
    }
    
    /**
     * Creates and configures the servlet context handler.
     * 
     * @param requestChannel The request channel to make available to servlets
     * @return Configured servlet context handler
     */
    private ServletContextHandler createServletContextHandler(RequestChannel requestChannel) {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath(CONTEXT_PATH);
        
        // Configure async support
        context.getSessionHandler().setMaxInactiveInterval(DEFAULT_TIMEOUT_MS);
        
        // Make RequestChannel available to servlets
        context.setAttribute("requestChannel", requestChannel);
        
        logger.debug("Created servlet context handler with path: {}", CONTEXT_PATH);
        return context;
    }
    
    /**
     * Starts the configured server.
     * 
     * @throws Exception if server startup fails
     */
    public void startServer() throws Exception {
        if (server == null) {
            throw new IllegalStateException("Server not initialized. Call initializeServer() first.");
        }
        
        if (server.isStarted()) {
            logger.warn("Server is already started");
            return;
        }
        
        logger.info("Starting Jetty server...");
        server.start();
        logger.info("Jetty server started successfully on port {}", config.getServerPort());
    }
    
    /**
     * Stops the configured server.
     * 
     * @throws Exception if server shutdown fails
     */
    public void stopServer() throws Exception {
        if (server == null) {
            logger.warn("Server not initialized, nothing to stop");
            return;
        }
        
        if (!server.isStarted()) {
            logger.warn("Server is not started, nothing to stop");
            return;
        }
        
        logger.info("Stopping Jetty server...");
        server.stop();
        logger.info("Jetty server stopped successfully");
    }
    
    /**
     * Checks if the server is running.
     * 
     * @return true if server is running, false otherwise
     */
    public boolean isServerRunning() {
        return server != null && server.isRunning();
    }
    
    /**
     * Gets the server port.
     * 
     * @return The configured server port
     */
    public int getServerPort() {
        return config.getServerPort();
    }
    
    /**
     * Gets the underlying Jetty server instance.
     * 
     * @return The Jetty server instance
     */
    public Server getServer() {
        return server;
    }
    
    /**
     * Gets the servlet context handler.
     * 
     * @return The servlet context handler
     */
    public ServletContextHandler getContextHandler() {
        return contextHandler;
    }
    
    /**
     * Gets server configuration information for logging/debugging.
     * 
     * @return Server configuration summary
     */
    public String getServerInfo() {
        if (server == null) {
            return "Server not initialized";
        }
        
        return String.format("Jetty Server - Port: %d, Running: %s, Started: %s", 
                           config.getServerPort(), 
                           server.isRunning(), 
                           server.isStarted());
    }
}
