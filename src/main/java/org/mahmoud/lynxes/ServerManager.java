package org.mahmoud.lynxes;

import org.mahmoud.lynxes.di.ServiceContainer;
import org.mahmoud.lynxes.server.async.AsyncHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle and operation of the Lynxes HTTP server.
 * This class handles server startup, shutdown hooks, logging, and the main server loop.
 * 
 * @author mahmoudreda
 */
public class ServerManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ServerManager.class);
    private static final int SERVER_SLEEP_INTERVAL_MS = 1000;
    
    private final ServiceContainer serviceContainer;
    private AsyncHttpServer httpServer;
    
    /**
     * Creates a new ServerManager instance.
     * 
     * @param environment The environment configuration to use
     */
    public ServerManager(String environment) {
        this.serviceContainer = ServiceContainer.getInstance(environment);
    }
    
    /**
     * Starts the HTTP server and begins serving requests.
     * 
     * @throws Exception if server startup fails
     */
    public void startServer() throws Exception {
        httpServer = serviceContainer.getService(AsyncHttpServer.class);
        httpServer.start();
        
        setupShutdownHook();
        logServerInfo();
        runServerLoop();
    }
    
    /**
     * Sets up a shutdown hook to gracefully stop the server.
     */
    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Lynxes Server...");
            try {
                if (httpServer != null) {
                    httpServer.stop();
                }
                logger.info("Lynxes Server shutdown completed");
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
    }
    
    /**
     * Logs server startup information and available endpoints.
     */
    private void logServerInfo() {
        if (httpServer == null) {
            logger.warn("Cannot log server info: HTTP server is null");
            return;
        }
        
        int port = httpServer.getPort();
        
        logger.info("Lynxes AsyncHttpServer started on port {}", port);
        logger.info("API Endpoints:");
        logger.info("  GET  /health - Health check");
        logger.info("  GET  /topics - List topics");
        logger.info("  POST /topics - Create topic");
        logger.info("  POST /topics/{name} - Publish message");
        logger.info("  GET  /topics/{name}?offset={n} - Consume message");
        logger.info("  GET  /metrics - Server metrics");
        logger.info("Press Ctrl+C to stop the server");
    }
    
    /**
     * Runs the main server loop, keeping the server running until it's stopped.
     * 
     * @throws InterruptedException if the sleep is interrupted
     */
    private void runServerLoop() throws InterruptedException {
        if (httpServer == null) {
            logger.error("Cannot run server loop: HTTP server is null");
            return;
        }
        
        while (httpServer.isRunning()) {
            Thread.sleep(SERVER_SLEEP_INTERVAL_MS);
        }
    }
    
    /**
     * Gets the current HTTP server instance.
     * 
     * @return The AsyncHttpServer instance, or null if not started
     */
    public AsyncHttpServer getHttpServer() {
        return httpServer;
    }
    
    /**
     * Checks if the server is currently running.
     * 
     * @return true if the server is running, false otherwise
     */
    public boolean isServerRunning() {
        return httpServer != null && httpServer.isRunning();
    }
    
    /**
     * Gets the port the server is running on.
     * 
     * @return The server port, or -1 if not started
     */
    public int getServerPort() {
        return httpServer != null ? httpServer.getPort() : -1;
    }
}
