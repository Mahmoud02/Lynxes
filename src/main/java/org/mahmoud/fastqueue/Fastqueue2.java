package org.mahmoud.fastqueue;

import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.di.ServiceContainer;
import org.mahmoud.fastqueue.server.async.AsyncHttpServer;
import org.mahmoud.fastqueue.server.http.JettyHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;

/**
 * FastQueue2 - High-Performance Message Queue Server
 *
 * @author mahmoudreda
 */
public class Fastqueue2 {
    private static final Logger logger = LoggerFactory.getLogger(Fastqueue2.class);

    public static void main(String[] args) {
        logger.info("FastQueue2 - High-Performance Message Queue Server");
        logger.info("==================================================");
        
        try {
            // Parse command line arguments
            String environment = parseEnvironment(args);
            
            // Start the HTTP server with configuration
            startHttpServer(environment);
            
        } catch (Exception e) {
            logger.error("Failed to start FastQueue2 server", e);
            System.exit(1);
        }
    }
    
    /**
     * Parses command line arguments to determine environment.
     */
    private static String parseEnvironment(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--env") && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return "default"; // Default environment
    }
    
    /**
     * Starts the HTTP server with dependency injection.
     */
    private static void startHttpServer(String environment) throws Exception {
        // Initialize dependency injection container
        ServiceContainer container = ServiceContainer.getInstance(environment);
        
        // Choose server type based on configuration or command line
        String serverType = getServerType(environment);
        
        if ("async".equalsIgnoreCase(serverType)) {
            // Get async HTTP server from DI container
            AsyncHttpServer httpServer = container.getService(AsyncHttpServer.class);
            httpServer.start();
            startServerLoop(httpServer, "AsyncHttpServer");
        } else {
            // Get synchronous Jetty HTTP server from DI container
            JettyHttpServer httpServer = container.getService(JettyHttpServer.class);
            httpServer.start();
            startServerLoop(httpServer, "JettyHttpServer");
        }
    }
    
    /**
     * Gets the server type to use (async or sync).
     */
    private static String getServerType(String environment) {
        // For now, default to async for dev/prod, sync for default
        if ("dev".equalsIgnoreCase(environment) || "prod".equalsIgnoreCase(environment)) {
            return "async";
        }
        return "sync"; // Default to synchronous for backward compatibility
    }
    
    /**
     * Starts the server loop and handles shutdown.
     */
    private static void startServerLoop(Object httpServer, String serverType) throws Exception {
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down FastQueue2 Server...");
            try {
                if (httpServer instanceof AsyncHttpServer) {
                    ((AsyncHttpServer) httpServer).stop();
                } else if (httpServer instanceof JettyHttpServer) {
                    ((JettyHttpServer) httpServer).stop();
                }
                logger.info("FastQueue2 Server shutdown completed");
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
        
        int port = 0;
        if (httpServer instanceof AsyncHttpServer) {
            port = ((AsyncHttpServer) httpServer).getPort();
        } else if (httpServer instanceof JettyHttpServer) {
            port = ((JettyHttpServer) httpServer).getPort();
        }
        
        logger.info("FastQueue2 {} started on port {}", serverType, port);
        logger.info("API Endpoints:");
        logger.info("  GET  /health - Health check");
        logger.info("  GET  /topics - List topics");
        logger.info("  POST /topics - Create topic");
        logger.info("  POST /topics/{name} - Publish message");
        logger.info("  GET  /topics/{name}?offset={n} - Consume message");
        logger.info("  GET  /metrics - Server metrics");
        logger.info("Press Ctrl+C to stop the server");
        
        // Keep server running
        boolean isRunning = false;
        if (httpServer instanceof AsyncHttpServer) {
            isRunning = ((AsyncHttpServer) httpServer).isRunning();
        } else if (httpServer instanceof JettyHttpServer) {
            isRunning = ((JettyHttpServer) httpServer).isRunning();
        }
        
        while (isRunning) {
            Thread.sleep(1000);
            // Check if still running
            if (httpServer instanceof AsyncHttpServer) {
                isRunning = ((AsyncHttpServer) httpServer).isRunning();
            } else if (httpServer instanceof JettyHttpServer) {
                isRunning = ((JettyHttpServer) httpServer).isRunning();
            }
        }
    }
    
}
