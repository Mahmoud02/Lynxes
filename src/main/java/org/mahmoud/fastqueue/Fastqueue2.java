package org.mahmoud.fastqueue;

import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.config.ConfigLoader;
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
     * Starts the HTTP server with Typesafe Config configuration.
     */
    private static void startHttpServer(String environment) throws Exception {
        // Load configuration using Typesafe Config
        QueueConfig config = ConfigLoader.loadConfig(environment);
        
        // Create and start HTTP server
        JettyHttpServer httpServer = new JettyHttpServer(config);
        httpServer.start();
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down FastQueue2 Server...");
            try {
                httpServer.stop();
                logger.info("FastQueue2 Server shutdown completed");
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
        
        logger.info("FastQueue2 HTTP Server started on port {}", httpServer.getPort());
        logger.info("API Endpoints:");
        logger.info("  GET  /health - Health check");
        logger.info("  GET  /topics - List topics");
        logger.info("  POST /topics - Create topic");
        logger.info("  POST /topics/{name} - Publish message");
        logger.info("  GET  /topics/{name}?offset={n} - Consume message");
        logger.info("  GET  /metrics - Server metrics");
        logger.info("Press Ctrl+C to stop the server");
        
        // Keep server running
        while (httpServer.isRunning()) {
            Thread.sleep(1000);
        }
    }
    
}
