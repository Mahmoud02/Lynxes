package org.mahmoud.fastqueue;

import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.server.http.JettyHttpServer;
import java.nio.file.Path;

/**
 * FastQueue2 - High-Performance Message Queue Server
 *
 * @author mahmoudreda
 */
public class Fastqueue2 {

    public static void main(String[] args) {
        System.out.println("FastQueue2 - High-Performance Message Queue Server");
        System.out.println("==================================================");
        
        try {
            // Start the HTTP server
            startHttpServer();
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Starts the HTTP server for production use.
     */
    private static void startHttpServer() throws Exception {
        // Create configuration
        QueueConfig config = new QueueConfig.Builder()
            .dataDirectory(Path.of("./data"))
            .maxSegmentSize(1024 * 1024) // 1MB segments
            .retentionPeriodMs(7 * 24 * 60 * 60 * 1000L) // 7 days
            .serverPort(8080)
            .threadPoolSize(10)
            .build();
        
        // Create and start HTTP server
        JettyHttpServer httpServer = new JettyHttpServer(config);
        httpServer.start();
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down FastQueue2 Server...");
            try {
                httpServer.stop();
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
            }
        }));
        
        System.out.println("FastQueue2 HTTP Server started on port " + httpServer.getPort());
        System.out.println("API Endpoints:");
        System.out.println("  GET  /health - Health check");
        System.out.println("  GET  /topics - List topics");
        System.out.println("  POST /topics - Create topic");
        System.out.println("  POST /topics/{name} - Publish message");
        System.out.println("  GET  /topics/{name}?offset={n} - Consume message");
        System.out.println("  GET  /metrics - Server metrics");
        System.out.println("\nPress Ctrl+C to stop the server");
        
        // Keep server running
        while (httpServer.isRunning()) {
            Thread.sleep(1000);
        }
    }
    
}
