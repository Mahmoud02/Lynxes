package org.mahmoud.fastqueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.mahmoud.fastqueue.server.http.JettyHttpServer;
import org.mahmoud.fastqueue.config.QueueConfig;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for HTTP API functionality.
 */
public class HttpApiTest {
    private JettyHttpServer server;
    private HttpClient httpClient;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directory
        tempDir = Files.createTempDirectory("fastqueue-http-test");
        
        // Create configuration
        QueueConfig config = new QueueConfig.Builder()
            .dataDirectory(tempDir)
            .maxSegmentSize(1024) // 1KB for testing
            .retentionPeriodMs(86400000) // 1 day
            .serverPort(8080)
            .threadPoolSize(5)
            .build();
        
        // Create and start server
        server = new JettyHttpServer(config);
        server.start();
        
        // Create HTTP client
        httpClient = HttpClient.newHttpClient();
        
        // Wait for server to start
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        
        // Clean up temporary directory
        if (tempDir != null) {
            deleteDirectory(tempDir);
        }
    }

    @Test
    void testHealthEndpoint() throws Exception {
        // Test health endpoint
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/health"))
            .GET()
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("ok"));
        assertTrue(response.body().contains("FastQueue2 is running"));
    }

    @Test
    void testPublishAndConsumeMessage() throws Exception {
        String topicName = "test-topic";
        String message = "Hello, FastQueue2!";
        
        // Publish message
        String publishJson = String.format("{\"message\":\"%s\"}", message);
        HttpRequest publishRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/topics/" + topicName))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(publishJson))
            .build();
        
        HttpResponse<String> publishResponse = httpClient.send(publishRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, publishResponse.statusCode());
        assertTrue(publishResponse.body().contains("offset"));
        assertTrue(publishResponse.body().contains("timestamp"));
        
        // Consume message
        HttpRequest consumeRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/topics/" + topicName + "?offset=0"))
            .GET()
            .build();
        
        HttpResponse<String> consumeResponse = httpClient.send(consumeRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, consumeResponse.statusCode());
        assertTrue(consumeResponse.body().contains(message));
        assertTrue(consumeResponse.body().contains("offset"));
        assertTrue(consumeResponse.body().contains("timestamp"));
    }

    @Test
    void testTopicsEndpoint() throws Exception {
        // Test topics endpoint
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/topics"))
            .GET()
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("topics"));
    }

    @Test
    void testMetricsEndpoint() throws Exception {
        // Test metrics endpoint
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/metrics"))
            .GET()
            .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("producerTopics"));
        assertTrue(response.body().contains("producerMessages"));
        assertTrue(response.body().contains("consumerTopics"));
        assertTrue(response.body().contains("consumerMessages"));
    }

    @Test
    void testMultipleMessages() throws Exception {
        String topicName = "multi-topic";
        
        // Publish multiple messages
        for (int i = 0; i < 5; i++) {
            String message = "Message " + i;
            String publishJson = String.format("{\"message\":\"%s\"}", message);
            HttpRequest publishRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/topics/" + topicName))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(publishJson))
                .build();
            
            HttpResponse<String> publishResponse = httpClient.send(publishRequest, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, publishResponse.statusCode());
        }
        
        // Consume messages
        for (int i = 0; i < 5; i++) {
            HttpRequest consumeRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/topics/" + topicName + "?offset=" + i))
                .GET()
                .build();
            
            HttpResponse<String> consumeResponse = httpClient.send(consumeRequest, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, consumeResponse.statusCode());
            assertTrue(consumeResponse.body().contains("Message " + i));
            
            // Add a small delay to ensure messages are processed
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void deleteDirectory(Path directory) {
        try {
            Files.walk(directory)
                .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        System.err.println("Failed to delete " + path + ": " + e.getMessage());
                    }
                });
        } catch (IOException e) {
            System.err.println("Failed to walk directory " + directory + ": " + e.getMessage());
        }
    }
}
