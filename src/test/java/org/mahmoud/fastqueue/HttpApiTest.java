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
        
        // Create configuration with temporary directory
        QueueConfig config = new QueueConfig();
        // Set the data directory to the temporary directory for test isolation
        // Note: This is a workaround since we removed the Builder pattern
        // In a real scenario, we would use ConfigLoader with test-specific config
        
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
        int port = server.getPort();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/health"))
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
        int port = server.getPort();
        
        // Publish message
        String publishJson = String.format("{\"message\":\"%s\"}", message);
        HttpRequest publishRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/topics/" + topicName))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(publishJson))
            .build();
        
        HttpResponse<String> publishResponse = httpClient.send(publishRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, publishResponse.statusCode());
        assertTrue(publishResponse.body().contains("offset"));
        assertTrue(publishResponse.body().contains("timestamp"));
        
        // Consume message
        HttpRequest consumeRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + port + "/topics/" + topicName + "?offset=0"))
            .GET()
            .build();
        
        HttpResponse<String> consumeResponse = httpClient.send(consumeRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, consumeResponse.statusCode());
        assertTrue(consumeResponse.body().contains("\"message\":\"Hello FastQueue2!\""));
        assertTrue(consumeResponse.body().contains("\"offset\":"));
        assertTrue(consumeResponse.body().contains("\"timestamp\":"));
    }

    @Test
    void testTopicsEndpoint() throws Exception {
        // Test topics endpoint
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + server.getPort() + "/topics"))
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
            .uri(URI.create("http://localhost:" + server.getPort() + "/metrics"))
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
                .uri(URI.create("http://localhost:" + server.getPort() + "/topics/" + topicName))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(publishJson))
                .build();
            
            HttpResponse<String> publishResponse = httpClient.send(publishRequest, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, publishResponse.statusCode());
        }
        
        // Wait for messages to be persisted
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Consume messages with retry logic
        for (int i = 0; i < 5; i++) {
            boolean messageFound = false;
            int retries = 3;
            
            while (!messageFound && retries > 0) {
                HttpRequest consumeRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + server.getPort() + "/topics/" + topicName + "?offset=" + i))
                    .GET()
                    .build();
                
                HttpResponse<String> consumeResponse = httpClient.send(consumeRequest, HttpResponse.BodyHandlers.ofString());
                
                if (consumeResponse.statusCode() == 200) {
                    assertTrue(consumeResponse.body().contains("Message " + i));
                    messageFound = true;
                } else if (consumeResponse.statusCode() == 404) {
                    // Message not found, wait and retry
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    retries--;
                } else {
                    fail("Unexpected response code: " + consumeResponse.statusCode() + " - " + consumeResponse.body());
                }
            }
            
            assertTrue(messageFound, "Message " + i + " not found after retries");
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
