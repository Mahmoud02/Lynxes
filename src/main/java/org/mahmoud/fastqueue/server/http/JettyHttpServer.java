package org.mahmoud.fastqueue.server.http;

import org.mahmoud.fastqueue.api.producer.Producer;
import org.mahmoud.fastqueue.api.consumer.Consumer;
import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.core.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * HTTP server using embedded Jetty for FastQueue2 message queue.
 * Provides REST API endpoints for message publishing and consumption.
 */
public class JettyHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(JettyHttpServer.class);
    
    private final QueueConfig config;
    public final Producer producer;
    public final Consumer consumer;
    private final ObjectMapper objectMapper;
    private final ThreadPoolExecutor executor;
    private Server server;
    private volatile boolean running;

    /**
     * Creates a new Jetty HTTP server with the specified configuration.
     * 
     * @param config The queue configuration
     */
    public JettyHttpServer(QueueConfig config) {
        this.config = config;
        this.producer = new Producer(config);
        this.consumer = new Consumer(config);
        this.objectMapper = new ObjectMapper();
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getThreadPoolSize());
        this.running = false;
    }

    /**
     * Starts the HTTP server.
     * 
     * @throws Exception if the server cannot be started
     */
    public void start() throws Exception {
        if (running) {
            throw new IllegalStateException("Server is already running");
        }
        
        // Create Jetty server
        this.server = new Server(config.getServerPort());
        
        // Create servlet context
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        
        // Add servlets
        context.addServlet(new ServletHolder(new HealthServlet()), "/health");
        context.addServlet(new ServletHolder(new TopicsServlet()), "/topics");
        context.addServlet(new ServletHolder(new MessageServlet()), "/topics/*");
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        
        // Set the servlet context
        server.setHandler(context);
        
        // Start server
        server.start();
        running = true;
        logger.info("FastQueue2 Jetty HTTP Server started on port {}", config.getServerPort());
    }

    /**
     * Stops the HTTP server.
     * 
     * @throws Exception if the server cannot be stopped
     */
    public void stop() throws Exception {
        if (!running) {
            return;
        }
        
        server.stop();
        executor.shutdown();
        running = false;
        logger.info("FastQueue2 Jetty HTTP Server stopped");
    }

    /**
     * Checks if the server is running.
     * 
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Gets the server port.
     * 
     * @return The server port
     */
    public int getPort() {
        return config.getServerPort();
    }

    /**
     * Health check servlet.
     */
    private class HealthServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            logger.debug("Health check requested from {}", request.getRemoteAddr());
            
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            HealthResponse healthResponse = new HealthResponse("ok", "FastQueue2 is running");
            String json = objectMapper.writeValueAsString(healthResponse);
            
            try (PrintWriter out = response.getWriter()) {
                out.print(json);
            }
            
            logger.debug("Health check completed successfully");
        }
    }

    /**
     * Topics management servlet.
     */
    private class TopicsServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            TopicsResponse topicsResponse = new TopicsResponse(producer.getTopicNames());
            String json = objectMapper.writeValueAsString(topicsResponse);
            
            try (PrintWriter out = response.getWriter()) {
                out.print(json);
            }
        }
        
        @Override
        protected void doPost(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            MessageResponse messageResponse = new MessageResponse("Topic will be created on first use");
            String json = objectMapper.writeValueAsString(messageResponse);
            
            try (PrintWriter out = response.getWriter()) {
                out.print(json);
            }
        }
    }

    /**
     * Message handling servlet for specific topics.
     */
    private class MessageServlet extends HttpServlet {
        @Override
        protected void doPost(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            String topicName = null;
            try {
                String path = request.getPathInfo();
                if (path == null || path.length() < 2) {
                    sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid path");
                    return;
                }
                
                topicName = path.substring(1); // Remove leading slash
                
                // Read request body
                String requestBody = new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                
                // Parse JSON request
                PublishRequest publishRequest = objectMapper.readValue(requestBody, PublishRequest.class);
                
                // Publish message
                logger.info("Publishing message to topic: {}", topicName);
                Record record = producer.publish(topicName, publishRequest.message.getBytes(StandardCharsets.UTF_8));
                
                // Send response
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_OK);
                
                PublishResponse publishResponse = new PublishResponse(record.getOffset(), record.getTimestamp());
                String json = objectMapper.writeValueAsString(publishResponse);
                
                try (PrintWriter out = response.getWriter()) {
                    out.print(json);
                }
                
                logger.info("Message published successfully to topic: {} with offset: {}", topicName, record.getOffset());
                
            } catch (Exception e) {
                logger.error("Error processing publish request for topic: {}", topicName, e);
                sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid request: " + e.getMessage());
            }
        }
        
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            String topicName = null;
            long offset = 0;
            try {
                String path = request.getPathInfo();
                if (path == null || path.length() < 2) {
                    sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid path");
                    return;
                }
                
                topicName = path.substring(1); // Remove leading slash
                
                // Get offset parameter
                String offsetParam = request.getParameter("offset");
                
                if (offsetParam != null) {
                    offset = Long.parseLong(offsetParam);
                }
                
                // Consume message
                logger.debug("Consuming message from topic: {} at offset: {}", topicName, offset);
                Record record = consumer.consume(topicName, offset);
                
                if (record == null) {
                    logger.debug("No message found at offset {} for topic: {}", offset, topicName);
                    sendError(response, HttpServletResponse.SC_NOT_FOUND, "No message found at offset " + offset);
                    return;
                }
                
                // Send response
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_OK);
                
                ConsumeResponse consumeResponse = new ConsumeResponse(
                    record.getOffset(),
                    record.getTimestamp(),
                    new String(record.getData(), StandardCharsets.UTF_8)
                );
                String json = objectMapper.writeValueAsString(consumeResponse);
                
                try (PrintWriter out = response.getWriter()) {
                    out.print(json);
                }
                
                logger.debug("Message consumed successfully from topic: {} at offset: {}", topicName, record.getOffset());
                
            } catch (Exception e) {
                logger.error("Error processing consume request for topic: {} at offset: {}", topicName, offset, e);
                sendError(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid request: " + e.getMessage());
            }
        }
    }

    /**
     * Metrics servlet.
     */
    private class MetricsServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            MetricsResponse metricsResponse = new MetricsResponse(
                producer.getTopicCount(),
                producer.getMessageCount(),
                consumer.getTopicCount(),
                consumer.getMessageCount()
            );
            String json = objectMapper.writeValueAsString(metricsResponse);
            
            try (PrintWriter out = response.getWriter()) {
                out.print(json);
            }
        }
    }

    /**
     * Sends an error response.
     */
    private void sendError(HttpServletResponse response, int statusCode, String message) 
            throws IOException {
        response.setContentType("application/json");
        response.setStatus(statusCode);
        
        ErrorResponse errorResponse = new ErrorResponse(statusCode, message);
        String json = objectMapper.writeValueAsString(errorResponse);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }

    // Response classes
    public static class HealthResponse {
        public String status;
        public String message;
        
        public HealthResponse(String status, String message) {
            this.status = status;
            this.message = message;
        }
    }

    public static class TopicsResponse {
        public String[] topics;
        
        public TopicsResponse(String[] topics) {
            this.topics = topics;
        }
    }

    public static class MessageResponse {
        public String message;
        
        public MessageResponse(String message) {
            this.message = message;
        }
    }

    public static class PublishRequest {
        public String message;
    }

    public static class PublishResponse {
        public long offset;
        public long timestamp;
        
        public PublishResponse(long offset, long timestamp) {
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }

    public static class ConsumeResponse {
        public long offset;
        public long timestamp;
        public String message;
        
        public ConsumeResponse(long offset, long timestamp, String message) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.message = message;
        }
    }

    public static class MetricsResponse {
        public int producerTopics;
        public long producerMessages;
        public int consumerTopics;
        public long consumerMessages;
        
        public MetricsResponse(int producerTopics, long producerMessages, int consumerTopics, long consumerMessages) {
            this.producerTopics = producerTopics;
            this.producerMessages = producerMessages;
            this.consumerTopics = consumerTopics;
            this.consumerMessages = consumerMessages;
        }
    }

    public static class ErrorResponse {
        public int code;
        public String message;
        
        public ErrorResponse(int code, String message) {
            this.code = code;
            this.message = message;
        }
    }
}
