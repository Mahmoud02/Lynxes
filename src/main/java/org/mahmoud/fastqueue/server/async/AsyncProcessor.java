package org.mahmoud.fastqueue.server.async;

import org.mahmoud.fastqueue.service.MessageService;
import org.mahmoud.fastqueue.service.HealthService;
import org.mahmoud.fastqueue.core.Record;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async processor that handles message operations in a separate thread pool.
 * Similar to Kafka's KafkaRequestHandler but adapted for HTTP requests.
 */
public class AsyncProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProcessor.class);
    
    private final RequestChannel requestChannel;
    private final ResponseChannel responseChannel;
    private final ExecutorService ioThreadPool;
    private final MessageService messageService;
    private final HealthService healthService;
    private final AtomicBoolean running;
    private final AtomicLong processedCount;
    private final AtomicLong errorCount;
    
    @Inject
    public AsyncProcessor(RequestChannel requestChannel, ResponseChannel responseChannel,
                         MessageService messageService, HealthService healthService, 
                         ExecutorService executorService) {
        this.requestChannel = requestChannel;
        this.responseChannel = responseChannel;
        this.messageService = messageService;
        this.healthService = healthService;
        this.ioThreadPool = executorService;
        
        this.running = new AtomicBoolean(false);
        this.processedCount = new AtomicLong(0);
        this.errorCount = new AtomicLong(0);
        
        logger.info("AsyncProcessor initialized with injected dependencies");
    }
    
    /**
     * Starts the async processor.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting AsyncProcessor...");
            
            // Start I/O worker
            ioThreadPool.submit(new IoWorker());
            
            logger.info("AsyncProcessor started successfully");
        } else {
            logger.warn("AsyncProcessor is already running");
        }
    }
    
    /**
     * Stops the async processor.
     */
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            logger.info("Shutting down AsyncProcessor...");
            ioThreadPool.shutdown();
            logger.info("AsyncProcessor shutdown completed");
        } else {
            logger.warn("AsyncProcessor is not running");
        }
    }
    
    /**
     * I/O worker thread that processes requests from the queue.
     */
    private class IoWorker implements Runnable {
        @Override
        public void run() {
            logger.info("I/O worker thread started: {}", Thread.currentThread().getName());
            
            while (running.get()) {
                try {
                    AsyncRequest request = requestChannel.takeRequest();
                    processRequest(request);
                } catch (InterruptedException e) {
                    logger.info("I/O worker thread interrupted: {}", Thread.currentThread().getName());
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in I/O worker thread", e);
                    errorCount.incrementAndGet();
                }
            }
            
            logger.info("I/O worker thread stopped: {}", Thread.currentThread().getName());
        }
    }
    
    /**
     * Processes a single request.
     */
    private void processRequest(AsyncRequest request) {
        try {
            logger.debug("Processing request: {}", request.getRequestId());
            
            switch (request.getType()) {
                case PUBLISH:
                    handlePublishRequest(request);
                    break;
                case CONSUME:
                    handleConsumeRequest(request);
                    break;
                case HEALTH:
                    handleHealthRequest(request);
                    break;
                case TOPICS:
                    handleTopicsRequest(request);
                    break;
                case METRICS:
                    handleMetricsRequest(request);
                    break;
                default:
                    logger.warn("Unknown request type: {}", request.getType());
                    sendErrorResponse(request, 400, "Unknown request type");
            }
            
            requestChannel.markCompleted(request);
            processedCount.incrementAndGet();
            
        } catch (Exception e) {
            logger.error("Error processing request: {}", request.getRequestId(), e);
            errorCount.incrementAndGet();
            sendErrorResponse(request, 500, "Internal server error: " + e.getMessage());
        }
    }
    
    /**
     * Handles publish requests.
     */
    private void handlePublishRequest(AsyncRequest request) throws IOException {
        String topicName = request.getTopicName();
        String message = request.getMessage();
        
        if (topicName == null || message == null) {
            sendErrorResponse(request, 400, "Missing topic name or message");
            return;
        }
        
        logger.debug("Publishing message to topic: {}", topicName);
        Record record = messageService.publishMessage(topicName, message.getBytes(StandardCharsets.UTF_8));
        
        // Send success response
        String responseBody = String.format("{\"offset\":%d,\"timestamp\":%d,\"message\":\"Message published successfully\"}", 
                                      record.getOffset(), record.getTimestamp());
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    /**
     * Handles consume requests.
     */
    private void handleConsumeRequest(AsyncRequest request) throws IOException {
        String topicName = request.getTopicName();
        Long offset = request.getOffset();
        
        if (topicName == null || offset == null) {
            sendErrorResponse(request, 400, "Missing topic name or offset");
            return;
        }
        
        logger.debug("Consuming message from topic: {} at offset: {}", topicName, offset);
        Record record = messageService.consumeMessage(topicName, offset);
        
        if (record == null) {
            sendErrorResponse(request, 404, "No message found at offset " + offset);
            return;
        }
        
        // Send success response
        String message = new String(record.getData(), StandardCharsets.UTF_8);
        String responseBody = String.format("{\"offset\":%d,\"timestamp\":%d,\"message\":\"%s\"}", 
                                      record.getOffset(), record.getTimestamp(), message);
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    /**
     * Handles health check requests.
     */
    private void handleHealthRequest(AsyncRequest request) throws IOException {
        HealthService.HealthStatus health = healthService.checkHealth();
        String responseBody = String.format("{\"status\":\"%s\",\"message\":\"%s\"}", 
                                      health.getStatus(), health.getMessage());
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    /**
     * Handles topics list requests.
     */
    private void handleTopicsRequest(AsyncRequest request) throws IOException {
        // For now, return empty topics list
        // TODO: Implement actual topics listing
        String responseBody = "{\"topics\":[]}";
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    /**
     * Handles metrics requests.
     */
    private void handleMetricsRequest(AsyncRequest request) throws IOException {
        String responseBody = String.format("{\"producerMessages\":%d,\"consumerMessages\":%d,\"processedRequests\":%d,\"errorCount\":%d}",
                                      messageService.getProducerMessageCount(),
                                      messageService.getConsumerMessageCount(),
                                      processedCount.get(), errorCount.get());
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    /**
     * Sends a successful response via ResponseChannel.
     */
    private void sendResponse(AsyncRequest request, int statusCode, String contentType, String body) {
        AsyncResponse response = new AsyncResponse(request.getRequestId(), request.getResponse(), 
                                                 statusCode, contentType, body);
        if (responseChannel.addResponse(response)) {
            logger.debug("Response queued for request: {}", request.getRequestId());
        } else {
            logger.warn("Response queue full, dropping response for request: {}", request.getRequestId());
            // Fallback: send directly if queue is full
            sendResponseDirectly(request, statusCode, contentType, body);
        }
    }
    
    /**
     * Sends an error response via ResponseChannel.
     */
    private void sendErrorResponse(AsyncRequest request, int statusCode, String message) {
        String errorBody = String.format("{\"error\":\"%s\",\"code\":%d}", message, statusCode);
        AsyncResponse response = new AsyncResponse(request.getRequestId(), request.getResponse(), 
                                                 statusCode, "application/json", errorBody);
        if (responseChannel.addResponse(response)) {
            logger.debug("Error response queued for request: {}", request.getRequestId());
        } else {
            logger.warn("Response queue full, dropping error response for request: {}", request.getRequestId());
            // Fallback: send directly if queue is full
            sendResponseDirectly(request, statusCode, "application/json", errorBody);
        }
    }

    /**
     * Fallback method to send response directly when ResponseChannel is full.
     */
    private void sendResponseDirectly(AsyncRequest request, int statusCode, String contentType, String body) {
        try {
            request.getResponse().setStatus(statusCode);
            request.getResponse().setContentType(contentType);
            request.getResponse().getWriter().print(body);
            request.getAsyncContext().complete();
            logger.debug("Response sent directly for request: {}", request.getRequestId());
        } catch (Exception e) {
            logger.error("Error sending response directly for request: {}", request.getRequestId(), e);
        }
    }
    
    // Metrics getters
    public long getProcessedCount() {
        return processedCount.get();
    }
    
    public long getErrorCount() {
        return errorCount.get();
    }
    
    public boolean isRunning() {
        return running.get();
    }
}
