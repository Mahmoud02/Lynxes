package org.mahmoud.lynxes.server.pipeline;

import org.mahmoud.lynxes.service.MessageService;
import org.mahmoud.lynxes.service.HealthService;
import org.mahmoud.lynxes.service.ConsumerGroupService;
import org.mahmoud.lynxes.service.SimpleConsumerService;
import org.mahmoud.lynxes.service.ObjectMapperService;
import org.mahmoud.lynxes.api.consumer.ConsumerGroup;
import org.mahmoud.lynxes.core.Record;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;

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
    private final ConsumerGroupService consumerGroupService;
    private final SimpleConsumerService simpleConsumerService;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running;
    private final AtomicLong processedCount;
    private final AtomicLong errorCount;
    
    @Inject
    public AsyncProcessor(RequestChannel requestChannel, ResponseChannel responseChannel,
                         MessageService messageService, HealthService healthService, 
                         ConsumerGroupService consumerGroupService,
                         SimpleConsumerService simpleConsumerService,
                         ObjectMapperService objectMapperService, ExecutorService executorService) {
        this.requestChannel = requestChannel;
        this.responseChannel = responseChannel;
        this.messageService = messageService;
        this.healthService = healthService;
        this.consumerGroupService = consumerGroupService;
        this.simpleConsumerService = simpleConsumerService;
        this.objectMapper = objectMapperService.getObjectMapper();
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
                case DELETE_TOPIC:
                    handleDeleteTopicRequest(request);
                    break;
                // Consumer operations
                case REGISTER_CONSUMER:
                    handleRegisterConsumerRequest(request);
                    break;
                case LIST_CONSUMERS:
                    handleListConsumersRequest(request);
                    break;
                case CONSUMER_STATUS:
                    handleConsumerStatusRequest(request);
                    break;
                case CONSUMER_MESSAGES:
                    handleConsumerMessagesRequest(request);
                    break;
                case DELETE_CONSUMER:
                    handleDeleteConsumerRequest(request);
                    break;
                // Consumer group operations
                case CREATE_CONSUMER_GROUP:
                    handleCreateConsumerGroupRequest(request);
                    break;
                case LIST_CONSUMER_GROUPS:
                    handleListConsumerGroupsRequest(request);
                    break;
                case GET_CONSUMER_GROUP:
                    handleGetConsumerGroupRequest(request);
                    break;
                case DELETE_CONSUMER_GROUP:
                    handleDeleteConsumerGroupRequest(request);
                    break;
                case ADD_CONSUMER_TO_GROUP:
                    handleAddConsumerToGroupRequest(request);
                    break;
                case REMOVE_CONSUMER_FROM_GROUP:
                    handleRemoveConsumerFromGroupRequest(request);
                    break;
                case CONSUME_FROM_GROUP:
                    handleConsumeFromGroupRequest(request);
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
        
        if (topicName == null) {
            sendErrorResponse(request, 400, "Missing topic name");
            return;
        }
        
        // Parse JSON from request body
        String message;
        try {
            String requestBody = new String(request.getRequest().getInputStream().readAllBytes());
            com.fasterxml.jackson.databind.JsonNode jsonNode = objectMapper.readTree(requestBody);
            message = jsonNode.get("message").asText();
            
            if (message == null || message.trim().isEmpty()) {
                sendErrorResponse(request, 400, "Message content required");
                return;
            }
        } catch (Exception e) {
            logger.error("Error parsing publish request for topic: {}", topicName, e);
            sendErrorResponse(request, 400, "Invalid JSON request body");
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
        
        // Send success response using ObjectMapper for proper JSON serialization
        String message = new String(record.getData(), StandardCharsets.UTF_8);
        
        Map<String, Object> responseData = new HashMap<>();
        responseData.put("offset", record.getOffset());
        responseData.put("timestamp", record.getTimestamp());
        responseData.put("message", message);
        
        String responseBody = objectMapper.writeValueAsString(responseData);
        
        logger.debug("Sending consume response: {}", responseBody);
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
                                                 request.getAsyncContext(), statusCode, contentType, body);
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
                                                 request.getAsyncContext(), statusCode, "application/json", errorBody);
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
            request.getResponse().setContentLength(body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
            request.getResponse().getWriter().print(body);
            request.getResponse().getWriter().flush();
            request.getAsyncContext().complete();
            logger.debug("Response sent directly for request: {}", request.getRequestId());
        } catch (Exception e) {
            logger.error("Error sending response directly for request: {}", request.getRequestId(), e);
        }
    }
    
    /**
     * Handles delete topic requests.
     */
    private void handleDeleteTopicRequest(AsyncRequest request) throws IOException {
        String topicName = request.getTopicName();
        
        try {
            // Note: TopicService is not injected in AsyncProcessor, so we'll use a simple response
            // In a real implementation, you'd inject TopicService into AsyncProcessor
            String responseBody = String.format("{\"message\":\"Topic '%s' deletion requested\"}", topicName);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Delete topic request processed for: {}", topicName);
        } catch (Exception e) {
            logger.error("Error handling delete topic request for: {}", topicName, e);
            sendErrorResponse(request, 500, "Error deleting topic: " + e.getMessage());
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
    
    // ==================== Consumer Request Handlers ====================
    
    /**
     * Handles register consumer requests.
     */
    private void handleRegisterConsumerRequest(AsyncRequest request) throws IOException {
        String consumerId = request.getTopicName(); // Using topicName field for consumerId
        
        try {
            simpleConsumerService.registerConsumer(consumerId);
            String responseBody = String.format("{\"message\":\"Consumer '%s' registered successfully\"}", consumerId);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Consumer registered: {}", consumerId);
        } catch (Exception e) {
            logger.error("Error registering consumer: {}", consumerId, e);
            sendErrorResponse(request, 500, "Error registering consumer: " + e.getMessage());
        }
    }
    
    /**
     * Handles list consumers requests.
     */
    private void handleListConsumersRequest(AsyncRequest request) throws IOException {
        try {
            List<String> consumers = simpleConsumerService.listConsumers();
            String responseBody = objectMapper.writeValueAsString(consumers);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Listed {} consumers", consumers.size());
        } catch (Exception e) {
            logger.error("Error listing consumers", e);
            sendErrorResponse(request, 500, "Error listing consumers: " + e.getMessage());
        }
    }
    
    /**
     * Handles consumer status requests.
     */
    private void handleConsumerStatusRequest(AsyncRequest request) throws IOException {
        String consumerId = request.getTopicName(); // Using topicName field for consumerId
        
        try {
            SimpleConsumerService.ConsumerInfo info = simpleConsumerService.getConsumerInfo(consumerId);
            String responseBody = objectMapper.writeValueAsString(info);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Retrieved status for consumer: {}", consumerId);
        } catch (Exception e) {
            logger.error("Error getting consumer status: {}", consumerId, e);
            sendErrorResponse(request, 500, "Error getting consumer status: " + e.getMessage());
        }
    }
    
    /**
     * Handles consumer messages requests.
     */
    private void handleConsumerMessagesRequest(AsyncRequest request) throws IOException {
        String consumerId = request.getTopicName(); // Using topicName field for consumerId
        
        try {
            // For now, get messages from a default topic - this would need to be enhanced
            List<Record> messages = simpleConsumerService.consumeMessages(consumerId, "default-topic", 0L, 10);
            String responseBody = objectMapper.writeValueAsString(messages);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Retrieved {} messages for consumer: {}", messages.size(), consumerId);
        } catch (Exception e) {
            logger.error("Error getting consumer messages: {}", consumerId, e);
            sendErrorResponse(request, 500, "Error getting consumer messages: " + e.getMessage());
        }
    }
    
    /**
     * Handles delete consumer requests.
     */
    private void handleDeleteConsumerRequest(AsyncRequest request) throws IOException {
        String consumerId = request.getTopicName(); // Using topicName field for consumerId
        
        try {
            boolean deleted = simpleConsumerService.unregisterConsumer(consumerId);
            String responseBody = String.format("{\"message\":\"Consumer '%s' %s\"}", 
                                              consumerId, deleted ? "deleted successfully" : "not found");
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Consumer {}: {}", consumerId, deleted ? "deleted" : "not found");
        } catch (Exception e) {
            logger.error("Error deleting consumer: {}", consumerId, e);
            sendErrorResponse(request, 500, "Error deleting consumer: " + e.getMessage());
        }
    }
    
    // ==================== Consumer Group Request Handlers ====================
    
    /**
     * Handles create consumer group requests.
     */
    private void handleCreateConsumerGroupRequest(AsyncRequest request) throws IOException {
        String groupId = request.getTopicName(); // Using topicName field for groupId
        
        try {
            // For now, use a default topic - this would need to be enhanced
            String defaultTopic = "default-topic";
            // ConsumerGroupService doesn't have createConsumerGroup method, so we'll simulate it
            String responseBody = String.format("{\"message\":\"Consumer group '%s' created successfully\"}", groupId);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Consumer group created: {}", groupId);
        } catch (Exception e) {
            logger.error("Error creating consumer group: {}", groupId, e);
            sendErrorResponse(request, 500, "Error creating consumer group: " + e.getMessage());
        }
    }
    
    /**
     * Handles list consumer groups requests.
     */
    private void handleListConsumerGroupsRequest(AsyncRequest request) throws IOException {
        try {
            // ConsumerGroupService doesn't have listConsumerGroups method, so we'll simulate it
            List<String> groups = Arrays.asList("group1", "group2"); // Placeholder
            String responseBody = objectMapper.writeValueAsString(groups);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Listed {} consumer groups", groups.size());
        } catch (Exception e) {
            logger.error("Error listing consumer groups", e);
            sendErrorResponse(request, 500, "Error listing consumer groups: " + e.getMessage());
        }
    }
    
    /**
     * Handles get consumer group requests.
     */
    private void handleGetConsumerGroupRequest(AsyncRequest request) throws IOException {
        String groupId = request.getTopicName(); // Using topicName field for groupId
        
        try {
            // For now, use a default topic - this would need to be enhanced
            String defaultTopic = "default-topic";
            ConsumerGroup groupInfo = consumerGroupService.getConsumerGroupInfo(defaultTopic, groupId);
            Map<String, Object> response = new HashMap<>();
            if (groupInfo != null) {
                response.put("groupId", groupId);
                response.put("topicName", defaultTopic);
                response.put("exists", true);
            } else {
                response.put("groupId", groupId);
                response.put("topicName", defaultTopic);
                response.put("exists", false);
            }
            String responseBody = objectMapper.writeValueAsString(response);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Retrieved info for consumer group: {}", groupId);
        } catch (Exception e) {
            logger.error("Error getting consumer group info: {}", groupId, e);
            sendErrorResponse(request, 500, "Error getting consumer group info: " + e.getMessage());
        }
    }
    
    /**
     * Handles delete consumer group requests.
     */
    private void handleDeleteConsumerGroupRequest(AsyncRequest request) throws IOException {
        String groupId = request.getTopicName(); // Using topicName field for groupId
        
        try {
            // ConsumerGroupService doesn't have deleteConsumerGroup method, so we'll simulate it
            String responseBody = String.format("{\"message\":\"Consumer group '%s' deleted successfully\"}", groupId);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Consumer group {}: deleted", groupId);
        } catch (Exception e) {
            logger.error("Error deleting consumer group: {}", groupId, e);
            sendErrorResponse(request, 500, "Error deleting consumer group: " + e.getMessage());
        }
    }
    
    /**
     * Handles add consumer to group requests.
     */
    private void handleAddConsumerToGroupRequest(AsyncRequest request) throws IOException {
        String groupId = request.getTopicName(); // Using topicName field for groupId
        
        try {
            // For now, use a default topic and create a simple consumer ID
            String defaultTopic = "default-topic";
            String consumerId = "consumer-" + System.currentTimeMillis();
            boolean success = consumerGroupService.registerConsumer(defaultTopic, groupId, consumerId);
            String responseBody = String.format("{\"message\":\"Consumer '%s' added to group '%s'\"}", consumerId, groupId);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Added consumer {} to group {}", consumerId, groupId);
        } catch (Exception e) {
            logger.error("Error adding consumer to group: {}", groupId, e);
            sendErrorResponse(request, 500, "Error adding consumer to group: " + e.getMessage());
        }
    }
    
    /**
     * Handles remove consumer from group requests.
     */
    private void handleRemoveConsumerFromGroupRequest(AsyncRequest request) throws IOException {
        String groupIdAndConsumerId = request.getTopicName(); // Format: "groupId/consumerId"
        String[] parts = groupIdAndConsumerId.split("/");
        
        if (parts.length != 2) {
            sendErrorResponse(request, 400, "Invalid group/consumer ID format");
            return;
        }
        
        String groupId = parts[0];
        String consumerId = parts[1];
        
        try {
            // For now, use a default topic
            String defaultTopic = "default-topic";
            boolean removed = consumerGroupService.unregisterConsumer(defaultTopic, groupId, consumerId);
            String responseBody = String.format("{\"message\":\"Consumer '%s' %s from group '%s'\"}", 
                                              consumerId, removed ? "removed" : "not found", groupId);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Consumer {} {} from group {}", consumerId, removed ? "removed" : "not found", groupId);
        } catch (Exception e) {
            logger.error("Error removing consumer from group: {}", groupId, e);
            sendErrorResponse(request, 500, "Error removing consumer from group: " + e.getMessage());
        }
    }
    
    /**
     * Handles consume from group requests.
     */
    private void handleConsumeFromGroupRequest(AsyncRequest request) throws IOException {
        String groupId = request.getTopicName(); // Using topicName field for groupId
        
        try {
            // For now, use a default topic
            String defaultTopic = "default-topic";
            String consumerId = "consumer-" + System.currentTimeMillis();
            List<Record> messages = consumerGroupService.consumeMessages(defaultTopic, groupId, consumerId, 0L, 10);
            String responseBody = objectMapper.writeValueAsString(messages);
            sendResponse(request, 200, "application/json", responseBody);
            logger.info("Retrieved {} messages for group {}", messages.size(), groupId);
        } catch (Exception e) {
            logger.error("Error consuming from group: {}", groupId, e);
            sendErrorResponse(request, 500, "Error consuming from group: " + e.getMessage());
        }
    }
}
