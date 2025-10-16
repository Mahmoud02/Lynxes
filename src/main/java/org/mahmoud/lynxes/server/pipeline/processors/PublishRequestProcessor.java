package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.AsyncRequestKeys;
import org.mahmoud.lynxes.server.pipeline.RequestProcessor;
import org.mahmoud.lynxes.service.MessageService;
import org.mahmoud.lynxes.core.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Processor for publish requests.
 * Handles PUBLISH request type.
 */
public class PublishRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(PublishRequestProcessor.class);
    
    private final MessageService messageService;
    private final ObjectMapper objectMapper;
    
    @Inject
    public PublishRequestProcessor(MessageService messageService, ObjectMapper objectMapper) {
        this.messageService = messageService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        Optional<String> topicNameOpt = request.getString(AsyncRequestKeys.TOPIC_NAME);
        
        if (topicNameOpt.isEmpty()) {
            sendErrorResponse(request, 400, "Missing topic name");
            return;
        }
        
        String topicName = topicNameOpt.get();
        
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
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.PUBLISH;
    }
    
    /**
     * Sends a response back to the client.
     */
    private void sendResponse(AsyncRequest request, int statusCode, String contentType, String body) throws IOException {
        request.getResponse().setStatus(statusCode);
        request.getResponse().setContentType(contentType);
        request.getResponse().getWriter().write(body);
        request.getAsyncContext().complete();
    }
    
    /**
     * Sends an error response back to the client.
     */
    private void sendErrorResponse(AsyncRequest request, int statusCode, String message) throws IOException {
        String errorBody = String.format("{\"error\":\"%s\"}", message);
        sendResponse(request, statusCode, "application/json", errorBody);
    }
}
