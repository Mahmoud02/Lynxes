package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequestKeys;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import org.mahmoud.lynxes.service.MessageService;
import org.mahmoud.lynxes.core.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Processor for consume requests.
 * Handles CONSUME request type.
 */
public class ConsumeRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeRequestProcessor.class);
    
    private final MessageService messageService;
    private final ObjectMapper objectMapper;
    
    @Inject
    public ConsumeRequestProcessor(MessageService messageService, ObjectMapper objectMapper) {
        this.messageService = messageService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        Optional<String> topicNameOpt = request.getString(AsyncRequestKeys.TOPIC_NAME);
        Optional<Long> offsetOpt = request.getLong(AsyncRequestKeys.OFFSET);
        
        if (topicNameOpt.isEmpty()) {
            sendErrorResponse(request, 400, "Missing topic name");
            return;
        }
        
        String topicName = topicNameOpt.get();
        Long offset = offsetOpt.orElse(0L);
        
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
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.CONSUME;
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
