package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.ResponseUtils;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import org.mahmoud.lynxes.service.TopicService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Processor for topic creation requests.
 * Handles CREATE_TOPIC request type.
 */
public class CreateTopicRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CreateTopicRequestProcessor.class);
    
    private final TopicService topicService;
    private final ObjectMapper objectMapper;
    
    @Inject
    public CreateTopicRequestProcessor(TopicService topicService, ObjectMapper objectMapper) {
        this.topicService = topicService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing create topic request: {}", request.getRequestId());
        
        try {
            // Parse JSON from request body
            String requestBody = new String(request.getRequest().getInputStream().readAllBytes());
            com.fasterxml.jackson.databind.JsonNode jsonNode = objectMapper.readTree(requestBody);
            
            String topicName = jsonNode.get("name").asText();
            
            if (topicName == null || topicName.trim().isEmpty()) {
                ResponseUtils.sendBadRequestError(request, "Topic name is required");
                return;
            }
            
            logger.debug("Creating topic: {}", topicName);
            
            // Create the topic using TopicService
            TopicService.TopicInfo topicInfo = topicService.createTopic(topicName);
            
            // Send success response with topic information
            String responseBody = String.format(
                "{\"name\":\"%s\",\"messageCount\":%d,\"createdAt\":\"%s\",\"nextOffset\":%d,\"sizeBytes\":%d,\"message\":\"Topic created successfully\"}",
                topicInfo.getName(),
                topicInfo.getMessageCount(),
                topicInfo.getCreatedAt().toString(),
                topicInfo.getNextOffset(),
                topicInfo.getSizeBytes()
            );
            
            ResponseUtils.sendSuccessResponse(request, responseBody);
            logger.debug("Create topic request {} processed successfully for topic: {}", request.getRequestId(), topicName);
            
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid topic creation request: {}", e.getMessage());
            ResponseUtils.sendBadRequestError(request, e.getMessage());
        } catch (Exception e) {
            logger.error("Error creating topic: {}", e.getMessage(), e);
            ResponseUtils.sendInternalServerError(request, "Failed to create topic: " + e.getMessage());
        }
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.CREATE_TOPIC;
    }
}
