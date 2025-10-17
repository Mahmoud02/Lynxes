package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequestKeys;
import org.mahmoud.lynxes.server.pipeline.core.ResponseUtils;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import org.mahmoud.lynxes.service.TopicService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * Processor for delete topic requests.
 * Handles DELETE_TOPIC request type.
 */
public class DeleteTopicRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DeleteTopicRequestProcessor.class);
    
    private final TopicService topicService;
    
    @Inject
    public DeleteTopicRequestProcessor(TopicService topicService) {
        this.topicService = topicService;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing delete topic request: {}", request.getRequestId());
        
        Optional<String> topicNameOpt = request.getString(AsyncRequestKeys.TOPIC_NAME);
        
        if (topicNameOpt.isEmpty()) {
            ResponseUtils.sendBadRequestError(request, "Missing topic name");
            return;
        }
        
        String topicName = topicNameOpt.get();
        
        try {
            boolean deleted = topicService.deleteTopic(topicName);
            
            if (deleted) {
                String responseBody = String.format("{\"message\":\"Topic '%s' deleted successfully\"}", topicName);
                ResponseUtils.sendSuccessResponse(request, responseBody);
                logger.info("Successfully deleted topic: {}", topicName);
            } else {
                ResponseUtils.sendNotFoundError(request, "Topic not found: " + topicName);
                logger.warn("Attempted to delete non-existent topic: {}", topicName);
            }
            
        } catch (Exception e) {
            logger.error("Error deleting topic: {}", topicName, e);
            ResponseUtils.sendInternalServerError(request, "Failed to delete topic: " + e.getMessage());
        }
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.DELETE_TOPIC;
    }
}
