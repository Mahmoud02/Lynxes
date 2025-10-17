package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.ResponseUtils;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import org.mahmoud.lynxes.service.TopicService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Processor for delete all topics requests.
 * Handles DELETE_ALL_TOPICS request type.
 */
public class DeleteAllTopicsRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DeleteAllTopicsRequestProcessor.class);
    
    private final TopicService topicService;
    
    @Inject
    public DeleteAllTopicsRequestProcessor(TopicService topicService) {
        this.topicService = topicService;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing delete all topics request: {}", request.getRequestId());
        
        try {
            // Get list of all topics
            List<TopicService.TopicInfo> topics = topicService.listTopics();
            
            if (topics.isEmpty()) {
                String responseBody = "{\"message\":\"No topics found to delete\"}";
                ResponseUtils.sendSuccessResponse(request, responseBody);
                logger.info("No topics found to delete");
                return;
            }
            
            int deletedCount = 0;
            int errorCount = 0;
            
            // Delete each topic
            for (TopicService.TopicInfo topicInfo : topics) {
                try {
                    boolean deleted = topicService.deleteTopic(topicInfo.getName());
                    if (deleted) {
                        deletedCount++;
                        logger.debug("Successfully deleted topic: {}", topicInfo.getName());
                    } else {
                        errorCount++;
                        logger.warn("Failed to delete topic: {}", topicInfo.getName());
                    }
                } catch (Exception e) {
                    errorCount++;
                    logger.error("Error deleting topic: {}", topicInfo.getName(), e);
                }
            }
            
            // Send response
            String responseBody;
            if (errorCount == 0) {
                responseBody = String.format("{\"message\":\"Successfully deleted %d topics\",\"deletedCount\":%d}", 
                                            deletedCount, deletedCount);
                logger.info("Successfully deleted {} topics", deletedCount);
            } else {
                responseBody = String.format("{\"message\":\"Deleted %d topics with %d errors\",\"deletedCount\":%d,\"errorCount\":%d}", 
                                            deletedCount, errorCount, deletedCount, errorCount);
                logger.warn("Deleted {} topics with {} errors", deletedCount, errorCount);
            }
            
            ResponseUtils.sendSuccessResponse(request, responseBody);
            
        } catch (Exception e) {
            logger.error("Error deleting all topics", e);
            ResponseUtils.sendInternalServerError(request, "Failed to delete all topics: " + e.getMessage());
        }
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.DELETE_ALL_TOPICS;
    }
}
