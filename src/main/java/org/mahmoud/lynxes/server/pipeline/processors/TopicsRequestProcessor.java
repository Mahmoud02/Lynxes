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
 * Processor for topics list requests.
 * Handles TOPICS request type.
 */
public class TopicsRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TopicsRequestProcessor.class);
    
    private final TopicService topicService;
    
    @Inject
    public TopicsRequestProcessor(TopicService topicService) {
        this.topicService = topicService;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing topics list request: {}", request.getRequestId());
        
        try {
            List<TopicService.TopicInfo> topics = topicService.listTopics();
            
            // Convert TopicInfo objects to JSON format for the API response
            StringBuilder responseBuilder = new StringBuilder();
            responseBuilder.append("{\"topics\":[");
            
            for (int i = 0; i < topics.size(); i++) {
                TopicService.TopicInfo topic = topics.get(i);
                if (i > 0) responseBuilder.append(",");
                
                responseBuilder.append("{");
                responseBuilder.append("\"name\":\"").append(topic.getName()).append("\",");
                responseBuilder.append("\"size\":").append(topic.getSizeBytes()).append(",");
                responseBuilder.append("\"createdAt\":\"").append(topic.getCreatedAt().toString()).append("\",");
                responseBuilder.append("\"nextOffset\":").append(topic.getNextOffset());
                responseBuilder.append("}");
            }
            
            responseBuilder.append("]}");
            
            logger.debug("Found {} topics with full metadata", topics.size());
            ResponseUtils.sendSuccessResponse(request, responseBuilder.toString());
            
        } catch (Exception e) {
            logger.error("Error listing topics: {}", e.getMessage(), e);
            ResponseUtils.sendInternalServerError(request, "Failed to list topics: " + e.getMessage());
        }
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.TOPICS;
    }
}
