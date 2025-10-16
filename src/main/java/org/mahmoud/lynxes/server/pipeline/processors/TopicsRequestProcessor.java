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
import java.util.stream.Collectors;

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
            
            // Convert TopicInfo objects to simple topic names for the API response
            List<String> topicNames = topics.stream()
                .map(TopicService.TopicInfo::getName)
                .collect(Collectors.toList());
            
            String responseBody = String.format("{\"topics\":%s}", 
                topicNames.stream()
                    .map(name -> "\"" + name + "\"")
                    .collect(Collectors.joining(",", "[", "]")));
            
            logger.debug("Found {} topics: {}", topicNames.size(), topicNames);
            ResponseUtils.sendSuccessResponse(request, responseBody);
            
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
