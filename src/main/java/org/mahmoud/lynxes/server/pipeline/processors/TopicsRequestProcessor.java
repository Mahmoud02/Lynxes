package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.ResponseUtils;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Processor for topics list requests.
 * Handles TOPICS request type.
 */
public class TopicsRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TopicsRequestProcessor.class);
    
    @Inject
    public TopicsRequestProcessor() {
        // No dependencies needed for now
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing topics list request: {}", request.getRequestId());
        
        // For now, return empty topics list
        // TODO: Implement actual topics listing
        String responseBody = "{\"topics\":[]}";
        
        ResponseUtils.sendSuccessResponse(request, responseBody);
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.TOPICS;
    }
}
