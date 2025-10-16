package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.RequestProcessor;
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
        
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.TOPICS;
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
}
