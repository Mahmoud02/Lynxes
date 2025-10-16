package org.mahmoud.lynxes.server.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Route handler for topics list endpoint.
 * Handles GET requests to /topics and processes them asynchronously.
 * Follows the async architecture pattern where servlets are thin wrappers that delegate to the processing pipeline.
 */
public class TopicsRouteHandler extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(TopicsRouteHandler.class);
    
    /**
     * Constructs a TopicsRouteHandler.
     * No dependencies needed since this only handles routing.
     */
    public TopicsRouteHandler() {
        // No dependencies - this servlet only parses URLs and queues requests
    }
    
    @Override
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                               AsyncRequest.RequestType defaultType) throws ServletException {
        logger.debug("Processing topics list request");
        processAsyncRequest(request, response, AsyncRequest.RequestType.TOPICS, null, null, null);
    }
}
