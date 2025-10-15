package org.mahmoud.lynxes.server.api;

import org.mahmoud.lynxes.service.TopicService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Route handler for topics list endpoint.
 * Handles GET requests to /topics and processes them asynchronously.
 * Follows the async architecture pattern where servlets are thin wrappers that delegate to the processing pipeline.
 */
public class TopicsRouteHandler extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(TopicsRouteHandler.class);
    
    private final TopicService topicService;
    private final ObjectMapper objectMapper;
    
    /**
     * Constructs a TopicsRouteHandler with the required services.
     * 
     * @param topicService The topic service for listing topics
     * @param objectMapper The JSON object mapper
     */
    public TopicsRouteHandler(TopicService topicService, ObjectMapper objectMapper) {
        this.topicService = topicService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                               AsyncRequest.RequestType defaultType) throws ServletException {
        logger.debug("Processing topics list request");
        processAsyncRequest(request, response, AsyncRequest.RequestType.TOPICS, null, null, null);
    }
}
