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
 * Servlet for topics list endpoint.
 * Handles GET requests to /topics and processes them asynchronously.
 */
public class TopicsServlet extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(TopicsServlet.class);
    
    private final TopicService topicService;
    private final ObjectMapper objectMapper;
    
    /**
     * Constructs a TopicsServlet with the required services.
     * 
     * @param topicService The topic service for listing topics
     * @param objectMapper The JSON object mapper
     */
    public TopicsServlet(TopicService topicService, ObjectMapper objectMapper) {
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
