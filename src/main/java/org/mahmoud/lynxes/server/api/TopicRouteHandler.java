package org.mahmoud.lynxes.server.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.slf4j.LoggerFactory;

/**
 * Route handler for individual topic operations.
 * Handles GET, POST, and DELETE requests to /topics/{name}.
 * GET: Consume messages (with offset parameter)
 * POST: Publish messages
 * DELETE: Delete topic
 * Follows the async architecture pattern where servlets are thin wrappers that delegate to the processing pipeline.
 */
public class TopicRouteHandler extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(TopicRouteHandler.class);
    
    /**
     * Constructs a TopicRouteHandler.
     * No dependencies needed since this only handles routing.
     */
    public TopicRouteHandler() {
        // No dependencies - this servlet only parses URLs and queues requests
    }
    
    @Override
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                               AsyncRequest.RequestType defaultType) throws ServletException {
        String pathInfo = request.getPathInfo();
        if (pathInfo == null || pathInfo.equals("/")) {
            sendErrorResponse(response, 400, "Topic name required");
            return;
        }
        
        String topicName = pathInfo.substring(1); // Remove leading slash
        String method = request.getMethod();
        
        logger.debug("Processing {} request for topic: {}", method, topicName);
        
        switch (method) {
            case "GET":
                handleConsumeRequest(request, response, topicName);
                break;
            case "POST":
                handlePublishRequest(request, response, topicName);
                break;
            case "DELETE":
                handleDeleteRequest(request, response, topicName);
                break;
            default:
                sendErrorResponse(response, 405, "Method not allowed: " + method);
        }
    }
    
    /**
     * Handles GET requests for consuming messages.
     */
    private void handleConsumeRequest(HttpServletRequest request, HttpServletResponse response, String topicName) {
        String offsetParam = request.getParameter("offset");
        if (offsetParam == null) {
            sendErrorResponse(response, 400, "Offset parameter required");
            return;
        }
        
        try {
            Long offset = Long.parseLong(offsetParam);
            processAsyncRequest(request, response, AsyncRequest.RequestType.CONSUME, topicName, offset, null);
        } catch (NumberFormatException e) {
            sendErrorResponse(response, 400, "Invalid offset parameter");
        }
    }
    
    /**
     * Handles POST requests for publishing messages.
     * Note: JSON parsing is now handled by AsyncRequestProcessorOrchestrator to maintain separation of concerns.
     */
    private void handlePublishRequest(HttpServletRequest request, HttpServletResponse response, String topicName) {
        // Pass the raw request to AsyncRequestProcessorOrchestrator for JSON parsing
        // This maintains separation of concerns - route handler only handles routing
        processAsyncRequest(request, response, AsyncRequest.RequestType.PUBLISH, topicName, null, null);
    }
    
    /**
     * Handles DELETE requests for deleting topics.
     */
    private void handleDeleteRequest(HttpServletRequest request, HttpServletResponse response, String topicName) {
        processAsyncRequest(request, response, AsyncRequest.RequestType.DELETE_TOPIC, topicName, null, null);
    }
}
