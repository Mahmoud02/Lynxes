package org.mahmoud.lynxes.server.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.slf4j.LoggerFactory;

/**
 * Servlet for individual topic operations.
 * Handles GET, POST, and DELETE requests to /topics/{name}.
 * GET: Consume messages (with offset parameter)
 * POST: Publish messages
 * DELETE: Delete topic
 */
public class TopicServlet extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(TopicServlet.class);
    
    private final ObjectMapper objectMapper;
    
    /**
     * Constructs a TopicServlet with the required services.
     * 
     * @param objectMapper The JSON object mapper
     */
    public TopicServlet(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
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
     */
    private void handlePublishRequest(HttpServletRequest request, HttpServletResponse response, String topicName) {
        try {
            String requestBody = new String(request.getInputStream().readAllBytes());
            com.fasterxml.jackson.databind.JsonNode jsonNode = objectMapper.readTree(requestBody);
            String message = jsonNode.get("message").asText();
            
            if (message == null || message.trim().isEmpty()) {
                sendErrorResponse(response, 400, "Message content required");
                return;
            }
            
            processAsyncRequest(request, response, AsyncRequest.RequestType.PUBLISH, topicName, null, message);
        } catch (Exception e) {
            logger.error("Error parsing publish request for topic: {}", topicName, e);
            sendErrorResponse(response, 400, "Invalid JSON request body");
        }
    }
    
    /**
     * Handles DELETE requests for deleting topics.
     */
    private void handleDeleteRequest(HttpServletRequest request, HttpServletResponse response, String topicName) {
        processAsyncRequest(request, response, AsyncRequest.RequestType.DELETE_TOPIC, topicName, null, null);
    }
}
