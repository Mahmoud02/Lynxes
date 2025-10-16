package org.mahmoud.lynxes.server.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.AsyncRequestKeys;

import java.io.IOException;
import jakarta.servlet.AsyncContext;
import org.mahmoud.lynxes.server.pipeline.RequestChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Route handler for consumer group operations.
 * This class only parses URLs and queues requests - all business logic is handled by AsyncRequestProcessorOrchestrator.
 * Follows the async architecture pattern where servlets are thin wrappers that delegate to the processing pipeline.
 */
public class ConsumerGroupRouteHandler extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupRouteHandler.class);

    /**
     * Constructs a ConsumerGroupRouteHandler.
     * No dependencies needed since this only handles routing.
     */
    public ConsumerGroupRouteHandler() {
        // No dependencies - this servlet only parses URLs and queues requests
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid path");
            return;
        }

        String[] pathParts = pathInfo.substring(1).split("/");
        logger.debug("POST pathInfo: {}, pathParts: {}", pathInfo, Arrays.toString(pathParts));

        if (pathParts.length >= 1) {
            String groupId = pathParts[0];
            
            if (pathParts.length >= 2 && "consumers".equals(pathParts[1])) {
                // POST /consumer-groups/{groupId}/consumers - Add consumer to group
                handleRequest(request, response, AsyncRequest.RequestType.ADD_CONSUMER_TO_GROUP, groupId);
            } else {
                // POST /consumer-groups/{groupId} - Create consumer group
                handleRequest(request, response, AsyncRequest.RequestType.CREATE_CONSUMER_GROUP, groupId);
            }
        } else {
            logger.debug("POST endpoint not found for path: {}", pathInfo);
            sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Endpoint not found");
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid path");
            return;
        }

        // Handle root path - GET /consumer-groups/ - List consumer groups
        if (pathInfo.equals("/")) {
            handleRequest(request, response, AsyncRequest.RequestType.LIST_CONSUMER_GROUPS, null);
            return;
        }

        String[] pathParts = pathInfo.substring(1).split("/");
        logger.debug("GET pathInfo: {}, pathParts: {}", pathInfo, Arrays.toString(pathParts));

        if (pathParts.length >= 1) {
            String groupId = pathParts[0];
            
            if (pathParts.length == 1) {
                // GET /consumer-groups/{groupId} - Get consumer group info
                handleRequest(request, response, AsyncRequest.RequestType.GET_CONSUMER_GROUP, groupId);
            } else if (pathParts.length == 2) {
                String action = pathParts[1];
                switch (action) {
                    case "consume":
                        // GET /consumer-groups/{groupId}/consume - Consume messages from group
                        handleRequest(request, response, AsyncRequest.RequestType.CONSUME_FROM_GROUP, groupId);
                        break;
                    default:
                        logger.debug("GET action not found: {}", action);
                        sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Action not found");
                        break;
                }
            } else {
                logger.debug("GET path too deep: {}", pathInfo);
                sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Path too deep");
            }
        } else {
            logger.debug("GET endpoint not found for path: {}", pathInfo);
            sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Endpoint not found");
        }
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, "Invalid path");
            return;
        }

        String[] pathParts = pathInfo.substring(1).split("/");
        logger.debug("DELETE pathInfo: {}, pathParts: {}", pathInfo, Arrays.toString(pathParts));

        if (pathParts.length >= 1) {
            String groupId = pathParts[0];
            
            if (pathParts.length >= 2 && "consumers".equals(pathParts[1]) && pathParts.length >= 3) {
                String consumerId = pathParts[2];
                // DELETE /consumer-groups/{groupId}/consumers/{consumerId} - Remove consumer from group
                handleRequest(request, response, AsyncRequest.RequestType.REMOVE_CONSUMER_FROM_GROUP, groupId + "/" + consumerId);
            } else {
                // DELETE /consumer-groups/{groupId} - Delete consumer group
                handleRequest(request, response, AsyncRequest.RequestType.DELETE_CONSUMER_GROUP, groupId);
            }
        } else {
            logger.debug("DELETE endpoint not found for path: {}", pathInfo);
            sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, "Endpoint not found");
        }
    }

    @Override
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                               AsyncRequest.RequestType defaultType) throws ServletException, IOException {
        // This method is not used since we override doGet, doPost, doDelete directly
        // But we need to implement it since it's abstract in BaseAsyncServlet
        throw new UnsupportedOperationException("Use specific HTTP method handlers instead");
    }
    
    /**
     * Handles the incoming request by parsing URL and queuing it for async processing.
     * 
     * @param request The HTTP request
     * @param response The HTTP response
     * @param requestType The type of request to process
     * @param groupId The consumer group ID extracted from URL (if applicable)
     */
    private void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                              AsyncRequest.RequestType requestType, String groupId) 
            throws ServletException, IOException {
        logger.debug("Processing consumer group request: type={}, groupId={}", requestType, groupId);
        
        // Create async context
        AsyncContext asyncContext = request.startAsync();
        asyncContext.setTimeout(30000); // 30 second timeout
        
        // Generate request ID
        String requestId = generateRequestId();
        
        // Create parameters map
        Map<String, Object> parameters = new HashMap<>();
        if (groupId != null) {
            parameters.put(AsyncRequestKeys.GROUP_ID, groupId);
        }
        
        // Create AsyncRequest with proper parameters
        AsyncRequest asyncRequest = new AsyncRequest(
            requestId, request, response, asyncContext, 
            requestType, Optional.of(parameters)
        );
        
        // Add to request channel for async processing
        RequestChannel requestChannel = getRequestChannel();
        boolean queued = requestChannel.addRequest(asyncRequest);
        
        if (!queued) {
            logger.warn("Request queue is full, rejecting request {}", requestId);
            sendErrorResponse(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Server busy, please try again");
            asyncContext.complete();
        }
    }

    /**
     * Sends an error response directly (for cases where we can't queue the request).
     */
    protected void sendErrorResponse(HttpServletResponse response, int statusCode, String message) {
        try {
            response.setStatus(statusCode);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\": \"" + message + "\"}");
        } catch (IOException e) {
            logger.error("Failed to send error response", e);
        }
    }
}
