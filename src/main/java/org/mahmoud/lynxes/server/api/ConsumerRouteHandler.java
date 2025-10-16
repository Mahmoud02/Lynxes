package org.mahmoud.lynxes.server.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequestKeys;

import java.io.IOException;
import jakarta.servlet.AsyncContext;
import org.mahmoud.lynxes.server.pipeline.channels.RequestChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Route handler for consumer operations.
 * This class only parses URLs and queues requests - all business logic is handled by AsyncRequestProcessorOrchestrator.
 * Follows the async architecture pattern where servlets are thin wrappers that delegate to the processing pipeline.
 */
public class ConsumerRouteHandler extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRouteHandler.class);

    /**
     * Constructs a ConsumerRouteHandler.
     * No dependencies needed since this only handles routing.
     */
    public ConsumerRouteHandler() {
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
            String consumerId = pathParts[0];
            // POST /consumers/{consumerId} - Register consumer
            handleRequest(request, response, AsyncRequest.RequestType.REGISTER_CONSUMER, consumerId);
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

        // Handle root path - GET /consumers/ - List consumers
        if (pathInfo.equals("/")) {
            handleRequest(request, response, AsyncRequest.RequestType.LIST_CONSUMERS, null);
            return;
        }

        String[] pathParts = pathInfo.substring(1).split("/");
        logger.debug("GET pathInfo: {}, pathParts: {}", pathInfo, Arrays.toString(pathParts));

        if (pathParts.length >= 1) {
            String consumerId = pathParts[0];
            
            if (pathParts.length == 1) {
                // GET /consumers/{consumerId} - Get consumer status
                handleRequest(request, response, AsyncRequest.RequestType.CONSUMER_STATUS, consumerId);
            } else if (pathParts.length == 2) {
                String action = pathParts[1];
                switch (action) {
                    case "messages":
                        // GET /consumers/{consumerId}/messages - Get consumer messages
                        handleRequest(request, response, AsyncRequest.RequestType.CONSUMER_MESSAGES, consumerId);
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
            String consumerId = pathParts[0];
            // DELETE /consumers/{consumerId} - Delete consumer
            handleRequest(request, response, AsyncRequest.RequestType.DELETE_CONSUMER, consumerId);
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
     * @param consumerId The consumer ID extracted from URL (if applicable)
     */
    private void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                              AsyncRequest.RequestType requestType, String consumerId) 
            throws ServletException, IOException {
        logger.debug("Processing consumer request: type={}, consumerId={}", requestType, consumerId);
        
        // Create async context
        AsyncContext asyncContext = request.startAsync();
        asyncContext.setTimeout(30000); // 30 second timeout
        
        // Generate request ID
        String requestId = generateRequestId();
        
        // Create parameters map
        Map<String, Object> parameters = new HashMap<>();
        if (consumerId != null) {
            parameters.put(AsyncRequestKeys.CONSUMER_ID, consumerId);
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
