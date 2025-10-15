package org.mahmoud.lynxes.server.api;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.mahmoud.lynxes.server.pipeline.RequestChannel;
import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.AsyncRequestKeys;

/**
 * Base servlet class for async operations.
 * Provides common functionality for handling async requests and responses.
 */
public abstract class BaseAsyncServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(BaseAsyncServlet.class);
    
    private static final AtomicLong requestIdCounter = new AtomicLong(0);
    private static final int ASYNC_TIMEOUT_MS = 30000;
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        handleRequest(request, response, AsyncRequest.RequestType.HEALTH);
    }
    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        handleRequest(request, response, AsyncRequest.RequestType.PUBLISH);
    }
    
    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        handleRequest(request, response, AsyncRequest.RequestType.DELETE_TOPIC);
    }
    
    /**
     * Handles the incoming request. Must be implemented by subclasses.
     * 
     * @param request The HTTP request
     * @param response The HTTP response
     * @param defaultType The default request type
     * @throws ServletException if servlet processing fails
     * @throws IOException if I/O operations fail
     */
    protected abstract void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                                        AsyncRequest.RequestType defaultType) throws ServletException, IOException;
    
    /**
     * Processes an async request by adding it to the request channel.
     * 
     * @param request The HTTP request
     * @param response The HTTP response
     * @param type The request type
     * @param topicName The topic name (if applicable)
     * @param offset The offset (if applicable)
     * @param message The message content (if applicable)
     */
    protected void processAsyncRequest(HttpServletRequest request, HttpServletResponse response, 
                                     AsyncRequest.RequestType type, String topicName, Long offset, String message) {
        // Enable async processing
        AsyncContext asyncContext = request.startAsync();
        asyncContext.setTimeout(ASYNC_TIMEOUT_MS);
        
        // Create parameters map
        Map<String, Object> parameters = new HashMap<>();
        if (topicName != null) {
            parameters.put(AsyncRequestKeys.TOPIC_NAME, topicName);
        }
        if (offset != null) {
            parameters.put(AsyncRequestKeys.OFFSET, offset);
        }
        if (message != null) {
            parameters.put(AsyncRequestKeys.MESSAGE, message);
        }
        
        // Create async request
        String requestId = generateRequestId();
        AsyncRequest asyncRequest = new AsyncRequest(requestId, request, response, asyncContext, 
                                                   type, Optional.of(parameters));
        
        // Get request channel from servlet context
        RequestChannel requestChannel = getRequestChannel();
        if (requestChannel == null) {
            logger.error("RequestChannel not found in servlet context");
            sendErrorResponse(response, 500, "Internal server error");
            asyncContext.complete();
            return;
        }
        
        // Add to request channel
        if (requestChannel.addRequest(asyncRequest)) {
            logger.debug("Request queued: {}", requestId);
        } else {
            logger.warn("Request queue full, rejecting: {}", requestId);
            sendErrorResponse(response, 503, "Service temporarily unavailable");
            asyncContext.complete();
        }
    }
    
    /**
     * Gets the RequestChannel from the servlet context.
     * 
     * @return The RequestChannel instance
     */
    protected RequestChannel getRequestChannel() {
        return (RequestChannel) getServletContext().getAttribute("requestChannel");
    }
    
    /**
     * Sends an error response with the specified status code and message.
     * 
     * @param response The HTTP response
     * @param statusCode The HTTP status code
     * @param message The error message
     */
    protected void sendErrorResponse(HttpServletResponse response, int statusCode, String message) {
        try {
            response.setStatus(statusCode);
            response.setContentType("application/json");
            response.getWriter().print(String.format("{\"error\":\"%s\",\"code\":%d}", message, statusCode));
        } catch (IOException e) {
            logger.error("Error sending error response", e);
        }
    }
    
    /**
     * Generates a unique request ID.
     * 
     * @return A unique request ID
     */
    protected String generateRequestId() {
        return "req-" + requestIdCounter.incrementAndGet() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
