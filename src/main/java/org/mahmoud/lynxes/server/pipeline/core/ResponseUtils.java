package org.mahmoud.lynxes.server.pipeline.core;

import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility class for sending HTTP responses.
 * Centralizes response formatting and error handling.
 */
public final class ResponseUtils {
    private static final Logger logger = LoggerFactory.getLogger(ResponseUtils.class);
    
    // Content types
    public static final String APPLICATION_JSON = "application/json";
    public static final String TEXT_PLAIN = "text/plain";
    public static final String TEXT_HTML = "text/html";
    
    private ResponseUtils() {
        // Private constructor to prevent instantiation
    }
    
    /**
     * Sends a successful JSON response.
     *
     * @param request The async request
     * @param responseBody The JSON response body
     * @throws IOException if response writing fails
     */
    public static void sendSuccessResponse(AsyncRequest request, String responseBody) throws IOException {
        sendResponse(request, HttpStatusCodes.OK, APPLICATION_JSON, responseBody);
    }
    
    /**
     * Sends an error response with JSON format.
     *
     * @param request The async request
     * @param statusCode The HTTP status code
     * @param errorMessage The error message
     * @throws IOException if response writing fails
     */
    public static void sendErrorResponse(AsyncRequest request, int statusCode, String errorMessage) throws IOException {
        String errorBody = String.format("{\"error\":\"%s\"}", errorMessage);
        sendResponse(request, statusCode, APPLICATION_JSON, errorBody);
    }
    
    /**
     * Sends a bad request error response.
     *
     * @param request The async request
     * @param errorMessage The error message
     * @throws IOException if response writing fails
     */
    public static void sendBadRequestError(AsyncRequest request, String errorMessage) throws IOException {
        sendErrorResponse(request, HttpStatusCodes.BAD_REQUEST, errorMessage);
    }
    
    /**
     * Sends a not found error response.
     *
     * @param request The async request
     * @param errorMessage The error message
     * @throws IOException if response writing fails
     */
    public static void sendNotFoundError(AsyncRequest request, String errorMessage) throws IOException {
        sendErrorResponse(request, HttpStatusCodes.NOT_FOUND, errorMessage);
    }
    
    /**
     * Sends an internal server error response.
     *
     * @param request The async request
     * @param errorMessage The error message
     * @throws IOException if response writing fails
     */
    public static void sendInternalServerError(AsyncRequest request, String errorMessage) throws IOException {
        sendErrorResponse(request, HttpStatusCodes.INTERNAL_SERVER_ERROR, errorMessage);
    }
    
    /**
     * Sends a generic HTTP response.
     *
     * @param request The async request
     * @param statusCode The HTTP status code
     * @param contentType The content type
     * @param body The response body
     * @throws IOException if response writing fails
     */
    public static void sendResponse(AsyncRequest request, int statusCode, String contentType, String body) throws IOException {
        try {
            HttpServletResponse response = request.getResponse();
            response.setStatus(statusCode);
            response.setContentType(contentType);
            response.getWriter().write(body);
            request.getAsyncContext().complete();
            
            logger.debug("Sent response for request {}: status={}, contentType={}", 
                        request.getRequestId(), statusCode, contentType);
        } catch (IOException e) {
            logger.error("Failed to send response for request {}: {}", request.getRequestId(), e.getMessage());
            throw e;
        }
    }
}
