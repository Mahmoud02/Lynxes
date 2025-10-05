package org.mahmoud.fastqueue.server.async;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Represents an asynchronous response in the FastQueue2 system.
 * This object carries the response data and context for sending back to the client.
 */
public class AsyncResponse {
    private static final Logger logger = LoggerFactory.getLogger(AsyncResponse.class);

    private final String requestId;
    private final HttpServletResponse response;
    private final AsyncContext asyncContext;
    private final int statusCode;
    private final String contentType;
    private final byte[] responseBody;
    private final long timestamp;

    public AsyncResponse(String requestId, HttpServletResponse response, AsyncContext asyncContext,
                        int statusCode, String contentType, String responseBody) {
        this.requestId = requestId;
        this.response = response;
        this.asyncContext = asyncContext;
        this.statusCode = statusCode;
        this.contentType = contentType;
        this.responseBody = responseBody.getBytes(StandardCharsets.UTF_8);
        this.timestamp = System.currentTimeMillis();
    }

    public AsyncResponse(String requestId, HttpServletResponse response, AsyncContext asyncContext,
                        int statusCode, String contentType, byte[] responseBody) {
        this.requestId = requestId;
        this.response = response;
        this.asyncContext = asyncContext;
        this.statusCode = statusCode;
        this.contentType = contentType;
        this.responseBody = responseBody;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Sends the response to the client and completes the async context.
     */
    public void send() {
        try {
            response.setStatus(statusCode);
            response.setContentType(contentType);
            response.setContentLength(responseBody.length);
            response.getOutputStream().write(responseBody);
            response.getOutputStream().flush();
            logger.debug("Response sent for request: {} ({} bytes)", requestId, responseBody.length);
        } catch (IOException e) {
            logger.error("Failed to send response for request: {}", requestId, e);
        } finally {
            // Always complete the async context
            complete();
        }
    }
    
    /**
     * Completes the async context safely.
     */
    private void complete() {
        try {
            if (asyncContext != null) {
                asyncContext.complete();
                logger.debug("Async context completed for request: {}", requestId);
            }
        } catch (IllegalStateException e) {
            // This is expected if the context was already completed
            logger.debug("Async context already completed for request: {}", requestId);
        } catch (Exception e) {
            logger.error("Failed to complete async context for request: {}", requestId, e);
        }
    }

    // Getters
    public String getRequestId() { return requestId; }
    public HttpServletResponse getResponse() { return response; }
    public int getStatusCode() { return statusCode; }
    public String getContentType() { return contentType; }
    public byte[] getResponseBody() { return responseBody; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("AsyncResponse{id='%s', status=%d, contentType='%s', size=%d bytes}",
                requestId, statusCode, contentType, responseBody.length);
    }
}
