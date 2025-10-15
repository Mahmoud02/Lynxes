package org.mahmoud.lynxes.server.pipeline;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Represents an asynchronous request in the FastQueue2 system.
 * Similar to Kafka's request handling but adapted for HTTP.
 */
public class AsyncRequest {
    
    public enum RequestType {
        PUBLISH,
        CONSUME,
        HEALTH,
        TOPICS,
        METRICS,
        DELETE_TOPIC
    }
    
    private final String requestId;
    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final AsyncContext asyncContext;
    private final RequestType type;
    private final long timestamp;
    private final String topicName;
    private final Long offset;
    private final String message;
    
    public AsyncRequest(String requestId, HttpServletRequest request, HttpServletResponse response,
                       AsyncContext asyncContext, RequestType type, String topicName, 
                       Long offset, String message) {
        this.requestId = requestId;
        this.request = request;
        this.response = response;
        this.asyncContext = asyncContext;
        this.type = type;
        this.topicName = topicName;
        this.offset = offset;
        this.message = message;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Getters
    public String getRequestId() { return requestId; }
    public HttpServletRequest getRequest() { return request; }
    public HttpServletResponse getResponse() { return response; }
    public AsyncContext getAsyncContext() { return asyncContext; }
    public RequestType getType() { return type; }
    public long getTimestamp() { return timestamp; }
    public String getTopicName() { return topicName; }
    public Long getOffset() { return offset; }
    public String getMessage() { return message; }
    
    @Override
    public String toString() {
        return String.format("AsyncRequest{id=%s, type=%s, topic=%s, offset=%s, timestamp=%d}", 
                           requestId, type, topicName, offset, timestamp);
    }
}
