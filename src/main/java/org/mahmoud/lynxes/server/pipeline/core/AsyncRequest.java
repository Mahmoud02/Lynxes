package org.mahmoud.lynxes.server.pipeline.core;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents an asynchronous request that can be processed by the AsyncProcessor.
 * Uses a generic parameters map to store request-specific data.
 */
public class AsyncRequest {
    
    public enum RequestType {
        PUBLISH,
        CONSUME,
        HEALTH,
        TOPICS,
        METRICS,
        DELETE_TOPIC,
        DELETE_ALL_TOPICS,
        // Consumer operations
        REGISTER_CONSUMER,
        LIST_CONSUMERS,
        CONSUMER_STATUS,
        CONSUMER_MESSAGES,
        DELETE_CONSUMER,
        // Consumer group operations
        CREATE_CONSUMER_GROUP,
        LIST_CONSUMER_GROUPS,
        GET_CONSUMER_GROUP,
        DELETE_CONSUMER_GROUP,
        ADD_CONSUMER_TO_GROUP,
        REMOVE_CONSUMER_FROM_GROUP,
        CONSUME_FROM_GROUP
    }
    
    private final String requestId;
    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private final AsyncContext asyncContext;
    private final RequestType type;
    private final long timestamp;
    private final Map<String, Object> parameters;
    
    /**
     * Creates a new AsyncRequest with optional parameters.
     * 
     * @param requestId Unique identifier for this request
     * @param request The HTTP request
     * @param response The HTTP response
     * @param asyncContext The async context for this request
     * @param type The type of request
     * @param parameters Optional map of request-specific parameters
     */
    public AsyncRequest(String requestId, HttpServletRequest request, HttpServletResponse response,
                       AsyncContext asyncContext, RequestType type, 
                       Optional<Map<String, Object>> parameters) {
        this.requestId = requestId;
        this.request = request;
        this.response = response;
        this.asyncContext = asyncContext;
        this.type = type;
        this.parameters = parameters.orElse(new HashMap<>());
        this.timestamp = System.currentTimeMillis();
    }
    
    // Getters for core fields
    public String getRequestId() { return requestId; }
    public HttpServletRequest getRequest() { return request; }
    public HttpServletResponse getResponse() { return response; }
    public AsyncContext getAsyncContext() { return asyncContext; }
    public RequestType getType() { return type; }
    public long getTimestamp() { return timestamp; }
    
    // Helper methods for safe parameter access
    public Optional<String> getString(String key) {
        Object value = parameters.get(key);
        return value != null ? Optional.of(value.toString()) : Optional.empty();
    }
    
    public Optional<Long> getLong(String key) {
        Object value = parameters.get(key);
        if (value instanceof Long) return Optional.of((Long) value);
        if (value instanceof Number) return Optional.of(((Number) value).longValue());
        return Optional.empty();
    }
    
    public Optional<Integer> getInt(String key) {
        Object value = parameters.get(key);
        if (value instanceof Integer) return Optional.of((Integer) value);
        if (value instanceof Number) return Optional.of(((Number) value).intValue());
        return Optional.empty();
    }
    
    public Optional<Boolean> getBoolean(String key) {
        Object value = parameters.get(key);
        if (value instanceof Boolean) return Optional.of((Boolean) value);
        if (value instanceof String) return Optional.of(Boolean.parseBoolean((String) value));
        return Optional.empty();
    }
    
    public Optional<Object> getParameter(String key) {
        return Optional.ofNullable(parameters.get(key));
    }
    
    public boolean hasParameter(String key) {
        return parameters.containsKey(key);
    }
    
    public Map<String, Object> getAllParameters() {
        return new HashMap<>(parameters);
    }
    
    @Override
    public String toString() {
        return String.format("AsyncRequest{id=%s, type=%s, params=%s, timestamp=%d}", 
                           requestId, type, parameters, timestamp);
    }
}
