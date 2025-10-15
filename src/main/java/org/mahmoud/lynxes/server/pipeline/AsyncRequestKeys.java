package org.mahmoud.lynxes.server.pipeline;

/**
 * Constants for AsyncRequest parameter keys to avoid typos and ensure consistency.
 * This class provides type-safe access to request parameters.
 */
public final class AsyncRequestKeys {
    
    // Topic-related parameters
    public static final String TOPIC_NAME = "topicName";
    public static final String MESSAGE = "message";
    public static final String OFFSET = "offset";
    
    // Consumer-related parameters
    public static final String CONSUMER_ID = "consumerId";
    public static final String CONSUMER_NAME = "consumerName";
    
    // Consumer group-related parameters
    public static final String GROUP_ID = "groupId";
    public static final String GROUP_NAME = "groupName";
    
    // Request-specific parameters
    public static final String REQUEST_BODY = "requestBody";
    public static final String QUERY_PARAMS = "queryParams";
    public static final String HEADERS = "headers";
    
    // Pagination parameters
    public static final String LIMIT = "limit";
    public static final String PAGE = "page";
    
    // Filter parameters
    public static final String FILTER = "filter";
    public static final String SORT_BY = "sortBy";
    public static final String SORT_ORDER = "sortOrder";
    
    // Prevent instantiation
    private AsyncRequestKeys() {
        throw new UnsupportedOperationException("Utility class");
    }
}
