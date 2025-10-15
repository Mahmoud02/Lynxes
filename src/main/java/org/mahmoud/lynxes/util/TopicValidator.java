package org.mahmoud.lynxes.util;

/**
 * Utility class for topic name validation and sanitization.
 * Provides methods for validating and cleaning topic names according to Lynxes naming conventions.
 */
public class TopicValidator {
    
    private static final String DEFAULT_TOPIC_NAME = "default";
    private static final String TOPIC_NAME_PATTERN = "^[a-zA-Z0-9_-]+$";
    private static final int MAX_TOPIC_NAME_LENGTH = 255;
    private static final int MIN_TOPIC_NAME_LENGTH = 1;
    
    /**
     * Validates a topic name according to Lynxes naming conventions.
     * 
     * @param topicName The topic name to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidTopicName(String topicName) {
        if (topicName == null) {
            return false;
        }
        
        String trimmed = topicName.trim();
        
        // Check length constraints
        if (trimmed.length() < MIN_TOPIC_NAME_LENGTH || trimmed.length() > MAX_TOPIC_NAME_LENGTH) {
            return false;
        }
        
        // Check if empty after trimming
        if (trimmed.isEmpty()) {
            return false;
        }
        
        // Check pattern (alphanumeric with hyphens and underscores)
        return trimmed.matches(TOPIC_NAME_PATTERN);
    }
    
    /**
     * Sanitizes a topic name by removing invalid characters.
     * Replaces invalid characters with underscores and handles edge cases.
     * 
     * @param topicName The topic name to sanitize
     * @return The sanitized topic name
     */
    public static String sanitizeTopicName(String topicName) {
        if (topicName == null) {
            return DEFAULT_TOPIC_NAME;
        }
        
        String trimmed = topicName.trim();
        
        // Return default if empty after trimming
        if (trimmed.isEmpty()) {
            return DEFAULT_TOPIC_NAME;
        }
        
        // Replace invalid characters with underscores
        String sanitized = trimmed.replaceAll("[^a-zA-Z0-9_-]", "_");
        
        // Ensure it doesn't start or end with underscore or hyphen
        sanitized = sanitized.replaceAll("^[_-]+", "");
        sanitized = sanitized.replaceAll("[_-]+$", "");
        
        // Return default if nothing left after sanitization
        if (sanitized.isEmpty()) {
            return DEFAULT_TOPIC_NAME;
        }
        
        // Truncate if too long
        if (sanitized.length() > MAX_TOPIC_NAME_LENGTH) {
            sanitized = sanitized.substring(0, MAX_TOPIC_NAME_LENGTH);
        }
        
        return sanitized;
    }
    
    /**
     * Gets validation error message for an invalid topic name.
     * 
     * @param topicName The topic name that failed validation
     * @return A descriptive error message
     */
    public static String getValidationErrorMessage(String topicName) {
        if (topicName == null) {
            return "Topic name cannot be null";
        }
        
        String trimmed = topicName.trim();
        
        if (trimmed.isEmpty()) {
            return "Topic name cannot be empty";
        }
        
        if (trimmed.length() < MIN_TOPIC_NAME_LENGTH) {
            return "Topic name must be at least " + MIN_TOPIC_NAME_LENGTH + " character long";
        }
        
        if (trimmed.length() > MAX_TOPIC_NAME_LENGTH) {
            return "Topic name cannot exceed " + MAX_TOPIC_NAME_LENGTH + " characters";
        }
        
        if (!trimmed.matches(TOPIC_NAME_PATTERN)) {
            return "Topic name can only contain letters, numbers, hyphens, and underscores";
        }
        
        return "Unknown validation error";
    }
    
    /**
     * Checks if a topic name is reserved or has special meaning.
     * 
     * @param topicName The topic name to check
     * @return true if reserved, false otherwise
     */
    public static boolean isReservedTopicName(String topicName) {
        if (topicName == null) {
            return false;
        }
        
        String normalized = topicName.toLowerCase().trim();
        
        // Add reserved names as needed
        return normalized.equals("__consumer_offsets") ||
               normalized.equals("__transaction_state") ||
               normalized.equals("__schemas") ||
               normalized.startsWith("__");
    }
}
