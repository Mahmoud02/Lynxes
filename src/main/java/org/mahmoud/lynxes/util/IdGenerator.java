package org.mahmoud.lynxes.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for generating unique identifiers.
 * Provides methods for generating various types of IDs used throughout the Lynxes system.
 */
public class IdGenerator {
    
    private static final String ALPHANUMERIC_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    
    /**
     * Generates a unique segment ID.
     * Format: segment-{timestamp}-{random}
     * 
     * @return A unique segment ID
     */
    public static String generateSegmentId() {
        long timestamp = System.currentTimeMillis();
        long random = ThreadLocalRandom.current().nextLong();
        return String.format("segment-%d-%d", timestamp, Math.abs(random));
    }
    
    /**
     * Generates a unique topic ID.
     * Format: topic-{timestamp}-{random}
     * 
     * @return A unique topic ID
     */
    public static String generateTopicId() {
        long timestamp = System.currentTimeMillis();
        long random = ThreadLocalRandom.current().nextLong();
        return String.format("topic-%d-%d", timestamp, Math.abs(random));
    }
    
    /**
     * Generates a random string of specified length.
     * Uses alphanumeric characters (A-Z, a-z, 0-9).
     * 
     * @param length The length of the string
     * @return Random string
     */
    public static String generateRandomString(int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive");
        }
        
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHANUMERIC_CHARS.charAt(ThreadLocalRandom.current().nextInt(ALPHANUMERIC_CHARS.length())));
        }
        
        return sb.toString();
    }
    
    /**
     * Generates a UUID-like string without hyphens.
     * Useful for creating compact unique identifiers.
     * 
     * @return A compact unique identifier
     */
    public static String generateCompactId() {
        return generateRandomString(16);
    }
}
