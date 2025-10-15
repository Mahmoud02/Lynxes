package org.mahmoud.lynxes.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for common queue operations.
 */
public class QueueUtils {
    
    private static final String HEX_CHARS = "0123456789ABCDEF";
    
    /**
     * Generates a unique segment ID.
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
     * 
     * @return A unique topic ID
         */
    public static String generateTopicId() {
        long timestamp = System.currentTimeMillis();
        long random = ThreadLocalRandom.current().nextLong();
        return String.format("topic-%d-%d", timestamp, Math.abs(random));
    }
    
    /**
     * Calculates a simple hash for a string.
     * 
     * @param input The input string
     * @return The hash value
     */
    public static long calculateHash(String input) {
        if (input == null) {
            return 0;
        }
        
        long hash = 0;
        for (int i = 0; i < input.length(); i++) {
            hash = hash * 31 + input.charAt(i);
        }
        return Math.abs(hash);
    }
    
    /**
     * Calculates MD5 hash of a byte array.
     * 
     * @param data The input data
     * @return The MD5 hash as a hex string
     */
    public static String calculateMD5(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }
    
    /**
     * Converts byte array to hex string.
     * 
     * @param bytes The byte array
     * @return The hex string
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(HEX_CHARS.charAt((b >> 4) & 0xF));
            result.append(HEX_CHARS.charAt(b & 0xF));
        }
        return result.toString();
    }
    
    /**
     * Converts hex string to byte array.
     * 
     * @param hex The hex string
     * @return The byte array
     */
    public static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
    
    /**
     * Safely deletes a directory and all its contents.
     * 
     * @param directory The directory to delete
     * @throws IOException if deletion fails
     */
    public static void deleteDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        
        Files.walk(directory)
            .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
            .forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    System.err.println("Failed to delete " + path + ": " + e.getMessage());
                }
            });
    }
    
    /**
     * Creates a directory if it doesn't exist.
     * 
     * @param directory The directory to create
     * @throws IOException if creation fails
     */
    public static void createDirectoryIfNotExists(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }
    }
    
    /**
     * Validates a topic name.
     * 
     * @param topicName The topic name to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidTopicName(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return false;
        }
        
        // Topic names should be alphanumeric with hyphens and underscores
        return topicName.matches("^[a-zA-Z0-9_-]+$");
    }
    
    /**
     * Sanitizes a topic name by removing invalid characters.
     * 
     * @param topicName The topic name to sanitize
     * @return The sanitized topic name
     */
    public static String sanitizeTopicName(String topicName) {
        if (topicName == null) {
            return "default";
        }
        
        return topicName.replaceAll("[^a-zA-Z0-9_-]", "_");
    }
    
    /**
     * Formats bytes to human-readable format.
     * 
     * @param bytes The number of bytes
     * @return Human-readable string
     */
    public static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
    
    /**
     * Formats duration to human-readable format.
     * 
     * @param milliseconds The duration in milliseconds
     * @return Human-readable string
     */
    public static String formatDuration(long milliseconds) {
        if (milliseconds < 1000) {
            return milliseconds + " ms";
        }
        
        long seconds = milliseconds / 1000;
        if (seconds < 60) {
            return seconds + " s";
        }
        
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + " m " + (seconds % 60) + " s";
        }
        
        long hours = minutes / 60;
        return hours + " h " + (minutes % 60) + " m";
    }
    
    /**
     * Sleeps for the specified number of milliseconds.
     * 
     * @param milliseconds The number of milliseconds to sleep
     */
    public static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Generates a random string of specified length.
     * 
     * @param length The length of the string
     * @return Random string
     */
    public static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(ThreadLocalRandom.current().nextInt(chars.length())));
        }
        
        return sb.toString();
    }
}
