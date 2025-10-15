package org.mahmoud.lynxes.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for hash operations and data integrity.
 * Provides methods for calculating hashes and converting between different data formats.
 */
public class HashUtils {
    
    private static final String HEX_CHARS = "0123456789ABCDEF";
    
    /**
     * Calculates a simple hash for a string.
     * Uses a basic hash function suitable for general purpose hashing.
     * 
     * @param input The input string
     * @return The hash value (always positive)
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
     * @throws RuntimeException if MD5 algorithm is not available
     */
    public static String calculateMD5(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }
    
    /**
     * Calculates SHA-256 hash of a byte array.
     * 
     * @param data The input data
     * @return The SHA-256 hash as a hex string
     * @throws RuntimeException if SHA-256 algorithm is not available
     */
    public static String calculateSHA256(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(data);
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
    
    /**
     * Converts byte array to hex string.
     * 
     * @param bytes The byte array
     * @return The hex string in uppercase
     */
    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Bytes cannot be null");
        }
        
        StringBuilder result = new StringBuilder(bytes.length * 2);
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
     * @throws IllegalArgumentException if hex string is invalid
     */
    public static byte[] hexToBytes(String hex) {
        if (hex == null) {
            throw new IllegalArgumentException("Hex string cannot be null");
        }
        
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("Hex string length must be even");
        }
        
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
    
    /**
     * Verifies if a hex string is valid.
     * 
     * @param hex The hex string to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValidHex(String hex) {
        if (hex == null || hex.isEmpty()) {
            return false;
        }
        
        if (hex.length() % 2 != 0) {
            return false;
        }
        
        for (char c : hex.toCharArray()) {
            if (Character.digit(c, 16) == -1) {
                return false;
            }
        }
        
        return true;
    }
}
