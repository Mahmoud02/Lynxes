package org.mahmoud.lynxes.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for file and directory operations.
 * Provides safe methods for common file system operations used in Lynxes.
 */
public class FileUtils {
    
    /**
     * Safely deletes a directory and all its contents.
     * Deletes files before directories to avoid issues with non-empty directories.
     * 
     * @param directory The directory to delete
     * @throws IOException if deletion fails
     */
    public static void deleteDirectory(Path directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("Directory cannot be null");
        }
        
        if (!Files.exists(directory)) {
            return;
        }
        
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("Path is not a directory: " + directory);
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
     * Creates all necessary parent directories.
     * 
     * @param directory The directory to create
     * @throws IOException if creation fails
     */
    public static void createDirectoryIfNotExists(Path directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("Directory cannot be null");
        }
        
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }
    }
    
    /**
     * Checks if a directory exists and is readable.
     * 
     * @param directory The directory to check
     * @return true if directory exists and is readable, false otherwise
     */
    public static boolean isReadableDirectory(Path directory) {
        if (directory == null) {
            return false;
        }
        
        return Files.exists(directory) && 
               Files.isDirectory(directory) && 
               Files.isReadable(directory);
    }
    
    /**
     * Checks if a directory exists and is writable.
     * 
     * @param directory The directory to check
     * @return true if directory exists and is writable, false otherwise
     */
    public static boolean isWritableDirectory(Path directory) {
        if (directory == null) {
            return false;
        }
        
        return Files.exists(directory) && 
               Files.isDirectory(directory) && 
               Files.isWritable(directory);
    }
    
    /**
     * Gets the size of a directory in bytes.
     * Recursively calculates the total size of all files in the directory.
     * 
     * @param directory The directory to measure
     * @return The total size in bytes
     * @throws IOException if calculation fails
     */
    public static long getDirectorySize(Path directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("Directory cannot be null");
        }
        
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            return 0;
        }
        
        return Files.walk(directory)
            .filter(Files::isRegularFile)
            .mapToLong(path -> {
                try {
                    return Files.size(path);
                } catch (IOException e) {
                    return 0;
                }
            })
            .sum();
    }
    
    /**
     * Counts the number of files in a directory.
     * 
     * @param directory The directory to count files in
     * @return The number of files
     * @throws IOException if counting fails
     */
    public static long countFiles(Path directory) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("Directory cannot be null");
        }
        
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            return 0;
        }
        
        return Files.walk(directory)
            .filter(Files::isRegularFile)
            .count();
    }
}
