package org.mahmoud.lynxes.util;

/**
 * Utility class for concurrency-related operations.
 * Provides safe methods for thread operations and synchronization.
 */
public class ConcurrencyUtils {
    
    /**
     * Sleeps for the specified number of milliseconds.
     * Properly handles InterruptedException by restoring the interrupt flag.
     * 
     * @param milliseconds The number of milliseconds to sleep
     * @throws InterruptedException if the sleep is interrupted
     */
    public static void sleep(long milliseconds) throws InterruptedException {
        if (milliseconds < 0) {
            throw new IllegalArgumentException("Sleep time cannot be negative");
        }
        
        Thread.sleep(milliseconds);
    }
    
    /**
     * Sleeps for the specified number of milliseconds.
     * Handles InterruptedException by restoring the interrupt flag and returning false.
     * 
     * @param milliseconds The number of milliseconds to sleep
     * @return true if sleep completed normally, false if interrupted
     */
    public static boolean sleepUninterruptibly(long milliseconds) {
        if (milliseconds < 0) {
            throw new IllegalArgumentException("Sleep time cannot be negative");
        }
        
        try {
            Thread.sleep(milliseconds);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Sleeps for the specified number of milliseconds with a maximum retry count.
     * Useful for implementing retry logic with exponential backoff.
     * 
     * @param milliseconds The number of milliseconds to sleep
     * @param maxRetries Maximum number of retries if interrupted
     * @return true if sleep completed successfully, false if max retries exceeded
     */
    public static boolean sleepWithRetry(long milliseconds, int maxRetries) {
        if (milliseconds < 0) {
            throw new IllegalArgumentException("Sleep time cannot be negative");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("Max retries cannot be negative");
        }
        
        int retries = 0;
        while (retries <= maxRetries) {
            try {
                Thread.sleep(milliseconds);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                retries++;
                if (retries > maxRetries) {
                    return false;
                }
            }
        }
        return false;
    }
    
    /**
     * Checks if the current thread is interrupted.
     * 
     * @return true if the current thread is interrupted, false otherwise
     */
    public static boolean isInterrupted() {
        return Thread.currentThread().isInterrupted();
    }
    
    /**
     * Clears the interrupt flag of the current thread.
     * Use with caution - only when you're sure you want to clear the interrupt status.
     */
    public static void clearInterrupt() {
        Thread.interrupted();
    }
    
    /**
     * Gets the current thread name.
     * 
     * @return The name of the current thread
     */
    public static String getCurrentThreadName() {
        return Thread.currentThread().getName();
    }
    
    /**
     * Gets the current thread ID.
     * 
     * @return The ID of the current thread
     */
    public static long getCurrentThreadId() {
        return Thread.currentThread().threadId();
    }
}
