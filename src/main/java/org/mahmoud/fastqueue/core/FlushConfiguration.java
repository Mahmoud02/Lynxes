package org.mahmoud.fastqueue.core;

/**
 * Configuration for flush behavior, inspired by Kafka's settings.
 * Provides fine-grained control over durability vs performance trade-offs.
 */
public class FlushConfiguration {
    
    private final FlushStrategy strategy;
    private final int messageInterval;
    private final int timeIntervalMs;
    private final boolean forceMetadata;
    private final boolean enablePageCache;
    
    /**
     * Creates a flush configuration with the specified parameters.
     * 
     * @param strategy The flush strategy to use
     * @param messageInterval Number of messages before forcing flush (if applicable)
     * @param timeIntervalMs Time interval in milliseconds before forcing flush (if applicable)
     * @param forceMetadata Whether to force metadata sync (more durable but slower)
     * @param enablePageCache Whether to use page cache optimization
     */
    public FlushConfiguration(FlushStrategy strategy, 
                            int messageInterval, 
                            int timeIntervalMs, 
                            boolean forceMetadata, 
                            boolean enablePageCache) {
        this.strategy = strategy;
        this.messageInterval = messageInterval;
        this.timeIntervalMs = timeIntervalMs;
        this.forceMetadata = forceMetadata;
        this.enablePageCache = enablePageCache;
        
        validateConfiguration();
    }
    
    /**
     * Creates a flush configuration with default values for the strategy.
     */
    public FlushConfiguration(FlushStrategy strategy) {
        this(strategy, 
             strategy.getDefaultMessageInterval(), 
             strategy.getDefaultTimeIntervalMs(), 
             true, // Default to forcing metadata for durability
             true); // Default to enabling page cache
    }
    
    /**
     * Creates a Kafka-style configuration (hybrid approach).
     */
    public static FlushConfiguration kafkaStyle() {
        return new FlushConfiguration(FlushStrategy.HYBRID, 10000, 3000, true, true);
    }
    
    /**
     * Creates a high-performance configuration (OS-controlled).
     */
    public static FlushConfiguration highPerformance() {
        return new FlushConfiguration(FlushStrategy.OS_CONTROLLED, Integer.MAX_VALUE, Integer.MAX_VALUE, false, true);
    }
    
    /**
     * Creates a high-durability configuration (immediate flush).
     */
    public static FlushConfiguration highDurability() {
        return new FlushConfiguration(FlushStrategy.IMMEDIATE, 1, 0, true, false);
    }
    
    /**
     * Creates a balanced configuration (moderate intervals).
     */
    public static FlushConfiguration balanced() {
        return new FlushConfiguration(FlushStrategy.HYBRID, 1000, 1000, true, true);
    }
    
    private void validateConfiguration() {
        if (messageInterval <= 0) {
            throw new IllegalArgumentException("Message interval must be positive");
        }
        if (timeIntervalMs < 0) {
            throw new IllegalArgumentException("Time interval cannot be negative");
        }
        if (strategy.usesMessageInterval() && messageInterval == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Message-based strategy requires finite message interval");
        }
        if (strategy.usesTimeInterval() && timeIntervalMs == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Time-based strategy requires finite time interval");
        }
    }
    
    public FlushStrategy getStrategy() {
        return strategy;
    }
    
    public int getMessageInterval() {
        return messageInterval;
    }
    
    public int getTimeIntervalMs() {
        return timeIntervalMs;
    }
    
    public boolean isForceMetadata() {
        return forceMetadata;
    }
    
    public boolean isEnablePageCache() {
        return enablePageCache;
    }
    
    /**
     * Check if flush should be triggered based on message count.
     */
    public boolean shouldFlushOnMessageCount(int messageCount) {
        return strategy.usesMessageInterval() && messageCount >= messageInterval;
    }
    
    /**
     * Check if flush should be triggered based on time elapsed.
     */
    public boolean shouldFlushOnTime(long elapsedMs) {
        return strategy.usesTimeInterval() && elapsedMs >= timeIntervalMs;
    }
    
    /**
     * Check if immediate flush is required.
     */
    public boolean requiresImmediateFlush() {
        return strategy.isImmediate();
    }
    
    /**
     * Check if this configuration uses message-based flushing.
     */
    public boolean usesMessageInterval() {
        return strategy.usesMessageInterval();
    }
    
    /**
     * Check if this configuration uses time-based flushing.
     */
    public boolean usesTimeInterval() {
        return strategy.usesTimeInterval();
    }
    
    /**
     * Get the maximum time between flushes in milliseconds.
     */
    public long getMaxTimeBetweenFlushes() {
        if (strategy.isImmediate()) {
            return 0;
        } else if (strategy.usesTimeInterval()) {
            return timeIntervalMs;
        } else {
            return Long.MAX_VALUE;
        }
    }
    
    /**
     * Get the maximum messages between flushes.
     */
    public int getMaxMessagesBetweenFlushes() {
        if (strategy.isImmediate()) {
            return 1;
        } else if (strategy.usesMessageInterval()) {
            return messageInterval;
        } else {
            return Integer.MAX_VALUE;
        }
    }
    
    @Override
    public String toString() {
        return String.format("FlushConfiguration{strategy=%s, messages=%d, timeMs=%d, forceMetadata=%s, pageCache=%s}",
                           strategy.getName(), messageInterval, timeIntervalMs, forceMetadata, enablePageCache);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        FlushConfiguration that = (FlushConfiguration) obj;
        return messageInterval == that.messageInterval &&
               timeIntervalMs == that.timeIntervalMs &&
               forceMetadata == that.forceMetadata &&
               enablePageCache == that.enablePageCache &&
               strategy == that.strategy;
    }
    
    @Override
    public int hashCode() {
        int result = strategy.hashCode();
        result = 31 * result + messageInterval;
        result = 31 * result + timeIntervalMs;
        result = 31 * result + (forceMetadata ? 1 : 0);
        result = 31 * result + (enablePageCache ? 1 : 0);
        return result;
    }
}
