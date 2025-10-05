package org.mahmoud.fastqueue.core;

/**
 * Flush strategy for controlling when data is written to disk.
 * Inspired by Kafka's approach with configurable durability vs performance trade-offs.
 */
public enum FlushStrategy {
    
    /**
     * Immediate flush after every write (highest durability, lowest performance).
     * Equivalent to fsync after every message.
     */
    IMMEDIATE("immediate", "Flush after every write", 1, 0),
    
    /**
     * Flush based on message count (Kafka-style).
     * Flushes every N messages or time interval, whichever comes first.
     */
    MESSAGE_BASED("message_based", "Flush based on message count", 1000, 1000),
    
    /**
     * Flush based on time intervals (Kafka-style).
     * Flushes every N milliseconds or message count, whichever comes first.
     */
    TIME_BASED("time_based", "Flush based on time intervals", 1000, 1000),
    
    /**
     * Hybrid approach - flush on both message count AND time (Kafka default).
     * Flushes when either condition is met: N messages OR N milliseconds.
     */
    HYBRID("hybrid", "Flush on both message count and time", 1000, 1000),
    
    /**
     * OS-controlled flushing (lowest durability, highest performance).
     * Only flushes on explicit calls or system shutdown.
     * WARNING: High data loss risk in case of crashes.
     */
    OS_CONTROLLED("os_controlled", "Let OS decide when to flush", Integer.MAX_VALUE, Integer.MAX_VALUE);
    
    private final String name;
    private final String description;
    private final int defaultMessageInterval;
    private final int defaultTimeIntervalMs;
    
    FlushStrategy(String name, String description, int defaultMessageInterval, int defaultTimeIntervalMs) {
        this.name = name;
        this.description = description;
        this.defaultMessageInterval = defaultMessageInterval;
        this.defaultTimeIntervalMs = defaultTimeIntervalMs;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDescription() {
        return description;
    }
    
    public int getDefaultMessageInterval() {
        return defaultMessageInterval;
    }
    
    public int getDefaultTimeIntervalMs() {
        return defaultTimeIntervalMs;
    }
    
    /**
     * Get flush strategy by name (case-insensitive).
     */
    public static FlushStrategy fromName(String name) {
        if (name == null) {
            return HYBRID; // Default
        }
        
        for (FlushStrategy strategy : values()) {
            if (strategy.name.equalsIgnoreCase(name)) {
                return strategy;
            }
        }
        
        throw new IllegalArgumentException("Unknown flush strategy: " + name);
    }
    
    /**
     * Check if this strategy requires immediate flushing.
     */
    public boolean isImmediate() {
        return this == IMMEDIATE;
    }
    
    /**
     * Check if this strategy uses message-based flushing.
     */
    public boolean usesMessageInterval() {
        return this == MESSAGE_BASED || this == HYBRID;
    }
    
    /**
     * Check if this strategy uses time-based flushing.
     */
    public boolean usesTimeInterval() {
        return this == TIME_BASED || this == HYBRID;
    }
    
    /**
     * Check if this strategy relies on OS for flushing.
     */
    public boolean isOsControlled() {
        return this == OS_CONTROLLED;
    }
}
