package org.mahmoud.fastqueue.config;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration class for FastQueue2 message queue server.
 * Provides default values and configuration management.
 */
public class QueueConfig {
    
    // Default configuration values
    private static final long DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 1024; // 1GB
    private static final long DEFAULT_RETENTION_PERIOD_MS = 7 * 24 * 60 * 60 * 1000L; // 7 days
    private static final int DEFAULT_INDEX_INTERVAL = 1024; // 1KB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB
    private static final int DEFAULT_SERVER_PORT = 8080;
    private static final String DEFAULT_DATA_DIR = "./data";
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;
    
    // Core settings
    private final Path dataDirectory;
    private final long maxSegmentSize;
    private final long retentionPeriodMs;
    private final int indexInterval;
    private final int maxMessageSize;
    
    // Server settings
    private final int serverPort;
    private final int threadPoolSize;
    private final boolean enableCompression;
    private final boolean enableMetrics;
    
    // Performance settings
    private final int flushIntervalMs;
    private final int maxConcurrentConnections;
    private final boolean enableBatching;
    private final int batchSize;
    
    /**
     * Creates a configuration with default values.
     */
    public QueueConfig() {
        this.dataDirectory = Paths.get(DEFAULT_DATA_DIR);
        this.maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
        this.retentionPeriodMs = DEFAULT_RETENTION_PERIOD_MS;
        this.indexInterval = DEFAULT_INDEX_INTERVAL;
        this.maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        this.serverPort = DEFAULT_SERVER_PORT;
        this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        this.enableCompression = false;
        this.enableMetrics = true;
        this.flushIntervalMs = 1000; // 1 second
        this.maxConcurrentConnections = 1000;
        this.enableBatching = true;
        this.batchSize = 100;
    }
    
    /**
     * Creates a configuration with custom values.
     */
    public QueueConfig(Builder builder) {
        this.dataDirectory = builder.dataDirectory;
        this.maxSegmentSize = builder.maxSegmentSize;
        this.retentionPeriodMs = builder.retentionPeriodMs;
        this.indexInterval = builder.indexInterval;
        this.maxMessageSize = builder.maxMessageSize;
        this.serverPort = builder.serverPort;
        this.threadPoolSize = builder.threadPoolSize;
        this.enableCompression = builder.enableCompression;
        this.enableMetrics = builder.enableMetrics;
        this.flushIntervalMs = builder.flushIntervalMs;
        this.maxConcurrentConnections = builder.maxConcurrentConnections;
        this.enableBatching = builder.enableBatching;
        this.batchSize = builder.batchSize;
    }
    
    // Getters
    public Path getDataDirectory() {
        return dataDirectory;
    }
    
    public long getMaxSegmentSize() {
        return maxSegmentSize;
    }
    
    public long getRetentionPeriodMs() {
        return retentionPeriodMs;
    }
    
    public int getIndexInterval() {
        return indexInterval;
    }
    
    public int getMaxMessageSize() {
        return maxMessageSize;
    }
    
    public int getServerPort() {
        return serverPort;
    }
    
    public int getThreadPoolSize() {
        return threadPoolSize;
    }
    
    public boolean isCompressionEnabled() {
        return enableCompression;
    }
    
    public boolean isMetricsEnabled() {
        return enableMetrics;
    }
    
    public int getFlushIntervalMs() {
        return flushIntervalMs;
    }
    
    public int getMaxConcurrentConnections() {
        return maxConcurrentConnections;
    }
    
    public boolean isBatchingEnabled() {
        return enableBatching;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    /**
     * Builder pattern for creating custom configurations.
     */
    public static class Builder {
        private Path dataDirectory = Paths.get(DEFAULT_DATA_DIR);
        private long maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
        private long retentionPeriodMs = DEFAULT_RETENTION_PERIOD_MS;
        private int indexInterval = DEFAULT_INDEX_INTERVAL;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        private int serverPort = DEFAULT_SERVER_PORT;
        private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        private boolean enableCompression = false;
        private boolean enableMetrics = true;
        private int flushIntervalMs = 1000;
        private int maxConcurrentConnections = 1000;
        private boolean enableBatching = true;
        private int batchSize = 100;
        
        public Builder dataDirectory(Path dataDirectory) {
            this.dataDirectory = dataDirectory;
            return this;
        }
        
        public Builder maxSegmentSize(long maxSegmentSize) {
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }
        
        public Builder retentionPeriodMs(long retentionPeriodMs) {
            this.retentionPeriodMs = retentionPeriodMs;
            return this;
        }
        
        public Builder indexInterval(int indexInterval) {
            this.indexInterval = indexInterval;
            return this;
        }
        
        public Builder maxMessageSize(int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }
        
        public Builder serverPort(int serverPort) {
            this.serverPort = serverPort;
            return this;
        }
        
        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }
        
        public Builder enableCompression(boolean enableCompression) {
            this.enableCompression = enableCompression;
            return this;
        }
        
        public Builder enableMetrics(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
            return this;
        }
        
        public Builder flushIntervalMs(int flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }
        
        public Builder maxConcurrentConnections(int maxConcurrentConnections) {
            this.maxConcurrentConnections = maxConcurrentConnections;
            return this;
        }
        
        public Builder enableBatching(boolean enableBatching) {
            this.enableBatching = enableBatching;
            return this;
        }
        
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }
        
        public QueueConfig build() {
            return new QueueConfig(this);
        }
    }
    
    @Override
    public String toString() {
        return String.format("QueueConfig{dataDir=%s, maxSegmentSize=%d, retentionPeriodMs=%d, " +
                           "indexInterval=%d, maxMessageSize=%d, serverPort=%d, threadPoolSize=%d, " +
                           "compression=%s, metrics=%s, flushIntervalMs=%d, maxConnections=%d, " +
                           "batching=%s, batchSize=%d}",
                           dataDirectory, maxSegmentSize, retentionPeriodMs, indexInterval,
                           maxMessageSize, serverPort, threadPoolSize, enableCompression,
                           enableMetrics, flushIntervalMs, maxConcurrentConnections,
                           enableBatching, batchSize);
    }
}
