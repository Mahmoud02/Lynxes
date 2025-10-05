package org.mahmoud.fastqueue.config;

import org.mahmoud.fastqueue.core.FlushConfiguration;
import org.mahmoud.fastqueue.core.FlushStrategy;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration class for FastQueue2 message queue server.
 * Uses @ConfigProperty annotations to map configuration values from Typesafe Config.
 */
public class QueueConfig {
    
    // Default configuration values - only used as fallback when config is not available
    private static final long DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024; // 1MB
    private static final long DEFAULT_RETENTION_PERIOD_MS = 7 * 24 * 60 * 60 * 1000L; // 7 days
    private static final int DEFAULT_INDEX_INTERVAL = 1024; // 1KB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB
    private static final int DEFAULT_SERVER_PORT = 8080;
    private static final String DEFAULT_DATA_DIR = "./data";
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;
    private static final String DEFAULT_LOG_LEVEL = "INFO";
    private static final String DEFAULT_LOG_FILE = "logs/fastqueue2.log";
    
    // Core settings
    @ConfigProperty("fastqueue.storage.dataDirectory")
    private Path dataDirectory;
    
    @ConfigProperty("fastqueue.storage.maxSegmentSize")
    private long maxSegmentSize;
    
    @ConfigProperty("fastqueue.storage.retentionPeriodMs")
    private long retentionPeriodMs;
    
    @ConfigProperty("fastqueue.storage.indexInterval")
    private int indexInterval;
    
    @ConfigProperty("fastqueue.storage.maxMessageSize")
    private int maxMessageSize;
    
    // Server settings
    @ConfigProperty("fastqueue.server.port")
    private int serverPort;
    
    @ConfigProperty("fastqueue.server.threadPoolSize")
    private int threadPoolSize;
    
    @ConfigProperty("fastqueue.performance.enableCompression")
    private boolean enableCompression;
    
    @ConfigProperty("fastqueue.performance.enableMetrics")
    private boolean enableMetrics;
    
    // Performance settings
    @ConfigProperty("fastqueue.performance.flushIntervalMs")
    private int flushIntervalMs;
    
    @ConfigProperty("fastqueue.performance.maxConcurrentConnections")
    private int maxConcurrentConnections;
    
    @ConfigProperty("fastqueue.performance.enableBatching")
    private boolean enableBatching;
    
    @ConfigProperty("fastqueue.performance.batchSize")
    private int batchSize;
    
    // Flush strategy settings
    @ConfigProperty("fastqueue.flush.strategy")
    private String flushStrategy;
    
    @ConfigProperty("fastqueue.flush.messageInterval")
    private int flushMessageInterval;
    
    @ConfigProperty("fastqueue.flush.timeIntervalMs")
    private int flushTimeIntervalMs;
    
    @ConfigProperty("fastqueue.flush.forceMetadata")
    private boolean flushForceMetadata;
    
    @ConfigProperty("fastqueue.flush.enablePageCache")
    private boolean flushEnablePageCache;
    
    // Logging settings
    @ConfigProperty("fastqueue.logging.level")
    private String logLevel;
    
    @ConfigProperty("fastqueue.logging.file")
    private String logFile;
    
    /**
     * Creates a configuration with default values.
     * These will be overridden by @ConfigProperty annotations when loaded via ConfigLoader.
     */
    public QueueConfig() {
        // Set default values - will be overridden by @ConfigProperty annotations
        this.dataDirectory = Paths.get(DEFAULT_DATA_DIR);
        this.maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
        this.retentionPeriodMs = DEFAULT_RETENTION_PERIOD_MS;
        this.indexInterval = DEFAULT_INDEX_INTERVAL;
        this.maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        this.serverPort = DEFAULT_SERVER_PORT;
        this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        this.enableCompression = false;
        this.enableMetrics = true;
        this.flushIntervalMs = 1000;
        this.maxConcurrentConnections = 1000;
        this.enableBatching = true;
        this.batchSize = 100;
        this.flushStrategy = "hybrid";
        this.flushMessageInterval = 1000;
        this.flushTimeIntervalMs = 1000;
        this.flushForceMetadata = true;
        this.flushEnablePageCache = true;
        this.logLevel = DEFAULT_LOG_LEVEL;
        this.logFile = DEFAULT_LOG_FILE;
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
    
    public String getLogLevel() {
        return logLevel;
    }
    
    public String getLogFile() {
        return logFile;
    }
    
    // Flush strategy getters
    public String getFlushStrategy() {
        return flushStrategy;
    }
    
    public int getFlushMessageInterval() {
        return flushMessageInterval;
    }
    
    public int getFlushTimeIntervalMs() {
        return flushTimeIntervalMs;
    }
    
    public boolean isFlushForceMetadata() {
        return flushForceMetadata;
    }
    
    public boolean isFlushEnablePageCache() {
        return flushEnablePageCache;
    }
    
    /**
     * Creates a FlushConfiguration from the current settings.
     * 
     * @return FlushConfiguration instance
     */
    public FlushConfiguration getFlushConfiguration() {
        FlushStrategy strategy = FlushStrategy.fromName(flushStrategy);
        return new FlushConfiguration(
            strategy,
            flushMessageInterval,
            flushTimeIntervalMs,
            flushForceMetadata,
            flushEnablePageCache
        );
    }
    
    @Override
    public String toString() {
        return String.format("QueueConfig{dataDir=%s, maxSegmentSize=%d, retentionPeriodMs=%d, " +
                           "indexInterval=%d, maxMessageSize=%d, serverPort=%d, threadPoolSize=%d, " +
                           "compression=%s, metrics=%s, flushIntervalMs=%d, maxConnections=%d, " +
                           "batching=%s, batchSize=%d, flushStrategy=%s, flushMessageInterval=%d, " +
                           "flushTimeIntervalMs=%d, flushForceMetadata=%s, flushEnablePageCache=%s, " +
                           "logLevel=%s, logFile=%s}",
                           dataDirectory, maxSegmentSize, retentionPeriodMs, indexInterval,
                           maxMessageSize, serverPort, threadPoolSize, enableCompression,
                           enableMetrics, flushIntervalMs, maxConcurrentConnections,
                           enableBatching, batchSize, flushStrategy, flushMessageInterval,
                           flushTimeIntervalMs, flushForceMetadata, flushEnablePageCache,
                           logLevel, logFile);
    }
}