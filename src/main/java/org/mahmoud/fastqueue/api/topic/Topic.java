package org.mahmoud.fastqueue.api.topic;

import org.mahmoud.fastqueue.core.Log;
import org.mahmoud.fastqueue.core.Record;
import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.util.QueueUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a message queue topic.
 * Each topic has its own log and manages message publishing and consumption.
 */
public class Topic {
    private final String name;
    private final Log log;
    private final QueueConfig config;
    private final ReadWriteLock lock;
    private final AtomicLong nextOffset;
    private volatile boolean closed;

    /**
     * Creates a new topic with the specified name and configuration.
     * 
     * @param name The topic name
     * @param config The queue configuration
     * @throws IOException if the topic cannot be created
     */
    public Topic(String name, QueueConfig config) throws IOException {
        this.name = QueueUtils.sanitizeTopicName(name);
        this.config = config;
        this.lock = new ReentrantReadWriteLock();
        this.closed = false;
        
        // Create topic directory
        Path topicDir = config.getDataDirectory().resolve("topics").resolve(this.name);
        QueueUtils.createDirectoryIfNotExists(topicDir);
        
        // Initialize log for this topic
        this.log = new Log(topicDir, config.getMaxSegmentSize(), config.getRetentionPeriodMs());
        
        // Initialize nextOffset based on existing log data
        this.nextOffset = new AtomicLong(this.log.getNextOffset());
    }
    
    /**
     * Creates a new topic with the specified name, configuration, and shared log.
     * This constructor is used by the TopicRegistry to ensure data sharing.
     * 
     * @param name The topic name
     * @param config The queue configuration
     * @param log The shared log instance
     */
    Topic(String name, QueueConfig config, Log log) {
        this.name = name;
        this.config = config;
        this.log = log;
        this.lock = new ReentrantReadWriteLock();
        // Initialize nextOffset based on existing log data
        this.nextOffset = new AtomicLong(log.getNextOffset());
        this.closed = false;
    }

    /**
     * Publishes a message to this topic.
     * 
     * @param data The message data
     * @return The published record
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the topic is closed
     */
    public Record publish(byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Topic is closed");
        }
        
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Message data cannot be null or empty");
        }
        
        if (data.length > config.getMaxMessageSize()) {
            throw new IllegalArgumentException("Message size exceeds maximum allowed size: " + 
                                             config.getMaxMessageSize());
        }
        
        lock.writeLock().lock();
        try {
            // Get the next offset but don't increment yet
            long offset = nextOffset.get();
            
            // Try to append the record first
            Record record = log.append(offset, data);
            
            // Only increment nextOffset after successful append
            nextOffset.incrementAndGet();
            
            // Note: We don't force immediate flush here - let the flush strategy handle it
            // This maintains page cache benefits while ensuring data consistency
            
            return record;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Publishes a message with a specific offset.
     * 
     * @param offset The specific offset to use
     * @param data The message data
     * @return The published record
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the topic is closed
     */
    public Record publish(long offset, byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Topic is closed");
        }
        
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Message data cannot be null or empty");
        }
        
        if (data.length > config.getMaxMessageSize()) {
            throw new IllegalArgumentException("Message size exceeds maximum allowed size: " + 
                                             config.getMaxMessageSize());
        }
        
        lock.writeLock().lock();
        try {
            return log.append(offset, data);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Publishes a message with configurable durability guarantees.
     * 
     * @param data The message data
     * @param requireDurability If true, forces immediate flush to disk (slower but more durable)
     * @return The published record
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the topic is closed
     */
    public Record publish(byte[] data, boolean requireDurability) throws IOException {
        if (closed) {
            throw new IllegalStateException("Topic is closed");
        }
        
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Message data cannot be null or empty");
        }
        
        if (data.length > config.getMaxMessageSize()) {
            throw new IllegalArgumentException("Message size exceeds maximum allowed size: " + 
                                             config.getMaxMessageSize());
        }
        
        lock.writeLock().lock();
        try {
            // Get the next offset but don't increment yet
            long offset = nextOffset.get();
            
            // Try to append the record first
            Record record = log.append(offset, data);
            
            // Only increment nextOffset after successful append
            nextOffset.incrementAndGet();
            
            // Apply durability requirements
            if (requireDurability) {
                // Force immediate flush for high-durability requirements
                log.flush();
            }
            // Otherwise, rely on the configured flush strategy for performance
            
            return record;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Consumes a message by offset.
     * 
     * @param offset The message offset
     * @return The message record, or null if not found
     * @throws IOException if consumption fails
     * @throws IllegalStateException if the topic is closed
     */
    public Record consume(long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Topic is closed");
        }
        
        lock.readLock().lock();
        try {
            return log.read(offset);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Consumes a message by offset without deserializing.
     * 
     * @param offset The message offset
     * @return The raw message data, or null if not found
     * @throws IOException if consumption fails
     * @throws IllegalStateException if the topic is closed
     */
    public byte[] consumeRaw(long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Topic is closed");
        }
        
        lock.readLock().lock();
        try {
            return log.readRaw(offset);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the next available offset.
     * 
     * @return The next offset
     */
    public long getNextOffset() {
        return nextOffset.get();
    }

    /**
     * Gets the total number of messages in this topic.
     * 
     * @return The message count
     */
    public long getMessageCount() {
        lock.readLock().lock();
        try {
            return log.getRecordCount();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the total size of this topic.
     * 
     * @return The size in bytes
     * @throws IOException if the operation fails
     */
    public long getSize() throws IOException {
        lock.readLock().lock();
        try {
            return log.getTotalSize();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the number of segments in this topic.
     * 
     * @return The segment count
     */
    public int getSegmentCount() {
        lock.readLock().lock();
        try {
            return log.getSegmentCount();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the topic name.
     * 
     * @return The topic name
     */
    public String getName() {
        return name;
    }

    /**
     * Checks if the topic is empty.
     * 
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return getMessageCount() == 0;
    }

    /**
     * Forces all pending changes to disk.
     * 
     * @throws IOException if the operation fails
     */
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Topic is closed");
        }
        
        lock.writeLock().lock();
        try {
            log.flush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Truncates old segments based on retention policy.
     * 
     * @throws IOException if the operation fails
     */
    public void truncate() throws IOException {
        if (closed) {
            throw new IllegalStateException("Topic is closed");
        }
        
        lock.writeLock().lock();
        try {
            log.truncate();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if the topic is closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes the topic and releases resources.
     * 
     * @throws IOException if the operation fails
     */
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        lock.writeLock().lock();
        try {
            log.close();
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        try {
            return String.format("Topic{name='%s', messageCount=%d, size=%s, segments=%d, closed=%s}", 
                               name, getMessageCount(), QueueUtils.formatBytes(getSize()), 
                               getSegmentCount(), closed);
        } catch (IOException e) {
            return String.format("Topic{name='%s', messageCount=%d, size=?, segments=%d, closed=%s}", 
                               name, getMessageCount(), getSegmentCount(), closed);
        }
    }
}
