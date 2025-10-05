package org.mahmoud.fastqueue.core;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * High-performance store implementation with configurable flush strategies.
 * : page cache + configurable durability.
 * 
 * Key features:
 * - Configurable flush strategies (immediate, batched, time-based, hybrid)
 * - Page cache optimization for performance
 * - Thread-safe operations
 * - Memory-mapped file support for hot reads
 */
public class OptimizedStore implements Closeable {
    private final Path filePath;
    private final FileChannel fileChannel;
    private final ReadWriteLock lock;
    private final AtomicLong currentPosition;
    private final FlushConfiguration flushConfig;
    private volatile boolean closed;
    
    // Flush tracking
    private final AtomicLong messageCount = new AtomicLong(0);
    private volatile long lastFlushTime = System.currentTimeMillis();
    
    // Background flush thread for time-based flushing
    private volatile Thread backgroundFlushThread;
    private volatile boolean shouldStopBackgroundFlush = false;

    /**
     * Creates an optimized store with the specified configuration.
     * 
     * @param filePath Path to the store file
     * @param flushConfig Flush configuration
     * @throws IOException if the file cannot be created or opened
     */
    public OptimizedStore(Path filePath, FlushConfiguration flushConfig) throws IOException {
        this.filePath = filePath;
        this.flushConfig = flushConfig;
        this.lock = new ReentrantReadWriteLock();
        
        // Create parent directories if they don't exist
        Files.createDirectories(filePath.getParent());
        
        // Open file with FileChannel for better performance
        this.fileChannel = FileChannel.open(filePath, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.READ, 
            StandardOpenOption.WRITE);
        
        this.currentPosition = new AtomicLong(fileChannel.size());
        this.closed = false;
        
        // Initialize flush tracking
        this.lastFlushTime = System.currentTimeMillis();
        
        // Start background flush thread if time-based flushing is enabled
        if (flushConfig.usesTimeInterval()) {
            startBackgroundFlushThread();
        }
    }

    /**
     * Starts the background flush thread for time-based flushing.
     */
    private void startBackgroundFlushThread() {
        backgroundFlushThread = new Thread(() -> {
            while (!shouldStopBackgroundFlush && !closed) {
                try {
                    // Check if we need to flush based on time
                    long currentTime = System.currentTimeMillis();
                    if (flushConfig.shouldFlushOnTime(currentTime - lastFlushTime) && messageCount.get() > 0) {
                        lock.writeLock().lock();
                        try {
                            if (!closed && messageCount.get() > 0) {
                                performFlush();
                            }
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                    
                    // Sleep for a short interval to avoid busy waiting
                    Thread.sleep(Math.min(flushConfig.getTimeIntervalMs() / 4, 100));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Log error but continue running
                    System.err.println("Error in background flush thread: " + e.getMessage());
                }
            }
        }, "OptimizedStore-BackgroundFlush");
        
        backgroundFlushThread.setDaemon(true);
        backgroundFlushThread.start();
    }

    /**
     * Appends a record to the store using the configured flush strategy.
     * 
     * @param record The record to append
     * @return The position where the record was written
     * @throws IOException if the write operation fails
     */
    public long append(Record record) throws IOException {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }

        lock.writeLock().lock();
        try {
            long position = currentPosition.get();
            byte[] serializedData = record.serialize();
            
            // Write data to file channel
            fileChannel.position(position);
            ByteBuffer buffer = ByteBuffer.wrap(serializedData);
            fileChannel.write(buffer);
            
            // Handle page cache setting
            if (!flushConfig.isEnablePageCache()) {
                // Force immediate flush to disk (minimize page cache usage)
                fileChannel.force(false);
            }
            
            // Update position and message count
            currentPosition.addAndGet(serializedData.length);
            long currentMessageCount = messageCount.incrementAndGet();
            
            // Check if we should flush based on the configured strategy
            if (shouldFlush(currentMessageCount)) {
                performFlush();
            }
            
            return position;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Determines if a flush should be performed based on the configured strategy.
     */
    private boolean shouldFlush(long currentMessageCount) {
        // Immediate flush strategy
        if (flushConfig.requiresImmediateFlush()) {
            return true;
        }
        
        // Message-based flush check
        if (flushConfig.shouldFlushOnMessageCount((int) currentMessageCount)) {
            return true;
        }
        
        // Time-based flush check
        long currentTime = System.currentTimeMillis();
        if (flushConfig.shouldFlushOnTime(currentTime - lastFlushTime)) {
            return true;
        }
        
        return false;
    }

    /**
     * Performs the actual flush operation based on configuration.
     */
    private void performFlush() throws IOException {
        // Only flush if page cache is enabled (otherwise we flush on every write)
        if (flushConfig.isEnablePageCache()) {
            if (flushConfig.isForceMetadata()) {
                // Force both data and metadata to disk (more durable)
                fileChannel.force(true);
            } else {
                // Force only data to disk (faster)
                fileChannel.force(false);
            }
        }
        
        // Update flush tracking
        lastFlushTime = System.currentTimeMillis();
        messageCount.set(0); // Reset message count after flush
    }

    /**
     * Reads a record from the specified position.
     * 
     * @param position The byte position in the file
     * @param offset The logical offset for the record
     * @return The deserialized record
     * @throws IOException if the read operation fails
     */
    public Record read(long position, long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }

        lock.readLock().lock();
        try {
            fileChannel.position(position);
            
            // Read the length first
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            fileChannel.read(lengthBuffer);
            lengthBuffer.flip();
            int dataLength = lengthBuffer.getInt();
            
            if (dataLength < 0 || dataLength > 1024 * 1024) { // Sanity check: max 1MB
                throw new IOException("Invalid data length: " + dataLength);
            }
            
            // Read the rest of the record
            byte[] serializedData = new byte[4 + 8 + 4 + dataLength]; // Length + Timestamp + Checksum + Data
            fileChannel.position(position); // Reset to beginning
            ByteBuffer dataBuffer = ByteBuffer.wrap(serializedData);
            fileChannel.read(dataBuffer);
            
            return Record.deserialize(serializedData, offset);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Forces an immediate flush to disk.
     * Useful for critical operations or shutdown procedures.
     * 
     * @throws IOException if the operation fails
     */
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }

        lock.writeLock().lock();
        try {
            performFlush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Forces both data and metadata to disk.
     * Most durable but slowest option.
     * 
     * @throws IOException if the operation fails
     */
    public void force() throws IOException {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }

        lock.writeLock().lock();
        try {
            fileChannel.force(true);
            lastFlushTime = System.currentTimeMillis();
            messageCount.set(0);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the current size of the store file.
     * 
     * @return Size in bytes
     * @throws IOException if the operation fails
     */
    public long size() throws IOException {
        lock.readLock().lock();
        try {
            return fileChannel.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the current write position.
     * 
     * @return Current position in bytes
     */
    public long getCurrentPosition() {
        return currentPosition.get();
    }

    /**
     * Gets the number of messages since last flush.
     * 
     * @return Message count
     */
    public long getMessageCountSinceFlush() {
        return messageCount.get();
    }

    /**
     * Gets the time since last flush in milliseconds.
     * 
     * @return Time in milliseconds
     */
    public long getTimeSinceLastFlush() {
        return System.currentTimeMillis() - lastFlushTime;
    }

    /**
     * Gets the flush configuration.
     * 
     * @return Flush configuration
     */
    public FlushConfiguration getFlushConfiguration() {
        return flushConfig;
    }

    /**
     * Gets performance statistics.
     * 
     * @return Performance stats as string
     */
    public String getPerformanceStats() {
        return String.format("OptimizedStore{file=%s, size=%d, messagesSinceFlush=%d, timeSinceFlush=%dms, config=%s}",
                           filePath.getFileName(), 
                           currentPosition.get(),
                           messageCount.get(),
                           getTimeSinceLastFlush(),
                           flushConfig);
    }

    /**
     * Checks if the store is empty.
     * 
     * @return true if empty, false otherwise
     * @throws IOException if the operation fails
     */
    public boolean isEmpty() throws IOException {
        return size() == 0;
    }

    /**
     * Gets the file path of this store.
     * 
     * @return The file path
     */
    public Path getFilePath() {
        return filePath;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        lock.writeLock().lock();
        try {
            // Stop background flush thread
            shouldStopBackgroundFlush = true;
            if (backgroundFlushThread != null) {
                backgroundFlushThread.interrupt();
                try {
                    backgroundFlushThread.join(1000); // Wait up to 1 second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            // Final flush before closing
            performFlush();
            
            fileChannel.close();
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if the store is closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }
}
