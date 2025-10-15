package org.mahmoud.lynxes.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparseIndex implements a sparse indexing mechanism similar to Kafka's approach.
 * Instead of indexing every message, it indexes only every N messages (sparse interval),
 * providing memory efficiency while maintaining fast lookups through binary search + linear scan.
 * 
 * Key benefits:
 * - Memory efficient: Only indexes every 1000th message by default
 * - Fast lookups: O(log n + k) where k is typically small
 * - Reliable: Uses FileChannel instead of memory mapping
 * - Kafka-compatible: Similar to Kafka's sparse index approach
 */
public class SparseIndex {
    private static final Logger logger = LoggerFactory.getLogger(SparseIndex.class);
    
    // Index entry size: offset (8) + position (8) + length (4) + checksum (4) = 24 bytes
    private static final int INDEX_ENTRY_SIZE = 24;
    
    // Sparse interval: index every N messages (default 1000)
    private static final int SPARSE_INTERVAL = 1000;
    
    // File channel for reliable disk operations
    private FileChannel channel;
    private Path indexPath;
    private ReadWriteLock lock;
    private boolean closed = false;
    
    // Current state
    private long currentSize = 0;
    private long lastIndexedOffset = -1;
    private int entryCount = 0;
    
    public SparseIndex(Path indexPath) throws IOException {
        this.indexPath = indexPath;
        this.lock = new ReentrantReadWriteLock();
        
        // Create index file if it doesn't exist
        if (!Files.exists(indexPath)) {
            Files.createFile(indexPath);
        }
        
        // Open file channel
        this.channel = FileChannel.open(indexPath, 
            StandardOpenOption.READ, 
            StandardOpenOption.WRITE, 
            StandardOpenOption.CREATE);
        
        // Recover existing index entries
        recoverIndex();
        
        logger.debug("SparseIndex: Created for {}, entryCount={}, lastIndexedOffset={}", 
                    indexPath, entryCount, lastIndexedOffset);
    }
    
    /**
     * Recover existing index entries from disk
     */
    private void recoverIndex() throws IOException {
        long fileSize = channel.size();
        if (fileSize == 0) {
            return;
        }
        
        logger.debug("SparseIndex.recoverIndex: Recovering from file size={}", fileSize);
        
        // Read all index entries
        ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
        channel.position(0);
        channel.read(buffer);
        buffer.flip();
        
        // Parse entries
        while (buffer.remaining() >= INDEX_ENTRY_SIZE) {
            long offset = buffer.getLong();
            long position = buffer.getLong();
            int length = buffer.getInt();
            int checksum = buffer.getInt();
            
            if (offset >= 0 && position >= 0 && length >= 0) {
                entryCount++;
                lastIndexedOffset = Math.max(lastIndexedOffset, offset);
                currentSize += INDEX_ENTRY_SIZE;
                
                logger.debug("SparseIndex.recoverIndex: Recovered entry {}, offset={}, position={}, length={}, checksum={}", 
                            entryCount, offset, position, length, checksum);
            } else {
                break;
            }
        }
        
        logger.debug("SparseIndex.recoverIndex: Recovered {} entries, lastIndexedOffset={}", 
                    entryCount, lastIndexedOffset);
    }
    
    /**
     * Add an index entry if it should be indexed (based on sparse interval)
     */
    public void addEntry(long offset, long position, int length, int checksum) throws IOException {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }
        
        lock.writeLock().lock();
        try {
            // Only index if this offset should be indexed based on sparse interval
            if (shouldIndex(offset)) {
                    logger.debug("SparseIndex.addEntry: Indexing offset={}, position={}, length={}, checksum={} (sparse interval={})", 
                                 offset, position, length, checksum, SPARSE_INTERVAL);
                
                // Write entry to end of file
                ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
                buffer.putLong(offset);
                buffer.putLong(position);
                buffer.putInt(length);
                buffer.putInt(checksum);
                buffer.flip();
                
                // Write to end of file
                channel.position(currentSize);
                channel.write(buffer);
                channel.force(true); // Force to disk
                
                currentSize += INDEX_ENTRY_SIZE;
                entryCount++;
                lastIndexedOffset = offset;
                
                    logger.debug("SparseIndex.addEntry: Added entry {}, currentSize={}, lastIndexedOffset={}", 
                                 entryCount, currentSize, lastIndexedOffset);
            } else {
                logger.debug("SparseIndex.addEntry: Skipping offset={} (not a sparse interval, lastIndexed={})", 
                            offset, lastIndexedOffset);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Check if an offset should be indexed based on sparse interval
     */
    private boolean shouldIndex(long offset) {
        // Index if:
        // 1. This is the first entry (offset 0)
        // 2. This offset is a multiple of SPARSE_INTERVAL
        // 3. This is the last message in a segment
        return offset == 0 || 
               offset % SPARSE_INTERVAL == 0 || 
               (lastIndexedOffset >= 0 && offset > lastIndexedOffset + SPARSE_INTERVAL);
    }
    
    /**
     * Find the closest index entry for a given offset
     * Returns the largest indexed offset <= targetOffset
     */
    public IndexEntry findClosestIndex(long targetOffset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }
        
        lock.readLock().lock();
        try {
            if (entryCount == 0) {
                return null;
            }
            
        logger.debug("SparseIndex.findClosestIndex: Looking for targetOffset={}, entryCount={}", 
                    targetOffset, entryCount);
            
            // Binary search for the closest index entry
            int left = 0;
            int right = entryCount - 1;
            IndexEntry result = null;
            
            while (left <= right) {
                int mid = (left + right) / 2;
                IndexEntry midEntry = getEntryAt(mid);
                
                if (midEntry.getOffset() <= targetOffset) {
                    result = midEntry;
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }
            
            logger.debug("SparseIndex.findClosestIndex: Found closest entry={}", 
                        result != null ? result.getOffset() : "null");
            
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the number of index entries
     */
    public int getEntryCount() {
        return entryCount;
    }
    
    /**
     * Get the highest indexed offset
     */
    public long getHighestOffset() {
        return lastIndexedOffset;
    }
    
    /**
     * Get an index entry at a specific index
     */
    private IndexEntry getEntryAt(int index) throws IOException {
        int position = index * INDEX_ENTRY_SIZE;
        
        ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        channel.position(position);
        channel.read(buffer);
        buffer.flip();
        
        long offset = buffer.getLong();
        long filePosition = buffer.getLong();
        int length = buffer.getInt();
        int checksum = buffer.getInt();
        
        return new IndexEntry(offset, filePosition, length, checksum);
    }
    
    /**
     * Close the index
     */
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        lock.writeLock().lock();
        try {
            if (channel != null) {
                channel.force(true); // Final flush
                channel.close();
            }
            closed = true;
            logger.debug("SparseIndex.close: Closed index for {}", indexPath);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Check if the index is closed
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * Get the current size of the index file
     */
    public long getCurrentSize() {
        return currentSize;
    }
    
    /**
     * Get the sparse interval
     */
    public int getSparseInterval() {
        return SPARSE_INTERVAL;
    }
}
