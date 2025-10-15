package org.mahmoud.lynxes.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A segment combines a store file and an index file.
 * Manages the coordination between storing records and indexing them.
 */
public class Segment implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Segment.class);
    
    private final Store store;
    private final SparseIndex index;
    private final long maxSize;
    private final AtomicLong nextOffset;
    private final Path basePath;
    private final String segmentId;
    private volatile boolean closed;

    /**
     * Creates a new segment with the specified base path and maximum size.
     * 
     * @param basePath Base directory for segment files
     * @param segmentId Unique identifier for this segment
     * @param maxSize Maximum size in bytes before rotation
     * @param startOffset Starting offset for records in this segment
     * @throws IOException if the segment cannot be created
     */
    public Segment(Path basePath, String segmentId, long maxSize, long startOffset) throws IOException {
        this.basePath = basePath;
        this.segmentId = segmentId;
        this.maxSize = maxSize;
        this.nextOffset = new AtomicLong(startOffset);
        this.closed = false;
        
        // Create store and index files (using Kafka-style .log extension)
        Path storePath = basePath.resolve(segmentId + ".log");
        Path indexPath = basePath.resolve(segmentId + ".index");
        
        this.store = new Store(storePath);
        this.index = new SparseIndex(indexPath);
    }

    /**
     * Appends a record to this segment.
     * 
     * @param data The message data to append
     * @return The record with assigned offset
     * @throws IOException if the operation fails
     * @throws IllegalStateException if the segment is full or closed
     */
    public Record append(byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Segment is closed");
        }
        
        if (isFull()) {
            throw new IllegalStateException("Segment is full");
        }
        
        long offset = nextOffset.getAndIncrement();
        Record record = new Record(offset, data);
        
        // Write to store
        long position = store.append(record);
        
        // Add to index
        index.addEntry(record.getOffset(), position, record.getData().length, record.getChecksum());
        
        return record;
    }

    /**
     * Appends a record with a specific offset to this segment.
     * 
     * @param offset The specific offset to use
     * @param data The message data to append
     * @return The record with the specified offset
     * @throws IOException if the operation fails
     * @throws IllegalStateException if the segment is full or closed
     */
    public Record append(long offset, byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Segment is closed");
        }
        
        if (isFull()) {
            throw new IllegalStateException("Segment is full");
        }
        
        logger.debug("Segment.append: Segment '{}' - Requested offset: {}, Current nextOffset: {}", 
                    segmentId, offset, nextOffset.get());
        
        Record record = new Record(offset, data);
        
        // Write to store
        long position = store.append(record);
        logger.debug("Segment.append: Segment '{}' - Written to store at position: {}", 
                    segmentId, position);
        
        // Add to index
        index.addEntry(record.getOffset(), position, record.getData().length, record.getChecksum());
        logger.debug("Segment.append: Segment '{}' - Added to index", segmentId);
        
        // Update next offset if necessary
        long oldNextOffset = nextOffset.get();
        nextOffset.updateAndGet(current -> Math.max(current, offset + 1));
        logger.debug("Segment.append: Segment '{}' - Updated nextOffset from {} to {}", 
                    segmentId, oldNextOffset, nextOffset.get());
        
        return record;
    }

    /**
     * Reads a record by its offset.
     * 
     * @param offset The record offset
     * @return The record, or null if not found
     * @throws IOException if the operation fails
     */
    public Record read(long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Segment is closed");
        }
        
        // Use sparse index to find the closest indexed entry
        IndexEntry closestEntry = index.findClosestIndex(offset);
        if (closestEntry == null) {
            return null;
        }
        
        // If we found the exact offset, return it directly
        if (closestEntry.getOffset() == offset) {
            return store.read(closestEntry.getPosition(), offset);
        }
        
        // Otherwise, we need to scan forward from the closest entry
        // This is the "linear scan" part of sparse indexing
        return scanForwardFromOffset(closestEntry, offset);
    }

    /**
     * Reads a record by its offset without deserializing.
     * 
     * @param offset The record offset
     * @return The raw serialized data, or null if not found
     * @throws IOException if the operation fails
     */
    public byte[] readRaw(long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Segment is closed");
        }
        
        // Use sparse index to find the closest indexed entry
        IndexEntry closestEntry = index.findClosestIndex(offset);
        if (closestEntry == null) {
            return null;
        }
        
        // If we found the exact offset, return it directly
        if (closestEntry.getOffset() == offset) {
            return store.readRaw(closestEntry.getPosition());
        }
        
        // Otherwise, we need to scan forward from the closest entry
        // This is the "linear scan" part of sparse indexing
        Record record = scanForwardFromOffset(closestEntry, offset);
        return record != null ? record.getData() : null;
    }

    /**
     * Scans forward from a given index entry to find a specific offset.
     * This implements the "linear scan" part of sparse indexing.
     * 
     * @param startEntry The index entry to start scanning from
     * @param targetOffset The offset to find
     * @return The record with the target offset, or null if not found
     * @throws IOException if the operation fails
     */
    private Record scanForwardFromOffset(IndexEntry startEntry, long targetOffset) throws IOException {
        logger.debug("Segment.scanForwardFromOffset: Scanning from offset {} to find {}", 
                    startEntry.getOffset(), targetOffset);
        
        // If the start entry is exactly the target offset, return it directly
        if (startEntry.getOffset() == targetOffset) {
            logger.debug("Segment.scanForwardFromOffset: Start entry matches target offset");
            return store.read(startEntry.getPosition(), targetOffset);
        }
        
        // Start scanning from the position after the indexed entry
        // Total record size = 4 (length) + 8 (timestamp) + 4 (checksum) + data_length = 16 + data_length
        long currentPosition = startEntry.getPosition() + 16 + startEntry.getLength();
        long currentOffset = startEntry.getOffset() + 1;
        
        logger.debug("Segment.scanForwardFromOffset: Starting scan from position {}, expected offset {}, target offset {}", 
                    currentPosition, currentOffset, targetOffset);
        
        // Scan forward until we find the target offset or go past it
        while (currentOffset <= targetOffset) {
            try {
                // Read the next record
                Record record = store.read(currentPosition, currentOffset);
                if (record == null) {
                    logger.debug("Segment.scanForwardFromOffset: No more records, target not found");
                    return null;
                }
                
                logger.debug("Segment.scanForwardFromOffset: Read record with offset {} at position {}", 
                            record.getOffset(), currentPosition);
                
                if (record.getOffset() == targetOffset) {
                    logger.debug("Segment.scanForwardFromOffset: Found target offset {}", targetOffset);
                    return record;
                }
                
                // If we've gone past the target offset, it doesn't exist
                if (record.getOffset() > targetOffset) {
                    logger.debug("Segment.scanForwardFromOffset: Past target offset {}, not found", targetOffset);
                    return null;
                }
                
                // Move to the next position
                // Total record size = 4 (length) + 8 (timestamp) + 4 (checksum) + data_length = 16 + data_length
                currentPosition += 16 + record.getData().length;
                currentOffset = record.getOffset() + 1;
                
            } catch (Exception e) {
                logger.error("Segment.scanForwardFromOffset: Error scanning at position {}: {}", 
                           currentPosition, e.getMessage(), e);
                return null;
            }
        }
        
        return null;
    }

    /**
     * Checks if this segment is full.
     * 
     * @return true if full, false otherwise
     * @throws IOException if the operation fails
     */
    public boolean isFull() throws IOException {
        return store.size() >= maxSize;
    }

    /**
     * Checks if this segment is empty.
     * 
     * @return true if empty, false otherwise
     * @throws IOException if the operation fails
     */
    public boolean isEmpty() throws IOException {
        return store.isEmpty();
    }

    /**
     * Gets the current size of this segment.
     * 
     * @return Size in bytes
     * @throws IOException if the operation fails
     */
    public long size() throws IOException {
        return store.size();
    }

    /**
     * Gets the number of records in this segment.
     * Note: This returns the number of indexed records (sparse), not total records.
     * 
     * @return Number of indexed records
     */
    public int getRecordCount() {
        return index.getEntryCount();
    }

    /**
     * Gets the next available offset.
     * 
     * @return Next offset
     */
    public long getNextOffset() {
        return nextOffset.get();
    }

    /**
     * Gets the highest offset in this segment.
     * Note: This returns the highest indexed offset (sparse), not necessarily the highest record offset.
     * 
     * @return The highest indexed offset, or -1 if the segment is empty
     */
    public long getHighestOffset() {
        return index.getHighestOffset();
    }

    /**
     * Gets the lowest offset in this segment.
     * Note: This returns the lowest indexed offset (sparse), not necessarily the lowest record offset.
     * 
     * @return The lowest indexed offset, or -1 if the segment is empty
     */
    public long getLowestOffset() {
        // For sparse index, the lowest offset is always 0 (first indexed entry)
        return index.getEntryCount() > 0 ? 0 : -1;
    }

    /**
     * Updates the next offset for this segment.
     * This is used during recovery to set the correct next offset.
     * 
     * @param newNextOffset The new next offset value
     */
    public void updateNextOffset(long newNextOffset) {
        nextOffset.set(newNextOffset);
    }

    /**
     * Gets the segment ID.
     * 
     * @return Segment ID
     */
    public String getSegmentId() {
        return segmentId;
    }

    /**
     * Gets the base path of this segment.
     * 
     * @return Base path
     */
    public Path getBasePath() {
        return basePath;
    }

    /**
     * Gets the store file path.
     * 
     * @return Store file path
     */
    public Path getStorePath() {
        return store.getFilePath();
    }

    /**
     * Gets the index file path.
     * 
     * @return Index file path
     */
    public Path getIndexPath() {
        return basePath.resolve(segmentId + ".index");
    }

    /**
     * Forces all pending changes to disk.
     * 
     * @throws IOException if the operation fails
     */
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Segment is closed");
        }
        
        store.flush();
        // SparseIndex handles its own flushing in addEntry()
        // No need to call index.flush() as it's not needed
    }

    /**
     * Checks if this segment is closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        try {
            flush();
        } finally {
            store.close();
            index.close();
            closed = true;
        }
    }

    @Override
    public String toString() {
        try {
            return String.format("Segment{id='%s', size=%d, records=%d, nextOffset=%d, closed=%s}", 
                               segmentId, size(), getRecordCount(), nextOffset.get(), closed);
        } catch (IOException e) {
            return String.format("Segment{id='%s', size=?, records=%d, nextOffset=%d, closed=%s}", 
                               segmentId, getRecordCount(), nextOffset.get(), closed);
        }
    }
}
