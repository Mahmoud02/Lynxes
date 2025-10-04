package org.mahmoud.fastqueue.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A segment combines a store file and an index file.
 * Manages the coordination between storing records and indexing them.
 */
public class Segment implements AutoCloseable {
    private final Store store;
    private final Index index;
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
        
        // Create store and index files
        Path storePath = basePath.resolve(segmentId + ".store");
        Path indexPath = basePath.resolve(segmentId + ".index");
        
        this.store = new Store(storePath);
        this.index = new Index(indexPath);
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
        index.addEntry(record, position);
        
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
        
        Record record = new Record(offset, data);
        
        // Write to store
        long position = store.append(record);
        
        // Add to index
        index.addEntry(record, position);
        
        // Update next offset if necessary
        nextOffset.updateAndGet(current -> Math.max(current, offset + 1));
        
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
        
        Index.IndexEntry entry = index.findEntry(offset);
        if (entry == null) {
            return null;
        }
        
        return store.read(entry.getPosition(), offset);
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
        
        Index.IndexEntry entry = index.findEntry(offset);
        if (entry == null) {
            return null;
        }
        
        return store.readRaw(entry.getPosition());
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
     * 
     * @return Number of records
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
        return index.getFilePath();
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
        index.flush();
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
