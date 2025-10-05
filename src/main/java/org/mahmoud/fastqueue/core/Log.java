package org.mahmoud.fastqueue.core;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages a collection of segments, providing the highest-level abstraction.
 * Handles segment rotation, recovery, and routing of reads/writes.
 */
public class Log implements AutoCloseable {
    private final Path basePath;
    private final long maxSegmentSize;
    private final long retentionPeriodMs;
    private final ReadWriteLock lock;
    private final Map<String, Segment> segments;
    private volatile Segment activeSegment;
    private volatile long nextSegmentId;
    private volatile boolean closed;

    /**
     * Creates a new log with the specified configuration.
     * 
     * @param basePath Base directory for log files
     * @param maxSegmentSize Maximum size per segment in bytes
     * @param retentionPeriodMs How long to keep old segments in milliseconds
     * @throws IOException if the log cannot be initialized
     */
    public Log(Path basePath, long maxSegmentSize, long retentionPeriodMs) throws IOException {
        this.basePath = basePath;
        this.maxSegmentSize = maxSegmentSize;
        this.retentionPeriodMs = retentionPeriodMs;
        this.lock = new ReentrantReadWriteLock();
        this.segments = new HashMap<>();
        this.closed = false;
        
        // Create base directory if it doesn't exist
        Files.createDirectories(basePath);
        
        // Recover existing segments
        recoverSegments();
        
        // Create active segment if none exists
        if (activeSegment == null) {
            createNewSegment();
        }
    }

    /**
     * Recovers existing segments from disk.
     * 
     * @throws IOException if recovery fails
     */
    private void recoverSegments() throws IOException {
        Set<String> segmentIds = new HashSet<>();
        
        // Find all segment files
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath, "*.store")) {
            for (Path storeFile : stream) {
                String fileName = storeFile.getFileName().toString();
                String segmentId = fileName.substring(0, fileName.lastIndexOf('.'));
                segmentIds.add(segmentId);
            }
        }
        
        // Load each segment
        for (String segmentId : segmentIds) {
            try {
                Segment segment = new Segment(basePath, segmentId, maxSegmentSize, 0);
                segments.put(segmentId, segment);
                
                // Find the highest segment ID
                long segmentNum = Long.parseLong(segmentId.substring("segment-".length()));
                nextSegmentId = Math.max(nextSegmentId, segmentNum + 1);
                
                // Set as active if it's not full
                if (activeSegment == null && !segment.isFull()) {
                    activeSegment = segment;
                }
            } catch (Exception e) {
                System.err.println("Failed to recover segment " + segmentId + ": " + e.getMessage());
            }
        }
        
        // After recovering all segments, update their nextOffset values
        for (Segment segment : segments.values()) {
            if (!segment.isClosed()) {
                long highestOffset = segment.getHighestOffset();
                if (highestOffset >= 0) {
                    // Update the segment's nextOffset to be highestOffset + 1
                    segment.updateNextOffset(highestOffset + 1);
                }
            }
        }
    }

    /**
     * Creates a new segment and makes it active.
     * 
     * @throws IOException if the segment cannot be created
     */
    private void createNewSegment() throws IOException {
        String segmentId = "segment-" + nextSegmentId++;
        Segment segment = new Segment(basePath, segmentId, maxSegmentSize, 0);
        segments.put(segmentId, segment);
        activeSegment = segment;
    }

    /**
     * Appends a record to the log.
     * 
     * @param data The message data to append
     * @return The record with assigned offset
     * @throws IOException if the operation fails
     */
    public Record append(byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Log is closed");
        }
        
        lock.writeLock().lock();
        try {
            // Check if active segment is full
            if (activeSegment.isFull()) {
                // Close current active segment
                activeSegment.close();
                
                // Create new active segment
                createNewSegment();
            }
            
            return activeSegment.append(data);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Appends a record with a specific offset to the log.
     * 
     * @param offset The specific offset to use
     * @param data The message data to append
     * @return The record with the specified offset
     * @throws IOException if the operation fails
     */
    public Record append(long offset, byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Log is closed");
        }
        
        lock.writeLock().lock();
        try {
            // Check if active segment is full
            if (activeSegment.isFull()) {
                // Close current active segment
                activeSegment.close();
                
                // Create new active segment
                createNewSegment();
            }
            
            return activeSegment.append(offset, data);
        } finally {
            lock.writeLock().unlock();
        }
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
            throw new IllegalStateException("Log is closed");
        }
        
        lock.readLock().lock();
        try {
            // Search through segments to find the one containing this offset
            for (Segment segment : segments.values()) {
                if (segment.isClosed()) {
                    continue;
                }
                
                Record record = segment.read(offset);
                if (record != null) {
                    return record;
                }
            }
            
            return null;
        } finally {
            lock.readLock().unlock();
        }
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
            throw new IllegalStateException("Log is closed");
        }
        
        lock.readLock().lock();
        try {
            // Search through segments to find the one containing this offset
            for (Segment segment : segments.values()) {
                if (segment.isClosed()) {
                    continue;
                }
                
                byte[] data = segment.readRaw(offset);
                if (data != null) {
                    return data;
                }
            }
            
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the total number of records in the log.
     * 
     * @return Total record count
     */
    public long getRecordCount() {
        lock.readLock().lock();
        try {
            return segments.values().stream()
                .mapToInt(Segment::getRecordCount)
                .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the total size of the log.
     * 
     * @return Total size in bytes
     * @throws IOException if the operation fails
     */
    public long getTotalSize() throws IOException {
        lock.readLock().lock();
        try {
            return segments.values().stream()
                .mapToLong(segment -> {
                    try {
                        return segment.size();
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the number of segments in the log.
     * 
     * @return Number of segments
     */
    public int getSegmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the next available offset for this log.
     * This is the highest offset + 1 across all segments.
     * 
     * @return Next available offset
     */
    public long getNextOffset() {
        lock.readLock().lock();
        try {
            long maxOffset = -1;
            for (Segment segment : segments.values()) {
                if (!segment.isClosed()) {
                    long highestOffset = segment.getHighestOffset();
                    if (highestOffset > maxOffset) {
                        maxOffset = highestOffset;
                    }
                }
            }
            return maxOffset + 1;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the active segment.
     * 
     * @return The active segment
     */
    public Segment getActiveSegment() {
        return activeSegment;
    }

    /**
     * Truncates old segments based on retention policy.
     * 
     * @throws IOException if the operation fails
     */
    public void truncate() throws IOException {
        if (closed) {
            throw new IllegalStateException("Log is closed");
        }
        
        lock.writeLock().lock();
        try {
            long cutoffTime = System.currentTimeMillis() - retentionPeriodMs;
            List<String> segmentsToRemove = new ArrayList<>();
            
            for (Map.Entry<String, Segment> entry : segments.entrySet()) {
                Segment segment = entry.getValue();
                
                // Skip active segment
                if (segment == activeSegment) {
                    continue;
                }
                
                // Check if segment is old enough to be removed
                try {
                    if (segment.isEmpty() || isSegmentOld(segment, cutoffTime)) {
                        segmentsToRemove.add(entry.getKey());
                    }
                } catch (IOException e) {
                    System.err.println("Error checking segment " + entry.getKey() + ": " + e.getMessage());
                }
            }
            
            // Remove old segments
            for (String segmentId : segmentsToRemove) {
                Segment segment = segments.remove(segmentId);
                if (segment != null) {
                    try {
                        segment.close();
                        deleteSegmentFiles(segmentId);
                    } catch (IOException e) {
                        System.err.println("Error removing segment " + segmentId + ": " + e.getMessage());
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if a segment is old enough to be removed.
     * 
     * @param segment The segment to check
     * @param cutoffTime The cutoff time in milliseconds
     * @return true if the segment is old enough
     * @throws IOException if the operation fails
     */
    private boolean isSegmentOld(Segment segment, long cutoffTime) throws IOException {
        // For simplicity, we'll use the segment's file modification time
        // In a real implementation, you might want to track creation time in metadata
        Path storePath = segment.getStorePath();
        if (Files.exists(storePath)) {
            long lastModified = Files.getLastModifiedTime(storePath).toMillis();
            return lastModified < cutoffTime;
        }
        return true; // Remove if file doesn't exist
    }

    /**
     * Deletes the files for a segment.
     * 
     * @param segmentId The segment ID
     * @throws IOException if the operation fails
     */
    private void deleteSegmentFiles(String segmentId) throws IOException {
        Path storePath = basePath.resolve(segmentId + ".store");
        Path indexPath = basePath.resolve(segmentId + ".index");
        
        Files.deleteIfExists(storePath);
        Files.deleteIfExists(indexPath);
    }

    /**
     * Forces all pending changes to disk.
     * 
     * @throws IOException if the operation fails
     */
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Log is closed");
        }
        
        lock.readLock().lock();
        try {
            for (Segment segment : segments.values()) {
                if (!segment.isClosed()) {
                    segment.flush();
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if the log is closed.
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
        
        lock.writeLock().lock();
        try {
            for (Segment segment : segments.values()) {
                segment.close();
            }
            segments.clear();
            activeSegment = null;
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        try {
            return String.format("Log{segments=%d, totalSize=%d, recordCount=%d, closed=%s}", 
                               getSegmentCount(), getTotalSize(), getRecordCount(), closed);
        } catch (IOException e) {
            return String.format("Log{segments=%d, totalSize=?, recordCount=%d, closed=%s}", 
                               getSegmentCount(), getRecordCount(), closed);
        }
    }
}
