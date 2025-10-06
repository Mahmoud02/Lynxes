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
        System.out.println("Log constructor: Creating new Log for path: " + basePath);
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
        System.out.println("Log constructor: Log created with " + segments.size() + " segments, nextOffset: " + getNextOffset());
    }

    /**
     * Recovers existing segments from disk.
     * Handles both old segment-N format and new offset-based format.
     * 
     * @throws IOException if recovery fails
     */
    private void recoverSegments() throws IOException {
        Set<String> segmentIds = new HashSet<>();
        
        // Find all segment files (both .store and .log for compatibility)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath, "*.log")) {
            for (Path logFile : stream) {
                String fileName = logFile.getFileName().toString();
                String segmentId = fileName.substring(0, fileName.lastIndexOf('.'));
                segmentIds.add(segmentId);
            }
        }
        
        // Also check for .store files (old format for backward compatibility)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath, "*.store")) {
            for (Path storeFile : stream) {
                String fileName = storeFile.getFileName().toString();
                String segmentId = fileName.substring(0, fileName.lastIndexOf('.'));
                segmentIds.add(segmentId);
            }
        }
        
        // Sort segments by their starting offset for proper recovery order
        List<String> sortedSegmentIds = new ArrayList<>(segmentIds);
        sortedSegmentIds.sort((a, b) -> {
            try {
                // Try to parse as offset-based naming first
                long offsetA = Long.parseLong(a);
                long offsetB = Long.parseLong(b);
                return Long.compare(offsetA, offsetB);
            } catch (NumberFormatException e) {
                // Fall back to old segment-N format
                if (a.startsWith("segment-") && b.startsWith("segment-")) {
                    try {
                        long numA = Long.parseLong(a.substring("segment-".length()));
                        long numB = Long.parseLong(b.substring("segment-".length()));
                        return Long.compare(numA, numB);
                    } catch (NumberFormatException ex) {
                        return a.compareTo(b);
                    }
                }
                return a.compareTo(b);
            }
        });
        
        // Load each segment in order
        for (String segmentId : sortedSegmentIds) {
            try {
                // Determine the starting offset for this segment
                long startOffset = 0;
                try {
                    // Try to parse as offset-based naming
                    startOffset = Long.parseLong(segmentId);
                } catch (NumberFormatException e) {
                    // Fall back to old segment-N format - calculate offset from previous segments
                    startOffset = getNextOffset();
                }
                
                // Create segment with the determined start offset
                Segment segment = new Segment(basePath, segmentId, maxSegmentSize, startOffset);
                
                // Get the actual offset range for this segment
                long lowestOffset = segment.getLowestOffset();
                long highestOffset = segment.getHighestOffset();
                
                System.out.println("Log.recoverSegments: Recovered segment '" + segmentId + 
                                 "' with offset range [" + lowestOffset + ", " + highestOffset + "]");
                
                if (lowestOffset >= 0 && highestOffset >= 0) {
                    // Update the segment's nextOffset to be highestOffset + 1
                    segment.updateNextOffset(highestOffset + 1);
                } else if (highestOffset >= 0) {
                    // If we can't get lowest offset, just use highest + 1
                    segment.updateNextOffset(highestOffset + 1);
                }
                
                segments.put(segmentId, segment);
                
                // Set as active if it's not full and we don't have an active segment yet
                if (activeSegment == null && !segment.isFull()) {
                    activeSegment = segment;
                    System.out.println("Log.recoverSegments: Set segment '" + segmentId + "' as active");
                }
            } catch (Exception e) {
                System.err.println("Failed to recover segment " + segmentId + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        // If all recovered segments are full, we need to create a new active segment
        if (activeSegment == null) {
            try {
                createNewSegment();
            } catch (IOException e) {
                System.err.println("Failed to create new active segment during recovery: " + e.getMessage());
            }
        }
    }

    /**
     * Creates a new segment and makes it active.
     * Uses Kafka-style offset-based naming: {offset:020d}
     * 
     * @throws IOException if the segment cannot be created
     */
    private void createNewSegment() throws IOException {
        // Get the next available offset from the current log state
        // This ensures offset continuity across segment rotations
        long startOffset = getNextOffset();
        
        // Use Kafka-style naming: 20-digit zero-padded offset
        String segmentId = String.format("%020d", startOffset);
        
        Segment segment = new Segment(basePath, segmentId, maxSegmentSize, startOffset);
        segments.put(segmentId, segment);
        activeSegment = segment;
        
        System.out.println("Log.createNewSegment: Created new segment '" + segmentId + "' starting at offset " + startOffset);
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
            System.out.println("Log.append: Requested offset: " + offset + 
                             ", Active segment: " + (activeSegment != null ? activeSegment.getSegmentId() : "null") +
                             ", Active segment nextOffset: " + (activeSegment != null ? activeSegment.getNextOffset() : "N/A"));
            
            // Check if active segment is full
            if (activeSegment.isFull()) {
                System.out.println("Log.append: Active segment is full, creating new segment");
                // Close current active segment
                activeSegment.close();
                
                // Create new active segment
                createNewSegment();
                System.out.println("Log.append: Created new active segment: " + activeSegment.getSegmentId());
            }
            
            Record result = activeSegment.append(offset, data);
            System.out.println("Log.append: Successfully appended record with offset: " + result.getOffset());
            return result;
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
            System.out.println("Log.getNextOffset: Checking " + segments.size() + " segments");
            
            // Sort segments by their starting offset to ensure proper order
            List<Segment> sortedSegments = new ArrayList<>(segments.values());
            sortedSegments.sort((a, b) -> {
                try {
                    // Try to parse as offset-based naming first
                    long offsetA = Long.parseLong(a.getSegmentId());
                    long offsetB = Long.parseLong(b.getSegmentId());
                    return Long.compare(offsetA, offsetB);
                } catch (NumberFormatException e) {
                    // Fall back to old segment-N format
                    if (a.getSegmentId().startsWith("segment-") && b.getSegmentId().startsWith("segment-")) {
                        try {
                            long numA = Long.parseLong(a.getSegmentId().substring("segment-".length()));
                            long numB = Long.parseLong(b.getSegmentId().substring("segment-".length()));
                            return Long.compare(numA, numB);
                        } catch (NumberFormatException ex) {
                            return a.getSegmentId().compareTo(b.getSegmentId());
                        }
                    }
                    return a.getSegmentId().compareTo(b.getSegmentId());
                }
            });
            
            for (Segment segment : sortedSegments) {
                // Include all segments, not just non-closed ones
                // During recovery, we need to consider all segments to get the correct next offset
                long highestOffset = segment.getHighestOffset();
                long nextOffset = segment.getNextOffset();
                
                System.out.println("Log.getNextOffset: Segment '" + segment.getSegmentId() + 
                                 "' highestOffset: " + highestOffset + ", nextOffset: " + nextOffset + 
                                 ", closed: " + segment.isClosed());
                
                // Use the higher of highestOffset or (nextOffset - 1)
                // nextOffset represents the next available offset, so (nextOffset - 1) is the last used offset
                long segmentMaxOffset = Math.max(highestOffset, nextOffset - 1);
                if (segmentMaxOffset > maxOffset) {
                    maxOffset = segmentMaxOffset;
                }
            }
            
            long result = maxOffset + 1;
            System.out.println("Log.getNextOffset: Max offset found: " + maxOffset + ", returning: " + result);
            return result;
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
