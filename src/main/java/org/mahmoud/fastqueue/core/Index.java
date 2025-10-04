package org.mahmoud.fastqueue.core;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Memory-mapped index for fast lookups of record positions.
 * Maps record offsets to their physical byte positions in the store file.
 */
public class Index implements Closeable {
    private static final int INDEX_ENTRY_SIZE = 24; // 8 (offset) + 8 (position) + 4 (length) + 4 (checksum)
    
    private final Path filePath;
    private final FileChannel channel;
    private final ReadWriteLock lock;
    private volatile MappedByteBuffer buffer;
    private volatile long currentSize;
    private volatile boolean closed;

    /**
     * Creates a new index with the specified file path.
     * 
     * @param filePath Path to the index file
     * @throws IOException if the file cannot be created or opened
     */
    public Index(Path filePath) throws IOException {
        this.filePath = filePath;
        this.lock = new ReentrantReadWriteLock();
        
        // Create parent directories if they don't exist
        Files.createDirectories(filePath.getParent());
        
        // Open file in read-write mode, create if it doesn't exist
        this.channel = FileChannel.open(filePath, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.READ, 
            StandardOpenOption.WRITE);
        
        this.currentSize = channel.size();
        this.buffer = null;
        this.closed = false;
        
        // Map the file to memory if it has content
        if (currentSize > 0) {
            mapToMemory();
        }
    }

    /**
     * Maps the index file to memory for fast access.
     * 
     * @throws IOException if the mapping fails
     */
    private void mapToMemory() throws IOException {
        if (currentSize == 0) {
            return;
        }
        
        // Round up to page size for better performance
        long mapSize = ((currentSize + 4095) / 4096) * 4096;
        
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
        this.buffer.position((int) currentSize);
    }

    /**
     * Ensures the index has enough space for new entries.
     * 
     * @param requiredEntries Number of entries needed
     * @throws IOException if the operation fails
     */
    private void ensureCapacity(int requiredEntries) throws IOException {
        long requiredSize = currentSize + (requiredEntries * INDEX_ENTRY_SIZE);
        
        if (buffer == null || requiredSize > buffer.capacity()) {
            // Unmap current buffer
            if (buffer != null) {
                buffer.force();
                buffer = null;
            }
            
            // Extend file size
            channel.truncate(requiredSize);
            currentSize = requiredSize;
            
            // Remap with new size
            mapToMemory();
        }
    }

    /**
     * Adds an index entry for a record.
     * 
     * @param offset The record offset
     * @param position The byte position in the store file
     * @param length The length of the serialized record
     * @param checksum The record checksum
     * @throws IOException if the operation fails
     */
    public void addEntry(long offset, long position, int length, int checksum) throws IOException {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }

        lock.writeLock().lock();
        try {
            ensureCapacity(1);
            
            buffer.putLong(offset);
            buffer.putLong(position);
            buffer.putInt(length);
            buffer.putInt(checksum);
            
            currentSize += INDEX_ENTRY_SIZE;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Adds an index entry for a record.
     * 
     * @param record The record to index
     * @param position The byte position in the store file
     * @throws IOException if the operation fails
     */
    public void addEntry(Record record, long position) throws IOException {
        addEntry(record.getOffset(), position, record.getSerializedSize(), record.getChecksum());
    }

    /**
     * Finds the position of a record with the given offset.
     * 
     * @param offset The record offset to find
     * @return IndexEntry containing position and length, or null if not found
     * @throws IOException if the operation fails
     */
    public IndexEntry findEntry(long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }

        if (buffer == null || currentSize == 0) {
            return null;
        }

        lock.readLock().lock();
        try {
            // Binary search for the offset
            int left = 0;
            int right = (int) (currentSize / INDEX_ENTRY_SIZE) - 1;
            
            while (left <= right) {
                int mid = (left + right) / 2;
                long midOffset = getOffsetAt(mid);
                
                if (midOffset == offset) {
                    return getEntryAt(mid);
                } else if (midOffset < offset) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }
            
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Finds the entry at the given index position.
     * 
     * @param index The index position (0-based)
     * @return IndexEntry at the position
     */
    private IndexEntry getEntryAt(int index) {
        int position = index * INDEX_ENTRY_SIZE;
        buffer.position(position);
        
        long offset = buffer.getLong();
        long filePosition = buffer.getLong();
        int length = buffer.getInt();
        int checksum = buffer.getInt();
        
        return new IndexEntry(offset, filePosition, length, checksum);
    }

    /**
     * Gets the offset at the given index position.
     * 
     * @param index The index position (0-based)
     * @return The offset at that position
     */
    private long getOffsetAt(int index) {
        int position = index * INDEX_ENTRY_SIZE;
        buffer.position(position);
        return buffer.getLong();
    }

    /**
     * Gets the number of entries in the index.
     * 
     * @return Number of entries
     */
    public int getEntryCount() {
        return (int) (currentSize / INDEX_ENTRY_SIZE);
    }

    /**
     * Gets the total size of the index file.
     * 
     * @return Size in bytes
     */
    public long size() {
        return currentSize;
    }

    /**
     * Checks if the index is empty.
     * 
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return currentSize == 0;
    }

    /**
     * Forces all pending changes to disk.
     * 
     * @throws IOException if the operation fails
     */
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }

        lock.writeLock().lock();
        try {
            if (buffer != null) {
                buffer.force();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the file path of this index.
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
            if (buffer != null) {
                buffer.force();
                buffer = null;
            }
            channel.close();
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Checks if the index is closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Represents an index entry containing offset, position, length, and checksum.
     */
    public static class IndexEntry {
        private final long offset;
        private final long position;
        private final int length;
        private final int checksum;

        public IndexEntry(long offset, long position, int length, int checksum) {
            this.offset = offset;
            this.position = position;
            this.length = length;
            this.checksum = checksum;
        }

        public long getOffset() {
            return offset;
        }

        public long getPosition() {
            return position;
        }

        public int getLength() {
            return length;
        }

        public int getChecksum() {
            return checksum;
        }

        @Override
        public String toString() {
            return String.format("IndexEntry{offset=%d, position=%d, length=%d, checksum=%d}", 
                               offset, position, length, checksum);
        }
    }
}
