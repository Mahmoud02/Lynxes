package org.mahmoud.fastqueue.core;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Handles append-only file operations for storing message records.
 * Provides thread-safe operations with durability guarantees.
 */
public class Store implements Closeable {
    private final Path filePath;
    private final RandomAccessFile file;
    private final ReadWriteLock lock;
    private volatile long currentPosition;
    private volatile boolean closed;

    /**
     * Creates a new store with the specified file path.
     * 
     * @param filePath Path to the store file
     * @throws IOException if the file cannot be created or opened
     */
    public Store(Path filePath) throws IOException {
        this.filePath = filePath;
        this.lock = new ReentrantReadWriteLock();
        
        // Create parent directories if they don't exist
        Files.createDirectories(filePath.getParent());
        
        // Open file in read-write mode, create if it doesn't exist
        this.file = new RandomAccessFile(filePath.toFile(), "rw");
        this.currentPosition = file.length();
        this.closed = false;
    }

    /**
     * Appends a record to the store file.
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
            long position = currentPosition;
            byte[] serializedData = record.serialize();
            
            file.seek(position);
            file.write(serializedData);
            
            // Force data to disk for durability
            file.getFD().sync();
            
            currentPosition += serializedData.length;
            return position;
        } finally {
            lock.writeLock().unlock();
        }
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
            file.seek(position);
            
            // Read the length first
            int dataLength = file.readInt();
            if (dataLength < 0 || dataLength > 1024 * 1024) { // Sanity check: max 1MB
                throw new IOException("Invalid data length: " + dataLength);
            }
            
            // Read the rest of the record
            byte[] serializedData = new byte[4 + 8 + 4 + dataLength]; // Length + Timestamp + Checksum + Data
            file.seek(position); // Reset to beginning
            int bytesRead = file.read(serializedData);
            
            if (bytesRead != serializedData.length) {
                throw new IOException("Incomplete read: expected " + serializedData.length + 
                                   ", got " + bytesRead);
            }
            
            return Record.deserialize(serializedData, offset);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Reads a record from the specified position without deserializing.
     * Useful for index operations that only need the raw data.
     * 
     * @param position The byte position in the file
     * @return The raw serialized data
     * @throws IOException if the read operation fails
     */
    public byte[] readRaw(long position) throws IOException {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }

        lock.readLock().lock();
        try {
            file.seek(position);
            
            // Read the length first
            int dataLength = file.readInt();
            if (dataLength < 0 || dataLength > 1024 * 1024) { // Sanity check: max 1MB
                throw new IOException("Invalid data length: " + dataLength);
            }
            
            // Read the rest of the record
            byte[] serializedData = new byte[4 + 8 + 4 + dataLength]; // Length + Timestamp + Checksum + Data
            file.seek(position); // Reset to beginning
            int bytesRead = file.read(serializedData);
            
            if (bytesRead != serializedData.length) {
                throw new IOException("Incomplete read: expected " + serializedData.length + 
                                   ", got " + bytesRead);
            }
            
            return serializedData;
        } finally {
            lock.readLock().unlock();
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
            return file.length();
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
        return currentPosition;
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
     * Truncates the store file to the specified size.
     * This operation is destructive and should be used with caution.
     * 
     * @param newSize The new size in bytes
     * @throws IOException if the operation fails
     */
    public void truncate(long newSize) throws IOException {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }

        lock.writeLock().lock();
        try {
            file.setLength(newSize);
            currentPosition = Math.min(currentPosition, newSize);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Forces all pending writes to disk.
     * 
     * @throws IOException if the operation fails
     */
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }

        lock.writeLock().lock();
        try {
            file.getFD().sync();
        } finally {
            lock.writeLock().unlock();
        }
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
            file.close();
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
