package org.mahmoud.lynxes.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Unit tests for core data structures.
 */
public class CoreDataStructuresTest {

    @TempDir
    Path tempDir;

    @Test
    public void testRecord() {
        // Test record creation
        byte[] data = "Hello, FastQueue2!".getBytes();
        Record record = new Record(1, data);
        
        assertEquals(1, record.getOffset());
        assertArrayEquals(data, record.getData());
        assertTrue(record.isValid());
        
        // Test serialization/deserialization
        byte[] serialized = record.serialize();
        Record deserialized = Record.deserialize(serialized, 1);
        
        assertEquals(1, deserialized.getOffset());
        assertArrayEquals(data, deserialized.getData());
        assertTrue(deserialized.isValid());
    }

    @Test
    public void testStore() throws IOException {
        Path storePath = tempDir.resolve("test.store");
        Store store = new Store(storePath);
        
        try {
            // Test appending records
            byte[] data1 = "Message 1".getBytes();
            byte[] data2 = "Message 2".getBytes();
            
            Record record1 = new Record(1, data1);
            Record record2 = new Record(2, data2);
            
            long pos1 = store.append(record1);
            long pos2 = store.append(record2);
            
            assertEquals(0, pos1); // First record starts at position 0
            assertTrue(pos2 > pos1); // Second record comes after first
            
            // Test reading records
            Record readRecord1 = store.read(pos1, 1);
            Record readRecord2 = store.read(pos2, 2);
            
            assertEquals(1, readRecord1.getOffset());
            assertArrayEquals(data1, readRecord1.getData());
            assertEquals(2, readRecord2.getOffset());
            assertArrayEquals(data2, readRecord2.getData());
            
        } finally {
            store.close();
        }
    }

    @Test
    public void testSparseIndex() throws IOException {
        Path indexPath = tempDir.resolve("test.index");
        SparseIndex index = new SparseIndex(indexPath);
        
        try {
            // Test adding entries (only sparse intervals will be indexed)
            index.addEntry(0, 0, 50, 123);    // Will be indexed (offset 0)
            index.addEntry(1, 100, 50, 123);  // Will be skipped
            index.addEntry(2, 200, 75, 456);  // Will be skipped
            index.addEntry(1000, 300, 100, 789); // Will be indexed (offset 1000)
            
            // Test finding entries
            IndexEntry entry0 = index.findClosestIndex(0);
            IndexEntry entry1 = index.findClosestIndex(1);
            IndexEntry entry2 = index.findClosestIndex(2);
            IndexEntry entry1000 = index.findClosestIndex(1000);
            IndexEntry notFound = index.findClosestIndex(999);
            
            assertNotNull(entry0);
            assertEquals(0, entry0.getOffset());
            assertEquals(0, entry0.getPosition());
            
            // For non-indexed offsets, should find the closest indexed entry
            assertNotNull(entry1);
            assertEquals(0, entry1.getOffset()); // Should find offset 0
            
            assertNotNull(entry2);
            assertEquals(0, entry2.getOffset()); // Should find offset 0
            
            assertNotNull(entry1000);
            assertEquals(1000, entry1000.getOffset());
            assertEquals(300, entry1000.getPosition());
            
            // For offset 999, should find the closest indexed entry (offset 0)
            assertNotNull(notFound);
            assertEquals(0, notFound.getOffset()); // Should find offset 0 as closest
            
        } finally {
            index.close();
        }
    }

    @Test
    public void testSegment() throws IOException {
        // Use unique segment name to avoid conflicts
        String segmentName = "test-segment-" + System.currentTimeMillis();
        Segment segment = new Segment(tempDir, segmentName, 1024, 0);
        
        try {
            // Test appending records
            byte[] data1 = "Segment Message 1".getBytes();
            byte[] data2 = "Segment Message 2".getBytes();
            
            Record record1 = segment.append(data1);
            Record record2 = segment.append(data2);
            
            assertEquals(0, record1.getOffset());
            assertEquals(1, record2.getOffset());
            assertArrayEquals(data1, record1.getData());
            assertArrayEquals(data2, record2.getData());
            
            // Test reading records
            Record readRecord1 = segment.read(0);
            Record readRecord2 = segment.read(1);
            
            assertNotNull(readRecord1);
            assertEquals(0, readRecord1.getOffset());
            assertNotNull(readRecord2);
            assertEquals(1, readRecord2.getOffset());
            
            // Test segment properties
            // Note: getRecordCount() returns indexed records (sparse), not total records
            // With sparse interval 1000, only offset 0 gets indexed, so count should be 1
            assertTrue(segment.getRecordCount() >= 1); // At least 1 indexed record
            assertFalse(segment.isEmpty());
            
        } finally {
            segment.close();
        }
    }

    @Test
    public void testLog() throws IOException {
        // Use unique subdirectory to avoid conflicts
        Path logDir = tempDir.resolve("log-" + System.currentTimeMillis());
        Files.createDirectories(logDir);
        Log log = new Log(logDir, 1024, 86400000); // 1KB segments, 1 day retention
        
        try {
            // Test appending records
            byte[] data1 = "Log Message 1".getBytes();
            byte[] data2 = "Log Message 2".getBytes();
            byte[] data3 = "Log Message 3".getBytes();
            
            Record record1 = log.append(data1);
            Record record2 = log.append(data2);
            Record record3 = log.append(data3);
            
            assertEquals(0, record1.getOffset());
            assertEquals(1, record2.getOffset());
            assertEquals(2, record3.getOffset());
            
            // Test reading records
            Record readRecord1 = log.read(0);
            Record readRecord2 = log.read(1);
            Record readRecord3 = log.read(2);
            
            assertNotNull(readRecord1);
            assertEquals(0, readRecord1.getOffset());
            assertNotNull(readRecord2);
            assertEquals(1, readRecord2.getOffset());
            assertNotNull(readRecord3);
            assertEquals(2, readRecord3.getOffset());
            
            // Test log properties
            // Note: getRecordCount() returns indexed records (sparse), not total records
            // With sparse interval 1000, only offset 0 gets indexed, so count should be 1
            assertTrue(log.getRecordCount() >= 1); // At least 1 indexed record
            assertTrue(log.getSegmentCount() >= 1);
            
        } finally {
            log.close();
        }
    }
}
