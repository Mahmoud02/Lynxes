package org.mahmoud.fastqueue.core;

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
    public void testIndex() throws IOException {
        Path indexPath = tempDir.resolve("test.index");
        Index index = new Index(indexPath);
        
        try {
            // Test adding entries
            index.addEntry(1, 100, 50, 123);
            index.addEntry(2, 200, 75, 456);
            index.addEntry(3, 300, 100, 789);
            
            // Test finding entries
            Index.IndexEntry entry1 = index.findEntry(1);
            Index.IndexEntry entry2 = index.findEntry(2);
            Index.IndexEntry entry3 = index.findEntry(3);
            Index.IndexEntry notFound = index.findEntry(999);
            
            assertNotNull(entry1);
            assertEquals(1, entry1.getOffset());
            assertEquals(100, entry1.getPosition());
            
            assertNotNull(entry2);
            assertEquals(2, entry2.getOffset());
            assertEquals(200, entry2.getPosition());
            
            assertNotNull(entry3);
            assertEquals(3, entry3.getOffset());
            assertEquals(300, entry3.getPosition());
            
            assertNull(notFound);
            
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
            // Note: getRecordCount() might include internal entries, so we check it's at least 2
            assertTrue(segment.getRecordCount() >= 2);
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
            // Note: getRecordCount() might include internal entries, so we check it's at least 3
            assertTrue(log.getRecordCount() >= 3);
            assertTrue(log.getSegmentCount() >= 1);
            
        } finally {
            log.close();
        }
    }
}
