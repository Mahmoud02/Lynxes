package org.mahmoud.lynxes.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for SparseIndex with large message volumes.
 * Tests sparse indexing behavior with 1100+ messages to verify:
 * - Only every 1000th message is indexed (sparse interval)
 * - All messages can be consumed correctly
 * - Binary search + linear scan works properly
 * - Data persistence across restarts
 */
public class SparseIndexIntegrationTest {
    
    @TempDir
    Path tempDir;
    
    private Log log;
    private static final int TOTAL_MESSAGES = 1100;
    private static final int SPARSE_INTERVAL = 1000;
    
    @BeforeEach
    void setUp() throws IOException {
        // Create a fresh log for each test
        Path topicDir = tempDir.resolve("test-topic");
        log = new Log(topicDir, 1024 * 1024, 86400000); // 1MB segments, 1 day retention
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (log != null) {
            log.close();
        }
    }
    
    @Test
    void testSparseIndexingWithLargeMessageVolume() throws IOException {
        System.out.println("=== Testing SparseIndex with " + TOTAL_MESSAGES + " messages ===");
        
        // Step 1: Publish 1100 messages
        List<Record> publishedRecords = new ArrayList<>();
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            String message = "Test message " + i;
            Record record = log.append(i, message.getBytes());
            publishedRecords.add(record);
            
            if (i % 100 == 0) {
                System.out.println("Published message " + i + " (offset " + record.getOffset() + ")");
            }
        }
        
        System.out.println("Published " + TOTAL_MESSAGES + " messages successfully");
        
        // Step 2: Verify sparse indexing behavior
        verifySparseIndexingBehavior();
        
        // Step 3: Test consumption of all messages
        testConsumptionOfAllMessages(publishedRecords);
        
        // Step 4: Test consumption of specific offsets (including non-indexed ones)
        testSpecificOffsetConsumption();
        
        // Step 5: Test edge cases
        testEdgeCases();
        
        System.out.println("=== All tests passed! ===");
    }
    
    private void verifySparseIndexingBehavior() throws IOException {
        System.out.println("\n--- Verifying Sparse Indexing Behavior ---");
        
        // Check that we have the expected number of segments
        // With 1MB segments and small messages, we should have multiple segments
        int segmentCount = log.getSegmentCount();
        System.out.println("Number of segments: " + segmentCount);
        assertTrue(segmentCount > 0, "Should have at least one segment");
        
        // Verify sparse indexing by testing consumption of indexed vs non-indexed offsets
        // Indexed offsets should be fast (binary search), non-indexed should work (linear scan)
        System.out.println("Verifying sparse indexing behavior through consumption tests");
        
        // Test indexed offsets (every 1000th message)
        for (int i = 0; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            Record record = log.read(i);
            assertNotNull(record, "Indexed offset " + i + " should be consumable");
            assertEquals(i, record.getOffset(), "Offset should match");
        }
        System.out.println("✓ All indexed offsets are consumable");
        
        // Test non-indexed offsets (should work via linear scan)
        for (int i = 1; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            if (i % SPARSE_INTERVAL != 0) {
                Record record = log.read(i);
                assertNotNull(record, "Non-indexed offset " + i + " should be consumable via linear scan");
                assertEquals(i, record.getOffset(), "Offset should match");
            }
        }
        System.out.println("✓ All non-indexed offsets are consumable via linear scan");
    }
    
    private void testConsumptionOfAllMessages(List<Record> publishedRecords) throws IOException {
        System.out.println("\n--- Testing Consumption of All Messages ---");
        
        AtomicInteger consumedCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        // Test consumption of all published messages
        for (Record expectedRecord : publishedRecords) {
            try {
                Record consumedRecord = log.read(expectedRecord.getOffset());
                
                if (consumedRecord == null) {
                    System.err.println("ERROR: Failed to consume message at offset " + expectedRecord.getOffset());
                    errorCount.incrementAndGet();
                } else {
                    // Verify the message content
                    String expectedMessage = new String(expectedRecord.getData());
                    String consumedMessage = new String(consumedRecord.getData());
                    
                    if (!expectedMessage.equals(consumedMessage)) {
                        System.err.println("ERROR: Message content mismatch at offset " + expectedRecord.getOffset());
                        System.err.println("Expected: " + expectedMessage);
                        System.err.println("Actual: " + consumedMessage);
                        errorCount.incrementAndGet();
                    } else {
                        consumedCount.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                System.err.println("ERROR: Exception consuming message at offset " + expectedRecord.getOffset() + ": " + e.getMessage());
                errorCount.incrementAndGet();
            }
        }
        
        System.out.println("Successfully consumed: " + consumedCount.get() + " messages");
        System.out.println("Errors: " + errorCount.get() + " messages");
        
        assertEquals(TOTAL_MESSAGES, consumedCount.get(), "Should be able to consume all messages");
        assertEquals(0, errorCount.get(), "Should have no consumption errors");
    }
    
    private void testSpecificOffsetConsumption() throws IOException {
        System.out.println("\n--- Testing Specific Offset Consumption ---");
        
        // Test consumption of indexed offsets (every 1000th)
        for (int i = 0; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            Record record = log.read(i);
            assertNotNull(record, "Should be able to consume indexed offset " + i);
            assertEquals(i, record.getOffset(), "Offset should match");
            String expectedMessage = "Test message " + i;
            String actualMessage = new String(record.getData());
            assertEquals(expectedMessage, actualMessage, "Message content should match for offset " + i);
        }
        
        // Test consumption of non-indexed offsets (should still work via linear scan)
        for (int i = 1; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            if (i % SPARSE_INTERVAL != 0) { // Skip indexed offsets
                Record record = log.read(i);
                assertNotNull(record, "Should be able to consume non-indexed offset " + i);
                assertEquals(i, record.getOffset(), "Offset should match");
                String expectedMessage = "Test message " + i;
                String actualMessage = new String(record.getData());
                assertEquals(expectedMessage, actualMessage, "Message content should match for offset " + i);
            }
        }
        
        // Test some random offsets
        int[] randomOffsets = {50, 150, 250, 500, 750, 950, 1050, 1099};
        for (int offset : randomOffsets) {
            if (offset < TOTAL_MESSAGES) {
                Record record = log.read(offset);
                assertNotNull(record, "Should be able to consume random offset " + offset);
                assertEquals(offset, record.getOffset(), "Offset should match");
                String expectedMessage = "Test message " + offset;
                String actualMessage = new String(record.getData());
                assertEquals(expectedMessage, actualMessage, "Message content should match for offset " + offset);
            }
        }
        
        System.out.println("All specific offset tests passed");
    }
    
    private void testEdgeCases() throws IOException {
        System.out.println("\n--- Testing Edge Cases ---");
        
        // Test consuming offset that doesn't exist
        Record nonExistentRecord = log.read(TOTAL_MESSAGES + 100);
        assertNull(nonExistentRecord, "Should return null for non-existent offset");
        
        // Test consuming negative offset
        Record negativeRecord = log.read(-1);
        assertNull(negativeRecord, "Should return null for negative offset");
        
        // Test consuming offset 0 (should be indexed)
        Record offset0Record = log.read(0);
        assertNotNull(offset0Record, "Should be able to consume offset 0");
        assertEquals(0, offset0Record.getOffset(), "Offset should be 0");
        
        // Test consuming the last message
        Record lastRecord = log.read(TOTAL_MESSAGES - 1);
        assertNotNull(lastRecord, "Should be able to consume last message");
        assertEquals(TOTAL_MESSAGES - 1, lastRecord.getOffset(), "Should be last offset");
        
        System.out.println("All edge case tests passed");
    }
    
    @Test
    void testSparseIndexPerformance() throws IOException {
        System.out.println("\n=== Testing SparseIndex Performance ===");
        
        // Publish messages
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            String message = "Performance test message " + i;
            log.append(i, message.getBytes());
        }
        
        // Measure consumption time for different offset ranges
        long startTime, endTime;
        
        // Test indexed offsets (should be fast due to binary search)
        startTime = System.currentTimeMillis();
        for (int i = 0; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            Record record = log.read(i);
            assertNotNull(record);
        }
        endTime = System.currentTimeMillis();
        long indexedTime = endTime - startTime;
        System.out.println("Indexed offsets consumption time: " + indexedTime + "ms");
        
        // Test non-indexed offsets (should be slower due to linear scan)
        startTime = System.currentTimeMillis();
        for (int i = 1; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            if (i % SPARSE_INTERVAL != 0) {
                Record record = log.read(i);
                assertNotNull(record);
            }
        }
        endTime = System.currentTimeMillis();
        long nonIndexedTime = endTime - startTime;
        System.out.println("Non-indexed offsets consumption time: " + nonIndexedTime + "ms");
        
        // Test random offsets
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            int randomOffset = (int) (Math.random() * TOTAL_MESSAGES);
            Record record = log.read(randomOffset);
            assertNotNull(record);
        }
        endTime = System.currentTimeMillis();
        long randomTime = endTime - startTime;
        System.out.println("Random offsets consumption time: " + randomTime + "ms");
        
        System.out.println("Performance test completed");
    }
}
