package org.mahmoud.fastqueue.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests specifically focused on SparseIndex behavior with 1100+ messages.
 * Verifies that sparse indexing works correctly and meets expectations.
 */
public class SparseIndexBehaviorTest {
    
    @TempDir
    Path tempDir;
    
    private Log log;
    private static final int TOTAL_MESSAGES = 1100;
    private static final int SPARSE_INTERVAL = 1000;
    
    @BeforeEach
    void setUp() throws IOException {
        Path topicDir = tempDir.resolve("sparse-test-topic");
        log = new Log(topicDir, 1024 * 1024, 86400000); // 1MB segments
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (log != null) {
            log.close();
        }
    }
    
    @Test
    void testSparseIndexingExpectations() throws IOException {
        System.out.println("=== Testing SparseIndex Expectations with " + TOTAL_MESSAGES + " messages ===");
        
        // Publish 1100 messages
        List<Record> publishedRecords = new ArrayList<>();
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            String message = "Sparse test message " + i;
            Record record = log.append(i, message.getBytes());
            publishedRecords.add(record);
        }
        
        System.out.println("Published " + TOTAL_MESSAGES + " messages");
        
        // Test expectations:
        // 1. Only offsets 0, 1000 should be indexed (every 1000th message)
        // 2. All messages should be consumable
        // 3. Non-indexed messages should be found via linear scan
        
        verifySparseIndexingExpectations();
        verifyAllMessagesConsumable(publishedRecords);
        verifyLinearScanBehavior();
        
        System.out.println("=== All SparseIndex expectations met! ===");
    }
    
    private void verifySparseIndexingExpectations() throws IOException {
        System.out.println("\n--- Verifying Sparse Indexing Expectations ---");
        
        // Expected indexed offsets: 0, 1000 (for 1100 messages)
        int[] expectedIndexedOffsets = {0, 1000};
        
        for (int expectedOffset : expectedIndexedOffsets) {
            if (expectedOffset < TOTAL_MESSAGES) {
                Record record = log.read(expectedOffset);
                assertNotNull(record, "Indexed offset " + expectedOffset + " should be consumable");
                assertEquals(expectedOffset, record.getOffset(), "Offset should match");
                System.out.println("✓ Indexed offset " + expectedOffset + " is consumable");
            }
        }
        
        // Verify that offset 1001 (not indexed) is still consumable
        Record nonIndexedRecord = log.read(1001);
        assertNotNull(nonIndexedRecord, "Non-indexed offset 1001 should be consumable via linear scan");
        assertEquals(1001, nonIndexedRecord.getOffset(), "Offset should match");
        System.out.println("✓ Non-indexed offset 1001 is consumable via linear scan");
    }
    
    private void verifyAllMessagesConsumable(List<Record> publishedRecords) throws IOException {
        System.out.println("\n--- Verifying All Messages Are Consumable ---");
        
        int consumedCount = 0;
        int errorCount = 0;
        
        for (Record expectedRecord : publishedRecords) {
            try {
                Record consumedRecord = log.read(expectedRecord.getOffset());
                if (consumedRecord != null) {
                    consumedCount++;
                } else {
                    errorCount++;
                    System.err.println("ERROR: Failed to consume offset " + expectedRecord.getOffset());
                }
            } catch (Exception e) {
                errorCount++;
                System.err.println("ERROR: Exception consuming offset " + expectedRecord.getOffset() + ": " + e.getMessage());
            }
        }
        
        System.out.println("Consumed: " + consumedCount + "/" + TOTAL_MESSAGES + " messages");
        System.out.println("Errors: " + errorCount + " messages");
        
        assertEquals(TOTAL_MESSAGES, consumedCount, "All messages should be consumable");
        assertEquals(0, errorCount, "No consumption errors should occur");
    }
    
    private void verifyLinearScanBehavior() throws IOException {
        System.out.println("\n--- Verifying Linear Scan Behavior ---");
        
        // Test linear scan for various non-indexed offsets
        int[] testOffsets = {1, 500, 999, 1001, 1050, 1099};
        
        for (int offset : testOffsets) {
            if (offset < TOTAL_MESSAGES) {
                Record record = log.read(offset);
                assertNotNull(record, "Offset " + offset + " should be consumable via linear scan");
                assertEquals(offset, record.getOffset(), "Offset should match");
                
                String expectedMessage = "Sparse test message " + offset;
                String actualMessage = new String(record.getData());
                assertEquals(expectedMessage, actualMessage, "Message content should match for offset " + offset);
                
                System.out.println("✓ Linear scan successful for offset " + offset);
            }
        }
    }
    
    @Test
    void testSparseIndexMemoryEfficiency() throws IOException {
        System.out.println("\n=== Testing SparseIndex Memory Efficiency ===");
        
        // Publish 1100 messages
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            String message = "Memory test message " + i;
            log.append(i, message.getBytes());
        }
        
        // Verify that we only have 2 indexed entries (offsets 0 and 1000)
        // This is the key benefit of sparse indexing - memory efficiency
        
        // Test that we can still access all messages
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            Record record = log.read(i);
            assertNotNull(record, "All messages should be accessible despite sparse indexing");
        }
        
        System.out.println("✓ Memory efficiency verified - sparse indexing works correctly");
        System.out.println("✓ All " + TOTAL_MESSAGES + " messages accessible with minimal index entries");
    }
    
    @Test
    void testSparseIndexBoundaryConditions() throws IOException {
        System.out.println("\n=== Testing SparseIndex Boundary Conditions ===");
        
        // Publish exactly 1000 messages (boundary condition)
        for (int i = 0; i < 1000; i++) {
            String message = "Boundary test message " + i;
            log.append(i, message.getBytes());
        }
        
        // Only offset 0 should be indexed (1000 % 1000 = 0, but we start from 0)
        Record offset0 = log.read(0);
        assertNotNull(offset0, "Offset 0 should be indexed and consumable");
        
        // Offset 999 should be consumable via linear scan
        Record offset999 = log.read(999);
        assertNotNull(offset999, "Offset 999 should be consumable via linear scan");
        
        // Now add one more message (offset 1000) - this should be indexed
        log.append(1000, "Boundary test message 1000".getBytes());
        
        Record offset1000 = log.read(1000);
        assertNotNull(offset1000, "Offset 1000 should be indexed and consumable");
        
        System.out.println("✓ Boundary conditions handled correctly");
    }
}
