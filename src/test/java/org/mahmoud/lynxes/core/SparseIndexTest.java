package org.mahmoud.lynxes.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Test class for SparseIndex implementation
 */
public class SparseIndexTest {
    
    private Path testDir;
    private Path indexPath;
    private SparseIndex sparseIndex;
    
    @BeforeEach
    void setUp() throws IOException {
        // Create test directory
        testDir = Files.createTempDirectory("sparse-index-test");
        indexPath = testDir.resolve("test.index");
        
        // Create new SparseIndex
        sparseIndex = new SparseIndex(indexPath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (sparseIndex != null && !sparseIndex.isClosed()) {
            sparseIndex.close();
        }
        
        // Clean up test directory
        if (Files.exists(testDir)) {
            Files.walk(testDir)
                .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
        }
    }
    
    @Test
    void testSparseIndexing() throws IOException {
        // Test that only sparse intervals are indexed
        System.out.println("Testing sparse indexing...");
        
        // Add entries for offsets 0, 1, 2, ..., 999, 1000, 1001, ..., 1999, 2000
        for (long offset = 0; offset <= 2000; offset++) {
            long position = offset * 100; // Simulate position
            int length = 50; // Simulate length
            int checksum = (int) (offset * 123); // Simulate checksum
            
            sparseIndex.addEntry(offset, position, length, checksum);
        }
        
        // Verify that only sparse intervals were indexed
        int expectedIndexedCount = 3; // 0, 1000, 2000
        assertEquals(expectedIndexedCount, sparseIndex.getEntryCount(), 
                   "Should have indexed only sparse intervals");
        
        // Verify highest offset
        assertEquals(2000, sparseIndex.getHighestOffset(), 
                   "Highest offset should be 2000");
        
        // Test finding closest index
        IndexEntry closest = sparseIndex.findClosestIndex(1500);
        assertNotNull(closest, "Should find closest index for offset 1500");
        assertEquals(1000, closest.getOffset(), 
                   "Closest index for 1500 should be 1000");
        
        // Test finding exact index
        IndexEntry exact = sparseIndex.findClosestIndex(1000);
        assertNotNull(exact, "Should find exact index for offset 1000");
        assertEquals(1000, exact.getOffset(), 
                   "Exact index for 1000 should be 1000");
        
        // Test finding index for offset 0
        IndexEntry zero = sparseIndex.findClosestIndex(0);
        assertNotNull(zero, "Should find index for offset 0");
        assertEquals(0, zero.getOffset(), 
                   "Index for 0 should be 0");
        
        System.out.println("Sparse indexing test passed!");
    }
    
    @Test
    void testBinarySearch() throws IOException {
        // Test binary search functionality
        System.out.println("Testing binary search...");
        
        // Add entries at sparse intervals: 0, 1000, 2000, 3000, 4000
        for (long offset = 0; offset <= 4000; offset += 1000) {
            long position = offset * 100;
            int length = 50;
            int checksum = (int) (offset * 123);
            
            sparseIndex.addEntry(offset, position, length, checksum);
        }
        
        // Test various lookups
        assertEquals(0, sparseIndex.findClosestIndex(0).getOffset(), 
                   "Should find exact match for 0");
        assertEquals(0, sparseIndex.findClosestIndex(500).getOffset(), 
                   "Should find 0 for 500");
        assertEquals(1000, sparseIndex.findClosestIndex(1000).getOffset(), 
                   "Should find exact match for 1000");
        assertEquals(1000, sparseIndex.findClosestIndex(1500).getOffset(), 
                   "Should find 1000 for 1500");
        assertEquals(2000, sparseIndex.findClosestIndex(2000).getOffset(), 
                   "Should find exact match for 2000");
        assertEquals(2000, sparseIndex.findClosestIndex(2500).getOffset(), 
                   "Should find 2000 for 2500");
        assertEquals(4000, sparseIndex.findClosestIndex(4000).getOffset(), 
                   "Should find exact match for 4000");
        assertEquals(4000, sparseIndex.findClosestIndex(4500).getOffset(), 
                   "Should find 4000 for 4500");
        
        // Test non-existent offset
        assertNull(sparseIndex.findClosestIndex(-1), 
                  "Should return null for negative offset");
        
        System.out.println("Binary search test passed!");
    }
    
    @Test
    void testRecovery() throws IOException {
        // Test index recovery from disk
        System.out.println("Testing index recovery...");
        
        // Add some entries
        for (long offset = 0; offset <= 3000; offset += 1000) {
            long position = offset * 100;
            int length = 50;
            int checksum = (int) (offset * 123);
            
            sparseIndex.addEntry(offset, position, length, checksum);
        }
        
        // Close and reopen
        sparseIndex.close();
        sparseIndex = new SparseIndex(indexPath);
        
        // Verify recovery
        assertEquals(4, sparseIndex.getEntryCount(), 
                   "Should recover 4 entries");
        assertEquals(3000, sparseIndex.getHighestOffset(), 
                   "Should recover highest offset 3000");
        
        // Verify entries are still accessible
        IndexEntry entry = sparseIndex.findClosestIndex(1500);
        assertNotNull(entry, "Should find entry after recovery");
        assertEquals(1000, entry.getOffset(), 
                   "Should find correct entry after recovery");
        
        System.out.println("Recovery test passed!");
    }
    
    @Test
    void testPerformance() throws IOException {
        // Test performance with many entries
        System.out.println("Testing performance...");
        
        long startTime = System.currentTimeMillis();
        
        // Add many entries (only sparse ones will be indexed)
        for (long offset = 0; offset < 100000; offset++) {
            long position = offset * 100;
            int length = 50;
            int checksum = (int) (offset * 123);
            
            sparseIndex.addEntry(offset, position, length, checksum);
        }
        
        long addTime = System.currentTimeMillis() - startTime;
        System.out.println("Added 100,000 entries in " + addTime + "ms");
        
        // Verify only sparse entries were indexed
        int expectedCount = 100; // 0, 1000, 2000, ..., 99000
        assertEquals(expectedCount, sparseIndex.getEntryCount(), 
                   "Should have indexed only sparse entries");
        
        // Test lookup performance
        startTime = System.currentTimeMillis();
        
        Random random = new Random(42);
        for (int i = 0; i < 1000; i++) {
            long targetOffset = random.nextLong() % 100000;
            if (targetOffset < 0) targetOffset = -targetOffset;
            
            IndexEntry entry = sparseIndex.findClosestIndex(targetOffset);
            assertNotNull(entry, "Should find entry for offset " + targetOffset);
        }
        
        long lookupTime = System.currentTimeMillis() - startTime;
        System.out.println("Performed 1,000 lookups in " + lookupTime + "ms");
        
        System.out.println("Performance test passed!");
    }
    
    @Test
    void testEdgeCases() throws IOException {
        // Test edge cases
        System.out.println("Testing edge cases...");
        
        // Test empty index
        assertNull(sparseIndex.findClosestIndex(0), 
                  "Should return null for empty index");
        assertEquals(0, sparseIndex.getEntryCount(), 
                   "Empty index should have 0 entries");
        assertEquals(-1, sparseIndex.getHighestOffset(), 
                   "Empty index should have -1 highest offset");
        
        // Test single entry
        sparseIndex.addEntry(0, 0, 50, 123);
        assertEquals(1, sparseIndex.getEntryCount(), 
                   "Should have 1 entry after adding one");
        assertEquals(0, sparseIndex.getHighestOffset(), 
                   "Highest offset should be 0");
        
        IndexEntry entry = sparseIndex.findClosestIndex(0);
        assertNotNull(entry, "Should find entry for offset 0");
        assertEquals(0, entry.getOffset(), 
                   "Entry offset should be 0");
        
        // Test negative offset lookup
        assertNull(sparseIndex.findClosestIndex(-1), 
                  "Should return null for negative offset");
        
        System.out.println("Edge cases test passed!");
    }
}
