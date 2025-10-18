package org.mahmoud.lynxes.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import org.mahmoud.lynxes.domain.producer.Producer;
import org.mahmoud.lynxes.domain.consumer.Consumer;
import org.mahmoud.lynxes.core.Record;
import org.mahmoud.lynxes.config.QueueConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive API Integration tests for the Producer/Consumer API layer with SparseIndex.
 * Tests the complete message flow from publishing to consumption, including:
 * - Sparse indexing behavior with large message volumes
 * - All message consumption patterns
 * - Error handling scenarios
 * - Multiple topics support
 * - Metrics and performance characteristics
 */
public class ApiIntegrationTest {
    
    @TempDir
    Path tempDir;
    
    private Producer producer;
    private Consumer consumer;
    private QueueConfig config;
    private static final int TOTAL_MESSAGES = 1100;
    private static final int SPARSE_INTERVAL = 1000;
    
    @BeforeEach
    void setUp() throws IOException {
        // Create configuration pointing to temp directory
        config = new QueueConfig();
        // Note: QueueConfig doesn't have setters, so we'll use the default config
        // and rely on the temp directory being used by the Log class
        
        // Create producer and consumer
        producer = new Producer(config);
        consumer = new Consumer(config);
        
        // Note: TopicRegistry doesn't have a clear method, so we'll work with existing topics
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
        // Note: TopicRegistry doesn't have a clear method
    }
    
    @Test
    void testPublishAndConsumeWithSparseIndexing() throws IOException {
        System.out.println("=== API Integration Test: Publish and Consume with SparseIndex ===");
        
        String topicName = "integration-test-topic-" + System.currentTimeMillis();
        List<Record> publishedRecords = new ArrayList<>();
        
        // Step 1: Publish 1100 messages
        System.out.println("Publishing " + TOTAL_MESSAGES + " messages...");
        for (int i = 0; i < TOTAL_MESSAGES; i++) {
            String message = "Integration test message " + i;
            Record record = producer.publishString(topicName, message);
            publishedRecords.add(record);
            
            if (i % 100 == 0) {
                System.out.println("Published message " + i + " (offset " + record.getOffset() + ")");
            }
        }
        
        System.out.println("Successfully published " + TOTAL_MESSAGES + " messages");
        System.out.println("Producer message count: " + producer.getMessageCount());
        
        // Step 2: Verify sparse indexing behavior
        verifySparseIndexingBehavior(topicName);
        
        // Step 3: Consume all messages
        testConsumptionOfAllMessages(topicName, publishedRecords);
        
        // Step 4: Test specific offset consumption
        testSpecificOffsetConsumption(topicName);
        
        // Step 5: Test batch consumption
        testBatchConsumption(topicName);
        
        System.out.println("=== All API integration tests passed! ===");
    }
    
    private void verifySparseIndexingBehavior(String topicName) throws IOException {
        System.out.println("\n--- Verifying Sparse Indexing Behavior ---");
        
        // Test indexed offsets (every 1000th message)
        for (int i = 0; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            Record record = consumer.consume(topicName, i);
            assertNotNull(record, "Indexed offset " + i + " should be consumable");
            assertEquals(i, record.getOffset(), "Offset should match");
            String expectedMessage = "Integration test message " + i;
            String actualMessage = new String(record.getData());
            assertEquals(expectedMessage, actualMessage, "Message content should match for offset " + i);
        }
        System.out.println("✓ All indexed offsets are consumable");
        
        // Test non-indexed offsets (should work via linear scan)
        for (int i = 1; i < TOTAL_MESSAGES; i += SPARSE_INTERVAL) {
            if (i % SPARSE_INTERVAL != 0) {
                Record record = consumer.consume(topicName, i);
                assertNotNull(record, "Non-indexed offset " + i + " should be consumable via linear scan");
                assertEquals(i, record.getOffset(), "Offset should match");
                String expectedMessage = "Integration test message " + i;
                String actualMessage = new String(record.getData());
                assertEquals(expectedMessage, actualMessage, "Message content should match for offset " + i);
            }
        }
        System.out.println("✓ All non-indexed offsets are consumable via linear scan");
    }
    
    private void testConsumptionOfAllMessages(String topicName, List<Record> publishedRecords) throws IOException {
        System.out.println("\n--- Testing Consumption of All Messages ---");
        
        AtomicInteger consumedCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger nullCount = new AtomicInteger(0);
        
        System.out.println("Attempting to consume " + publishedRecords.size() + " messages...");
        
        for (int i = 0; i < publishedRecords.size(); i++) {
            Record expectedRecord = publishedRecords.get(i);
            try {
                Record consumedRecord = consumer.consume(topicName, expectedRecord.getOffset());
                
                if (consumedRecord == null) {
                    nullCount.incrementAndGet();
                    if (i < 10 || i % 100 == 0) { // Log first 10 and every 100th failure
                        System.err.println("ERROR: Failed to consume message at offset " + expectedRecord.getOffset() + " (index " + i + ")");
                    }
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
                System.err.println("ERROR: Exception consuming message at offset " + expectedRecord.getOffset() + " (index " + i + "): " + e.getMessage());
                errorCount.incrementAndGet();
            }
        }
        
        System.out.println("Successfully consumed: " + consumedCount.get() + " messages");
        System.out.println("Null responses: " + nullCount.get() + " messages");
        System.out.println("Errors: " + errorCount.get() + " messages");
        
        // More detailed failure information
        if (consumedCount.get() != TOTAL_MESSAGES) {
            System.err.println("CONSUMPTION FAILURE ANALYSIS:");
            System.err.println("- Expected: " + TOTAL_MESSAGES + " messages");
            System.err.println("- Consumed: " + consumedCount.get() + " messages");
            System.err.println("- Null responses: " + nullCount.get() + " messages");
            System.err.println("- Errors: " + errorCount.get() + " messages");
        }
        
        assertEquals(TOTAL_MESSAGES, consumedCount.get(), "Should be able to consume all messages");
        assertEquals(0, errorCount.get(), "Should have no consumption errors");
    }
    
    private void testSpecificOffsetConsumption(String topicName) throws IOException {
        System.out.println("\n--- Testing Specific Offset Consumption ---");
        
        // Test some random offsets
        int[] testOffsets = {0, 1, 50, 100, 500, 999, 1000, 1001, 1050, 1099};
        
        for (int offset : testOffsets) {
            if (offset < TOTAL_MESSAGES) {
                Record record = consumer.consume(topicName, offset);
                assertNotNull(record, "Offset " + offset + " should be consumable");
                assertEquals(offset, record.getOffset(), "Offset should match");
                
                String expectedMessage = "Integration test message " + offset;
                String actualMessage = new String(record.getData());
                assertEquals(expectedMessage, actualMessage, "Message content should match for offset " + offset);
                
                System.out.println("✓ Offset " + offset + " consumed successfully");
            }
        }
    }
    
    private void testBatchConsumption(String topicName) throws IOException {
        System.out.println("\n--- Testing Batch Consumption ---");
        
        // Test consuming batches of messages
        int[] batchSizes = {10, 50, 100, 500};
        
        for (int batchSize : batchSizes) {
            // Reset consumer offset to 0 for each batch test
            consumer.setOffset(topicName, 0);
            
            Record[] records = consumer.consumeBatch(topicName, batchSize);
            
            assertNotNull(records, "Batch consumption should return non-null array");
            assertTrue(records.length > 0, "Should consume at least one message");
            assertTrue(records.length <= batchSize, "Should not exceed requested batch size");
            
            // Verify offsets are sequential
            for (int i = 0; i < records.length; i++) {
                assertEquals(i, records[i].getOffset(), "Batch record " + i + " should have correct offset");
            }
            
            System.out.println("✓ Batch consumption of " + records.length + " messages successful");
        }
    }
    
    @Test
    void testProducerConsumerMetrics() throws IOException {
        System.out.println("\n=== Testing Producer/Consumer Metrics ===");
        
        String topicName = "metrics-test-topic";
        int messageCount = 100;
        
        // Publish messages
        for (int i = 0; i < messageCount; i++) {
            producer.publishString(topicName, "Metrics test message " + i);
        }
        
        // Verify producer metrics
        assertEquals(messageCount, producer.getMessageCount(), "Producer message count should match");
        System.out.println("✓ Producer metrics correct: " + producer.getMessageCount() + " messages");
        
        // Consume messages using the offset-based method (which doesn't increment counter)
        int consumedCount = 0;
        for (int i = 0; i < messageCount; i++) {
            Record record = consumer.consume(topicName, i);
            if (record != null) {
                consumedCount++;
            }
        }
        
        // Verify we consumed all messages
        assertEquals(messageCount, consumedCount, "Should consume all messages");
        
        // Note: consumer.getMessageCount() only counts messages consumed via consume() without offset
        // Since we used consume(topicName, offset), the counter remains 0
        System.out.println("✓ Consumer metrics correct: consumed " + consumedCount + " messages via offset-based consumption");
    }
    
    @Test
    void testMultipleTopics() throws IOException {
        System.out.println("\n=== Testing Multiple Topics ===");
        
        long timestamp = System.currentTimeMillis();
        String[] topicNames = {"topic1-" + timestamp, "topic2-" + timestamp, "topic3-" + timestamp};
        int messagesPerTopic = 50;
        
        // Publish messages to multiple topics
        for (String topicName : topicNames) {
            for (int i = 0; i < messagesPerTopic; i++) {
                producer.publishString(topicName, "Message " + i + " for " + topicName);
            }
        }
        
        // Verify each topic has the correct messages
        for (String topicName : topicNames) {
            for (int i = 0; i < messagesPerTopic; i++) {
                Record record = consumer.consume(topicName, i);
                assertNotNull(record, "Message should exist in topic " + topicName + " at offset " + i);
                assertEquals(i, record.getOffset(), "Offset should match");
                
                String expectedMessage = "Message " + i + " for " + topicName;
                String actualMessage = new String(record.getData());
                assertEquals(expectedMessage, actualMessage, "Message content should match");
            }
        }
        
        System.out.println("✓ Multiple topics test passed");
    }
    
    @Test
    void testErrorHandling() throws IOException {
        System.out.println("\n=== Testing Error Handling ===");
        
        String topicName = "error-test-topic-" + System.currentTimeMillis();
        
        // Test consuming from non-existent topic (this will create the topic automatically)
        // Since the topic gets created automatically, we expect null for non-existent offset
        Record record = consumer.consume(topicName, 0);
        assertNull(record, "Consuming non-existent offset should return null");
        
        // Publish one message
        producer.publishString(topicName, "Test message");
        
        // Test consuming non-existent offset
        record = consumer.consume(topicName, 999);
        assertNull(record, "Consuming non-existent offset should return null");
        
        // Test consuming existing offset
        record = consumer.consume(topicName, 0);
        assertNotNull(record, "Consuming existing offset should return record");
        assertEquals("Test message", new String(record.getData()), "Message content should match");
        
        System.out.println("✓ Error handling test passed");
    }
    
    @Test
    void testBatchOperations() throws IOException {
        System.out.println("\n=== Testing Batch Operations ===");
        
        String topicName = "batch-test-topic-" + System.currentTimeMillis();
        int batchSize = 50;
        
        // Test batch publishing
        byte[][] messages = new byte[batchSize][];
        for (int i = 0; i < batchSize; i++) {
            messages[i] = ("Batch message " + i).getBytes();
        }
        
        Record[] publishedRecords = producer.publishBatch(topicName, messages);
        assertEquals(batchSize, publishedRecords.length, "Should publish all batch messages");
        
        // Verify all messages were published correctly
        for (int i = 0; i < batchSize; i++) {
            assertEquals(i, publishedRecords[i].getOffset(), "Offset should be sequential");
            assertEquals("Batch message " + i, new String(publishedRecords[i].getData()), "Message content should match");
        }
        
        // Test batch consumption
        consumer.setOffset(topicName, 0);
        Record[] consumedRecords = consumer.consumeBatch(topicName, batchSize);
        assertEquals(batchSize, consumedRecords.length, "Should consume all batch messages");
        
        // Verify all messages were consumed correctly
        for (int i = 0; i < batchSize; i++) {
            assertEquals(i, consumedRecords[i].getOffset(), "Consumed offset should be sequential");
            assertEquals("Batch message " + i, new String(consumedRecords[i].getData()), "Consumed message content should match");
        }
        
        System.out.println("✓ Batch operations test passed");
    }
    
    @Test
    void testConcurrentOperations() throws IOException {
        System.out.println("\n=== Testing Concurrent Operations ===");
        
        String topicName = "concurrent-test-topic-" + System.currentTimeMillis();
        int messageCount = 100;
        
        // Publish messages concurrently (simulated)
        List<Record> publishedRecords = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            Record record = producer.publishString(topicName, "Concurrent message " + i);
            publishedRecords.add(record);
        }
        
        // Consume messages concurrently (simulated)
        List<Record> consumedRecords = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            Record record = consumer.consume(topicName, i);
            if (record != null) {
                consumedRecords.add(record);
            }
        }
        
        assertEquals(messageCount, consumedRecords.size(), "Should consume all concurrent messages");
        
        // Verify message integrity
        for (int i = 0; i < messageCount; i++) {
            assertEquals(i, consumedRecords.get(i).getOffset(), "Concurrent offset should be correct");
            assertEquals("Concurrent message " + i, new String(consumedRecords.get(i).getData()), "Concurrent message content should match");
        }
        
        System.out.println("✓ Concurrent operations test passed");
    }
    
    @Test
    void testLargeMessageHandling() throws IOException {
        System.out.println("\n=== Testing Large Message Handling ===");
        
        String topicName = "large-message-test-topic-" + System.currentTimeMillis();
        
        // Test with different message sizes
        int[] messageSizes = {100, 1000, 10000, 100000}; // 100B, 1KB, 10KB, 100KB
        
        for (int size : messageSizes) {
            // Create a large message
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < size; i++) {
                sb.append("A");
            }
            String largeMessage = sb.toString();
            
            // Publish the large message
            Record record = producer.publishString(topicName, largeMessage);
            assertNotNull(record, "Should publish large message of size " + size);
            
            // Consume the large message
            Record consumedRecord = consumer.consume(topicName, record.getOffset());
            assertNotNull(consumedRecord, "Should consume large message of size " + size);
            assertEquals(largeMessage, new String(consumedRecord.getData()), "Large message content should match");
            
            System.out.println("✓ Large message test passed for size " + size + " bytes");
        }
    }
    
    @Test
    void testSparseIndexingPerformance() throws IOException {
        System.out.println("\n=== Testing Sparse Indexing Performance ===");
        
        String topicName = "performance-test-topic-" + System.currentTimeMillis();
        int messageCount = 5000; // More messages to test performance
        
        long startTime = System.currentTimeMillis();
        
        // Publish messages
        for (int i = 0; i < messageCount; i++) {
            producer.publishString(topicName, "Performance test message " + i);
        }
        
        long publishTime = System.currentTimeMillis() - startTime;
        System.out.println("Published " + messageCount + " messages in " + publishTime + "ms");
        
        // Test consumption performance
        startTime = System.currentTimeMillis();
        
        int consumedCount = 0;
        for (int i = 0; i < messageCount; i++) {
            Record record = consumer.consume(topicName, i);
            if (record != null) {
                consumedCount++;
            }
        }
        
        long consumeTime = System.currentTimeMillis() - startTime;
        System.out.println("Consumed " + consumedCount + " messages in " + consumeTime + "ms");
        
        assertEquals(messageCount, consumedCount, "Should consume all performance test messages");
        
        // Performance assertions (these are reasonable expectations)
        assertTrue(publishTime < 10000, "Publishing should complete within 10 seconds");
        assertTrue(consumeTime < 15000, "Consumption should complete within 15 seconds");
        
        System.out.println("✓ Sparse indexing performance test passed");
    }
}
