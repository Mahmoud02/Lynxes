package org.mahmoud.fastqueue.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for configurable flush strategies.
 * Demonstrates different durability vs performance trade-offs.
 */
public class FlushStrategyTest {
    
    @TempDir
    Path tempDir;
    
    private OptimizedStore store;
    
    @AfterEach
    void cleanup() throws IOException {
        if (store != null && !store.isClosed()) {
            store.close();
        }
    }
    
    @Test
    void testImmediateFlushStrategy() throws IOException {
        // Test immediate flush (highest durability, lowest performance)
        FlushConfiguration config = FlushConfiguration.highDurability();
        store = new OptimizedStore(tempDir.resolve("immediate.store"), config);
        
        // Append a record
        Record record = new Record(1L, System.currentTimeMillis(), "test message".getBytes());
        long position = store.append(record);
        
        // With immediate flush, message count should be 0 after append
        assertEquals(0, store.getMessageCountSinceFlush());
        assertEquals(0, store.getTimeSinceLastFlush());
        
        // Verify the record was written
        assertTrue(store.getCurrentPosition() > 0);
    }
    
    @Test
    void testMessageBasedFlushStrategy() throws IOException {
        // Test message-based flush (flush every 5 messages)
        FlushConfiguration config = new FlushConfiguration(
            FlushStrategy.MESSAGE_BASED, 5, 10000, true, true);
        store = new OptimizedStore(tempDir.resolve("message-based.store"), config);
        
        // Append 3 messages - should not trigger flush
        for (int i = 0; i < 3; i++) {
            Record record = new Record(i, System.currentTimeMillis(), ("message " + i).getBytes());
            store.append(record);
        }
        
        assertEquals(3, store.getMessageCountSinceFlush());
        
        // Append 2 more messages - should trigger flush
        for (int i = 3; i < 5; i++) {
            Record record = new Record(i, System.currentTimeMillis(), ("message " + i).getBytes());
            store.append(record);
        }
        
        // Message count should be reset after flush
        assertEquals(0, store.getMessageCountSinceFlush());
    }
    
    @Test
    void testTimeBasedFlushStrategy() throws IOException, InterruptedException {
        // Test time-based flush (flush every 100ms)
        FlushConfiguration config = new FlushConfiguration(
            FlushStrategy.TIME_BASED, 10000, 100, true, true);
        store = new OptimizedStore(tempDir.resolve("time-based.store"), config);
        
        // Append a message
        Record record = new Record(1L, System.currentTimeMillis(), "test message".getBytes());
        store.append(record);
        
        assertEquals(1, store.getMessageCountSinceFlush());
        assertTrue(store.getTimeSinceLastFlush() < 100);
        
        // Wait for time-based flush to trigger
        Thread.sleep(150);
        
        // Append another message to trigger the time-based check
        Record record2 = new Record(2L, System.currentTimeMillis(), "test message 2".getBytes());
        store.append(record2);
        
        // Should have been flushed due to time interval
        assertEquals(1, store.getMessageCountSinceFlush()); // Only the new message
    }
    
    @Test
    void testHybridFlushStrategy() throws IOException, InterruptedException {
        // Test hybrid flush (Kafka-style: both message count AND time)
        FlushConfiguration config = new FlushConfiguration(
            FlushStrategy.HYBRID, 3, 200, true, true);
        store = new OptimizedStore(tempDir.resolve("hybrid.store"), config);
        
        // Append 2 messages - should not trigger flush
        for (int i = 0; i < 2; i++) {
            Record record = new Record(i, System.currentTimeMillis(), ("message " + i).getBytes());
            store.append(record);
        }
        
        assertEquals(2, store.getMessageCountSinceFlush());
        
        // Wait for time-based flush
        Thread.sleep(250);
        
        // Append another message - should trigger flush due to time
        Record record = new Record(2L, System.currentTimeMillis(), "message 2".getBytes());
        store.append(record);
        
        // Should have been flushed due to time interval
        assertEquals(1, store.getMessageCountSinceFlush());
    }
    
    @Test
    void testKafkaStyleConfiguration() throws IOException {
        // Test Kafka-style configuration
        FlushConfiguration config = FlushConfiguration.kafkaStyle();
        store = new OptimizedStore(tempDir.resolve("kafka-style.store"), config);
        
        assertEquals(FlushStrategy.HYBRID, config.getStrategy());
        assertEquals(10000, config.getMessageInterval());
        assertEquals(3000, config.getTimeIntervalMs());
        assertTrue(config.isForceMetadata());
        assertTrue(config.isEnablePageCache());
    }
    
    @Test
    void testHighPerformanceConfiguration() throws IOException {
        // Test high-performance configuration
        FlushConfiguration config = FlushConfiguration.highPerformance();
        store = new OptimizedStore(tempDir.resolve("high-perf.store"), config);
        
        assertEquals(FlushStrategy.OS_CONTROLLED, config.getStrategy());
        assertEquals(Integer.MAX_VALUE, config.getMessageInterval());
        assertEquals(Integer.MAX_VALUE, config.getTimeIntervalMs());
        assertFalse(config.isForceMetadata());
        assertTrue(config.isEnablePageCache());
    }
    
    @Test
    void testBalancedConfiguration() throws IOException {
        // Test balanced configuration
        FlushConfiguration config = FlushConfiguration.balanced();
        store = new OptimizedStore(tempDir.resolve("balanced.store"), config);
        
        assertEquals(FlushStrategy.HYBRID, config.getStrategy());
        assertEquals(1000, config.getMessageInterval());
        assertEquals(1000, config.getTimeIntervalMs());
        assertTrue(config.isForceMetadata());
        assertTrue(config.isEnablePageCache());
    }
    
    @Test
    void testFlushStrategyFromName() {
        // Test parsing flush strategy from name
        assertEquals(FlushStrategy.IMMEDIATE, FlushStrategy.fromName("immediate"));
        assertEquals(FlushStrategy.MESSAGE_BASED, FlushStrategy.fromName("message_based"));
        assertEquals(FlushStrategy.TIME_BASED, FlushStrategy.fromName("time_based"));
        assertEquals(FlushStrategy.HYBRID, FlushStrategy.fromName("hybrid"));
        assertEquals(FlushStrategy.OS_CONTROLLED, FlushStrategy.fromName("os_controlled"));
        
        // Test case insensitive
        assertEquals(FlushStrategy.HYBRID, FlushStrategy.fromName("HYBRID"));
        assertEquals(FlushStrategy.HYBRID, FlushStrategy.fromName("Hybrid"));
        
        // Test default for null
        assertEquals(FlushStrategy.HYBRID, FlushStrategy.fromName(null));
        
        // Test invalid name
        assertThrows(IllegalArgumentException.class, () -> FlushStrategy.fromName("invalid"));
    }
    
    @Test
    void testFlushConfigurationValidation() {
        // Test valid configurations
        assertDoesNotThrow(() -> new FlushConfiguration(FlushStrategy.IMMEDIATE, 1, 0, true, true));
        assertDoesNotThrow(() -> new FlushConfiguration(FlushStrategy.MESSAGE_BASED, 1000, 10000, true, true));
        assertDoesNotThrow(() -> new FlushConfiguration(FlushStrategy.TIME_BASED, 10000, 1000, true, true));
        assertDoesNotThrow(() -> new FlushConfiguration(FlushStrategy.HYBRID, 1000, 1000, true, true));
        assertDoesNotThrow(() -> new FlushConfiguration(FlushStrategy.OS_CONTROLLED, Integer.MAX_VALUE, Integer.MAX_VALUE, false, true));
        
        // Test invalid configurations
        assertThrows(IllegalArgumentException.class, () -> 
            new FlushConfiguration(FlushStrategy.MESSAGE_BASED, 0, 1000, true, true));
        assertThrows(IllegalArgumentException.class, () -> 
            new FlushConfiguration(FlushStrategy.MESSAGE_BASED, -1, 1000, true, true));
        assertThrows(IllegalArgumentException.class, () -> 
            new FlushConfiguration(FlushStrategy.TIME_BASED, 1000, -1, true, true));
        assertThrows(IllegalArgumentException.class, () -> 
            new FlushConfiguration(FlushStrategy.MESSAGE_BASED, Integer.MAX_VALUE, 1000, true, true));
        assertThrows(IllegalArgumentException.class, () -> 
            new FlushConfiguration(FlushStrategy.TIME_BASED, 1000, Integer.MAX_VALUE, true, true));
    }
    
    @Test
    void testPerformanceStats() throws IOException {
        FlushConfiguration config = FlushConfiguration.balanced();
        store = new OptimizedStore(tempDir.resolve("stats.store"), config);
        
        // Append some messages
        for (int i = 0; i < 5; i++) {
            Record record = new Record(i, System.currentTimeMillis(), ("message " + i).getBytes());
            store.append(record);
        }
        
        String stats = store.getPerformanceStats();
        assertTrue(stats.contains("OptimizedStore"));
        assertTrue(stats.contains("messagesSinceFlush=5"));
        assertTrue(stats.contains("config=" + config.toString()));
    }
}
