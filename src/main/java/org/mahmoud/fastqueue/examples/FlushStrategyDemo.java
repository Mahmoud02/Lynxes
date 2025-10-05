package org.mahmoud.fastqueue.examples;

import org.mahmoud.fastqueue.core.FlushConfiguration;
import org.mahmoud.fastqueue.core.FlushStrategy;
import org.mahmoud.fastqueue.core.OptimizedStore;
import org.mahmoud.fastqueue.core.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Demonstration of configurable flush strategies in FastQueue2.
 * Shows different durability vs performance trade-offs.
 */
public class FlushStrategyDemo {
    private static final Logger logger = LoggerFactory.getLogger(FlushStrategyDemo.class);
    
    public static void main(String[] args) {
        try {
            // Create demo directory
            Path demoDir = Paths.get("demo-flush-strategies");
            Files.createDirectories(demoDir);
            
            logger.info("=== FastQueue2 Configurable Flush Strategies Demo ===\n");
            
            // Demo 1: Immediate Flush (Highest Durability)
            demoImmediateFlush(demoDir);
            
            // Demo 2: Kafka-Style Hybrid (Balanced)
            demoKafkaStyleFlush(demoDir);
            
            // Demo 3: High Performance (OS-Controlled)
            demoHighPerformanceFlush(demoDir);
            
            // Demo 4: Custom Configuration
            demoCustomConfiguration(demoDir);
            
            // Demo 5: Performance Comparison
            demoPerformanceComparison(demoDir);
            
            logger.info("\n=== Demo completed successfully! ===");
            
        } catch (Exception e) {
            logger.error("Demo failed", e);
        }
    }
    
    private static void demoImmediateFlush(Path demoDir) throws IOException {
        logger.info("1. IMMEDIATE FLUSH STRATEGY (Highest Durability)");
        logger.info("   - Flushes after every single message");
        logger.info("   - Maximum durability, minimum performance");
        logger.info("   - Use case: Critical financial transactions\n");
        
        FlushConfiguration config = FlushConfiguration.highDurability();
        Path storePath = demoDir.resolve("immediate.store");
        
        try (OptimizedStore store = new OptimizedStore(storePath, config)) {
            long startTime = System.currentTimeMillis();
            
            // Write 10 messages
            for (int i = 0; i < 10; i++) {
                Record record = new Record(i, System.currentTimeMillis(), 
                    ("immediate-message-" + i).getBytes());
                store.append(record);
                
                // With immediate flush, message count should always be 0
                logger.info("   Message {}: position={}, messagesSinceFlush={}, timeSinceFlush={}ms",
                    i, store.getCurrentPosition(), store.getMessageCountSinceFlush(), 
                    store.getTimeSinceLastFlush());
            }
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("   Completed 10 messages in {}ms (immediate flush after each)\n", duration);
        }
    }
    
    private static void demoKafkaStyleFlush(Path demoDir) throws IOException, InterruptedException {
        logger.info("2. KAFKA-STYLE HYBRID STRATEGY (Balanced)");
        logger.info("   - Flushes every 10,000 messages OR 3 seconds");
        logger.info("   - Good balance of durability and performance");
        logger.info("   - Use case: Production message queues\n");
        
        FlushConfiguration config = FlushConfiguration.kafkaStyle();
        Path storePath = demoDir.resolve("kafka-style.store");
        
        try (OptimizedStore store = new OptimizedStore(storePath, config)) {
            long startTime = System.currentTimeMillis();
            
            // Write 15 messages (should trigger flush at 10,000 or 3 seconds)
            for (int i = 0; i < 15; i++) {
                Record record = new Record(i, System.currentTimeMillis(), 
                    ("kafka-message-" + i).getBytes());
                store.append(record);
                
                logger.info("   Message {}: messagesSinceFlush={}, timeSinceFlush={}ms",
                    i, store.getMessageCountSinceFlush(), store.getTimeSinceLastFlush());
                
                // Small delay to see time progression
                Thread.sleep(100);
            }
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("   Completed 15 messages in {}ms (Kafka-style batching)\n", duration);
        }
    }
    
    private static void demoHighPerformanceFlush(Path demoDir) throws IOException {
        logger.info("3. HIGH PERFORMANCE STRATEGY (OS-Controlled)");
        logger.info("   - Lets OS decide when to flush");
        logger.info("   - Maximum performance, minimum durability");
        logger.info("   - Use case: High-throughput, non-critical data\n");
        
        FlushConfiguration config = FlushConfiguration.highPerformance();
        Path storePath = demoDir.resolve("high-perf.store");
        
        try (OptimizedStore store = new OptimizedStore(storePath, config)) {
            long startTime = System.currentTimeMillis();
            
            // Write 100 messages quickly
            for (int i = 0; i < 100; i++) {
                Record record = new Record(i, System.currentTimeMillis(), 
                    ("perf-message-" + i).getBytes());
                store.append(record);
            }
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("   Completed 100 messages in {}ms (OS-controlled flushing)", duration);
            logger.info("   Messages since flush: {}, Time since flush: {}ms\n", 
                store.getMessageCountSinceFlush(), store.getTimeSinceLastFlush());
        }
    }
    
    private static void demoCustomConfiguration(Path demoDir) throws IOException, InterruptedException {
        logger.info("4. CUSTOM CONFIGURATION");
        logger.info("   - Flush every 5 messages OR 500ms");
        logger.info("   - Customized for specific use case");
        logger.info("   - Use case: Development/testing\n");
        
        FlushConfiguration config = new FlushConfiguration(
            FlushStrategy.HYBRID, 5, 500, true, true);
        Path storePath = demoDir.resolve("custom.store");
        
        try (OptimizedStore store = new OptimizedStore(storePath, config)) {
            long startTime = System.currentTimeMillis();
            
            // Write 8 messages with delays
            for (int i = 0; i < 8; i++) {
                Record record = new Record(i, System.currentTimeMillis(), 
                    ("custom-message-" + i).getBytes());
                store.append(record);
                
                logger.info("   Message {}: messagesSinceFlush={}, timeSinceFlush={}ms",
                    i, store.getMessageCountSinceFlush(), store.getTimeSinceLastFlush());
                
                // Delay to trigger time-based flush
                Thread.sleep(200);
            }
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("   Completed 8 messages in {}ms (custom hybrid strategy)\n", duration);
        }
    }
    
    private static void demoPerformanceComparison(Path demoDir) throws IOException {
        logger.info("5. PERFORMANCE COMPARISON");
        logger.info("   Comparing different strategies with 1000 messages\n");
        
        int messageCount = 1000;
        
        // Test immediate flush
        long immediateTime = testStrategy(demoDir, "immediate-perf", 
            FlushConfiguration.highDurability(), messageCount);
        
        // Test balanced flush
        long balancedTime = testStrategy(demoDir, "balanced-perf", 
            FlushConfiguration.balanced(), messageCount);
        
        // Test high performance flush
        long highPerfTime = testStrategy(demoDir, "high-perf-perf", 
            FlushConfiguration.highPerformance(), messageCount);
        
        logger.info("   Performance Results ({} messages):", messageCount);
        logger.info("   - Immediate Flush:    {}ms", immediateTime);
        logger.info("   - Balanced Flush:     {}ms", balancedTime);
        logger.info("   - High Performance:   {}ms", highPerfTime);
        logger.info("   - Performance Ratio:  {}x faster (balanced vs immediate)", 
            (double) immediateTime / balancedTime);
        logger.info("   - Performance Ratio:  {}x faster (high-perf vs immediate)", 
            (double) immediateTime / highPerfTime);
    }
    
    private static long testStrategy(Path demoDir, String name, 
                                   FlushConfiguration config, int messageCount) throws IOException {
        Path storePath = demoDir.resolve(name + ".store");
        
        try (OptimizedStore store = new OptimizedStore(storePath, config)) {
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < messageCount; i++) {
                Record record = new Record(i, System.currentTimeMillis(), 
                    ("perf-test-" + i).getBytes());
                store.append(record);
            }
            
            // Force final flush
            store.force();
            
            return System.currentTimeMillis() - startTime;
        }
    }
}
