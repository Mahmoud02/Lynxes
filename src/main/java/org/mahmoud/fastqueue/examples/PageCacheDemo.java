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

/**
 * Demonstration of page cache effects in OptimizedStore.
 */
public class PageCacheDemo {
    private static final Logger logger = LoggerFactory.getLogger(PageCacheDemo.class);
    
    public static void main(String[] args) {
        try {
            Path demoDir = Paths.get("demo-page-cache");
            Files.createDirectories(demoDir);
            
            logger.info("=== Page Cache Effects Demo ===\n");
            
            // Test with page cache enabled
            testWithPageCache(demoDir, true);
            
            // Test with page cache disabled
            testWithPageCache(demoDir, false);
            
            logger.info("=== Demo completed! ===");
            
        } catch (Exception e) {
            logger.error("Demo failed", e);
        }
    }
    
    private static void testWithPageCache(Path demoDir, boolean enablePageCache) throws IOException {
        String configName = enablePageCache ? "WITH page cache" : "WITHOUT page cache";
        logger.info("Testing {}:", configName);
        
        // Create configuration
        FlushConfiguration config = new FlushConfiguration(
            FlushStrategy.OS_CONTROLLED,  // Let OS handle flushing
            Integer.MAX_VALUE,            // No message-based flush
            Integer.MAX_VALUE,            // No time-based flush
            false,                        // Don't force metadata
            enablePageCache               // Enable/disable page cache
        );
        
        Path storePath = demoDir.resolve("page-cache-" + enablePageCache + ".store");
        
        try (OptimizedStore store = new OptimizedStore(storePath, config)) {
            int messageCount = 10000;
            long startTime = System.currentTimeMillis();
            
            // Write messages
            for (int i = 0; i < messageCount; i++) {
                Record record = new Record(i, System.currentTimeMillis(), 
                    ("page-cache-test-message-" + i).getBytes());
                store.append(record);
            }
            
            long writeTime = System.currentTimeMillis() - startTime;
            
            // Force flush to disk to see real disk I/O time
            long flushStartTime = System.currentTimeMillis();
            store.force();
            long flushTime = System.currentTimeMillis() - flushStartTime;
            
            long totalTime = System.currentTimeMillis() - startTime;
            
            logger.info("   Messages written: {}", messageCount);
            logger.info("   Write time: {}ms (to page cache)", writeTime);
            logger.info("   Flush time: {}ms (to disk)", flushTime);
            logger.info("   Total time: {}ms", totalTime);
            logger.info("   Messages/sec: {}", messageCount * 1000 / totalTime);
            logger.info("   Store size: {} bytes", store.getCurrentPosition());
            logger.info("");
        }
    }
}
