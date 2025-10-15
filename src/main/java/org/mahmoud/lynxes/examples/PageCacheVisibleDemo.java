package org.mahmoud.lynxes.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Demonstration that makes page cache behavior visible.
 */
public class PageCacheVisibleDemo {
    private static final Logger logger = LoggerFactory.getLogger(PageCacheVisibleDemo.class);
    
    public static void main(String[] args) {
        try {
            Path demoDir = Paths.get("demo-page-cache-visible");
            Files.createDirectories(demoDir);
            
            logger.info("=== Page Cache Visibility Demo ===\n");
            
            // Test 1: Write to page cache (no force)
            testPageCacheWrite(demoDir);
            
            // Test 2: Write and force immediately
            testImmediateFlush(demoDir);
            
            // Test 3: Show the difference
            testPageCacheVsDisk(demoDir);
            
            logger.info("=== Demo completed! ===");
            
        } catch (Exception e) {
            logger.error("Demo failed", e);
        }
    }
    
    private static void testPageCacheWrite(Path demoDir) throws IOException {
        logger.info("1. WRITE TO PAGE CACHE (no force)");
        logger.info("   Data goes to OS page cache, NOT disk yet\n");
        
        Path file = demoDir.resolve("page-cache-test.txt");
        
        try (FileChannel channel = FileChannel.open(file, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.WRITE)) {
            
            // Write data
            String data = "This data goes to page cache first!";
            ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
            channel.write(buffer);
            
            logger.info("   Data written: '{}'", data);
            logger.info("   FileChannel.write() completed");
            logger.info("   Data is in OS page cache (RAM)");
            logger.info("   Data is NOT on disk yet!");
            logger.info("   File size on disk: {} bytes", Files.size(file));
            logger.info("");
        }
    }
    
    private static void testImmediateFlush(Path demoDir) throws IOException {
        logger.info("2. WRITE AND FORCE IMMEDIATELY");
        logger.info("   Data goes to page cache, then forced to disk\n");
        
        Path file = demoDir.resolve("immediate-flush-test.txt");
        
        try (FileChannel channel = FileChannel.open(file, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.WRITE)) {
            
            // Write data
            String data = "This data goes to page cache then disk!";
            ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
            channel.write(buffer);
            
            logger.info("   Data written: '{}'", data);
            logger.info("   FileChannel.write() completed");
            logger.info("   Data is in OS page cache (RAM)");
            
            // Force flush
            channel.force(false);
            logger.info("   FileChannel.force() completed");
            logger.info("   Data is now on disk!");
            logger.info("   File size on disk: {} bytes", Files.size(file));
            logger.info("");
        }
    }
    
    private static void testPageCacheVsDisk(Path demoDir) throws IOException {
        logger.info("3. PAGE CACHE vs DISK COMPARISON");
        logger.info("   Shows the difference between page cache and disk\n");
        
        Path file1 = demoDir.resolve("page-cache-only.txt");
        Path file2 = demoDir.resolve("page-cache-and-disk.txt");
        
        // File 1: Page cache only
        try (FileChannel channel = FileChannel.open(file1, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.WRITE)) {
            
            String data = "Page cache only - not on disk!";
            ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
            channel.write(buffer);
            
            logger.info("   File 1 (page cache only):");
            logger.info("     Data: '{}'", data);
            logger.info("     FileChannel.write() completed");
            logger.info("     FileChannel.force() NOT called");
            logger.info("     Data in page cache: YES");
            logger.info("     Data on disk: NO (until OS flushes)");
            logger.info("     File size: {} bytes", Files.size(file1));
        }
        
        // File 2: Page cache + disk
        try (FileChannel channel = FileChannel.open(file2, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.WRITE)) {
            
            String data = "Page cache and disk - forced!";
            ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
            channel.write(buffer);
            channel.force(false);
            
            logger.info("   File 2 (page cache + disk):");
            logger.info("     Data: '{}'", data);
            logger.info("     FileChannel.write() completed");
            logger.info("     FileChannel.force() called");
            logger.info("     Data in page cache: YES");
            logger.info("     Data on disk: YES");
            logger.info("     File size: {} bytes", Files.size(file2));
        }
        
        logger.info("");
    }
}
