package org.mahmoud.lynxes.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Demonstration of ByteBuffer benefits for high-performance I/O.
 */
public class ByteBufferDemo {
    private static final Logger logger = LoggerFactory.getLogger(ByteBufferDemo.class);
    
    public static void main(String[] args) {
        try {
            Path demoDir = Paths.get("demo-bytebuffer");
            Files.createDirectories(demoDir);
            
            logger.info("=== ByteBuffer Performance Demo ===\n");
            
            // Test different approaches
            testTraditionalIO(demoDir);
            testByteBufferIO(demoDir);
            testMemoryMappedIO(demoDir);
            
            logger.info("=== Demo completed! ===");
            
        } catch (Exception e) {
            logger.error("Demo failed", e);
        }
    }
    
    private static void testTraditionalIO(Path demoDir) throws IOException {
        logger.info("1. TRADITIONAL I/O (FileOutputStream)");
        
        Path file = demoDir.resolve("traditional.txt");
        long startTime = System.currentTimeMillis();
        
        try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
            for (int i = 0; i < 10000; i++) {
                String data = "message-" + i + "\n";
                fos.write(data.getBytes());
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long fileSize = Files.size(file);
        
        logger.info("   Time: {}ms", duration);
        logger.info("   Size: {} bytes", fileSize);
        logger.info("   Messages/sec: {}", 10000 * 1000 / duration);
        logger.info("");
    }
    
    private static void testByteBufferIO(Path demoDir) throws IOException {
        logger.info("2. BYTEBUFFER I/O (FileChannel)");
        
        Path file = demoDir.resolve("bytebuffer.txt");
        long startTime = System.currentTimeMillis();
        
        try (FileChannel channel = FileChannel.open(file, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.WRITE)) {
            
            for (int i = 0; i < 10000; i++) {
                String data = "message-" + i + "\n";
                ByteBuffer buffer = ByteBuffer.wrap(data.getBytes());
                channel.write(buffer);
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long fileSize = Files.size(file);
        
        logger.info("   Time: {}ms", duration);
        logger.info("   Size: {} bytes", fileSize);
        logger.info("   Messages/sec: {}", 10000 * 1000 / duration);
        logger.info("");
    }
    
    private static void testMemoryMappedIO(Path demoDir) throws IOException {
        logger.info("3. MEMORY-MAPPED I/O (MappedByteBuffer)");
        
        Path file = demoDir.resolve("memorymapped.txt");
        long startTime = System.currentTimeMillis();
        
        // Pre-allocate file size
        long fileSize = 10000 * 20L; // Approximate size
        try (FileChannel channel = FileChannel.open(file, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.READ, 
                StandardOpenOption.WRITE)) {
            
            // Map entire file into memory
            var mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            
            for (int i = 0; i < 10000; i++) {
                String data = "message-" + i + "\n";
                byte[] bytes = data.getBytes();
                mappedBuffer.put(bytes);
            }
            
            // Force to disk
            mappedBuffer.force();
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long actualFileSize = Files.size(file);
        
        logger.info("   Time: {}ms", duration);
        logger.info("   Size: {} bytes", actualFileSize);
        logger.info("   Messages/sec: {}", 10000 * 1000 / duration);
        logger.info("");
    }
}
