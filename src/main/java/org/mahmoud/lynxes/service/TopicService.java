package org.mahmoud.lynxes.service;

import org.mahmoud.lynxes.api.topic.Topic;
import org.mahmoud.lynxes.api.topic.TopicRegistry;
import org.mahmoud.lynxes.config.QueueConfig;
import org.mahmoud.lynxes.util.TopicValidator;
import org.mahmoud.lynxes.util.FileUtils;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for managing topics in FastQueue2.
 * Provides CRUD operations and topic metadata for the UI.
 */
public class TopicService {
    
    private final QueueConfig config;
    
    @Inject
    public TopicService(QueueConfig config) {
        this.config = config;
    }
    
    /**
     * Creates a new topic.
     * 
     * @param topicName The name of the topic to create
     * @return TopicInfo with metadata
     * @throws IllegalArgumentException if topic name is invalid
     * @throws IOException if topic creation fails
     */
    public TopicInfo createTopic(String topicName) throws IllegalArgumentException, IOException {
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        
        String sanitizedName = TopicValidator.sanitizeTopicName(topicName.trim());
        if (sanitizedName.isEmpty()) {
            throw new IllegalArgumentException("Invalid topic name: " + topicName);
        }
        
        // Check if topic already exists
        if (topicExists(sanitizedName)) {
            throw new IllegalArgumentException("Topic already exists: " + sanitizedName);
        }
        
        // Create the topic (this will create the directory structure)
        Topic topic = TopicRegistry.getOrCreateTopic(sanitizedName, config);
        
        return new TopicInfo(
            sanitizedName,
            0, // No messages yet
            LocalDateTime.now(),
            topic.getNextOffset(),
            getTopicDirectorySize(sanitizedName)
        );
    }
    
    /**
     * Lists all existing topics.
     * 
     * @return List of TopicInfo objects
     */
    public List<TopicInfo> listTopics() {
        List<TopicInfo> topics = new ArrayList<>();
        
        try {
            Path topicsDir = config.getDataDirectory().resolve("topics");
            if (Files.exists(topicsDir)) {
                Files.list(topicsDir)
                    .filter(Files::isDirectory)
                    .forEach(topicDir -> {
                        String topicName = topicDir.getFileName().toString();
                        try {
                            TopicInfo info = getTopicInfo(topicName);
                            if (info != null) {
                                topics.add(info);
                            }
                        } catch (Exception e) {
                            // Skip topics that can't be read
                            System.err.println("Error reading topic " + topicName + ": " + e.getMessage());
                        }
                    });
            }
        } catch (IOException e) {
            System.err.println("Error listing topics: " + e.getMessage());
        }
        
        return topics.stream()
            .sorted(Comparator.comparing(TopicInfo::getName))
            .collect(Collectors.toList());
    }
    
    /**
     * Gets information about a specific topic.
     * 
     * @param topicName The topic name
     * @return TopicInfo or null if topic doesn't exist
     */
    public TopicInfo getTopicInfo(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return null;
        }
        
        String sanitizedName = TopicValidator.sanitizeTopicName(topicName.trim());
        if (!topicExists(sanitizedName)) {
            return null;
        }
        
        try {
            Topic topic = TopicRegistry.getOrCreateTopic(sanitizedName, config);
            long messageCount = topic.getMessageCount();
            long directorySize = getTopicDirectorySize(sanitizedName);
            
            // Get creation time from directory
            LocalDateTime createdAt = getTopicCreationTime(sanitizedName);
            
            return new TopicInfo(
                sanitizedName,
                messageCount,
                createdAt,
                topic.getNextOffset(),
                directorySize
            );
        } catch (Exception e) {
            System.err.println("Error getting topic info for " + sanitizedName + ": " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Deletes a topic and all its data.
     * 
     * @param topicName The topic name to delete
     * @return true if topic was deleted, false if it didn't exist
     * @throws IOException if deletion fails
     */
    public boolean deleteTopic(String topicName) throws IOException {
        if (topicName == null || topicName.trim().isEmpty()) {
            return false;
        }
        
        String sanitizedName = TopicValidator.sanitizeTopicName(topicName.trim());
        if (!topicExists(sanitizedName)) {
            return false;
        }
        
        // Note: TopicRegistry doesn't expose remove methods, so we rely on directory deletion
        // The registry will handle cleanup when the application shuts down
        
        // Delete the directory
        Path topicDir = config.getDataDirectory().resolve("topics").resolve(sanitizedName);
        if (Files.exists(topicDir)) {
            FileUtils.deleteDirectory(topicDir);
        }
        
        return true;
    }
    
    /**
     * Checks if a topic exists.
     * 
     * @param topicName The topic name
     * @return true if topic exists
     */
    public boolean topicExists(String topicName) {
        if (topicName == null || topicName.trim().isEmpty()) {
            return false;
        }
        
        String sanitizedName = TopicValidator.sanitizeTopicName(topicName.trim());
        Path topicDir = config.getDataDirectory().resolve("topics").resolve(sanitizedName);
        return Files.exists(topicDir);
    }
    
    /**
     * Gets the total number of topics.
     * 
     * @return Number of topics
     */
    public int getTopicCount() {
        return TopicRegistry.getTopicCount();
    }
    
    /**
     * Gets the total size of all topic data.
     * 
     * @return Total size in bytes
     */
    public long getTotalTopicSize() {
        try {
            Path topicsDir = config.getDataDirectory().resolve("topics");
            if (!Files.exists(topicsDir)) {
                return 0;
            }
            
            return Files.list(topicsDir)
                .filter(Files::isDirectory)
                .mapToLong(this::getDirectorySize)
                .sum();
        } catch (IOException e) {
            return 0;
        }
    }
    
    /**
     * Gets the total number of messages across all topics.
     * 
     * @return Total message count
     */
    public long getTotalMessageCount() {
        try {
            List<TopicInfo> topics = listTopics();
            return topics.stream()
                .mapToLong(TopicInfo::getMessageCount)
                .sum();
        } catch (Exception e) {
            return 0;
        }
    }
    
    // Helper methods
    
    private long getTopicDirectorySize(String topicName) {
        Path topicDir = config.getDataDirectory().resolve("topics").resolve(topicName);
        return getDirectorySize(topicDir);
    }
    
    private long getDirectorySize(Path directory) {
        try {
            if (!Files.exists(directory)) {
                return 0;
            }
            
            return Files.walk(directory)
                .filter(Files::isRegularFile)
                .mapToLong(path -> {
                    try {
                        return Files.size(path);
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .sum();
        } catch (IOException e) {
            return 0;
        }
    }
    
    private LocalDateTime getTopicCreationTime(String topicName) {
        try {
            Path topicDir = config.getDataDirectory().resolve("topics").resolve(topicName);
            if (Files.exists(topicDir)) {
                return LocalDateTime.ofInstant(
                    Files.getLastModifiedTime(topicDir).toInstant(),
                    ZoneId.systemDefault()
                );
            }
        } catch (IOException e) {
            // Fall back to current time
        }
        return LocalDateTime.now();
    }
    
    /**
     * Data class representing topic information for the UI.
     */
    public static class TopicInfo {
        private final String name;
        private final long messageCount;
        private final LocalDateTime createdAt;
        private final long nextOffset;
        private final long sizeBytes;
        
        public TopicInfo(String name, long messageCount, LocalDateTime createdAt, long nextOffset, long sizeBytes) {
            this.name = name;
            this.messageCount = messageCount;
            this.createdAt = createdAt;
            this.nextOffset = nextOffset;
            this.sizeBytes = sizeBytes;
        }
        
        public String getName() { return name; }
        public long getMessageCount() { return messageCount; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public long getNextOffset() { return nextOffset; }
        public long getSizeBytes() { return sizeBytes; }
        
        public String getFormattedSize() {
            if (sizeBytes < 1024) {
                return sizeBytes + " B";
            } else if (sizeBytes < 1024 * 1024) {
                return String.format("%.1f KB", sizeBytes / 1024.0);
            } else {
                return String.format("%.1f MB", sizeBytes / (1024.0 * 1024.0));
            }
        }
    }
}
