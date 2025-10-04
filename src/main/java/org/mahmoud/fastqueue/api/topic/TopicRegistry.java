package org.mahmoud.fastqueue.api.topic;

import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.core.Log;
import org.mahmoud.fastqueue.util.QueueUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing Topic instances and ensuring data sharing.
 * Ensures that all Topic instances for the same name share the same underlying Log.
 */
public class TopicRegistry {
    private static final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Log> logs = new ConcurrentHashMap<>();
    
    /**
     * Gets or creates a Topic instance for the specified name.
     * All Topic instances for the same name will share the same underlying Log.
     * 
     * @param name The topic name
     * @param config The queue configuration
     * @return The Topic instance
     * @throws IOException if the topic cannot be created
     */
    public static synchronized Topic getOrCreateTopic(String name, QueueConfig config) throws IOException {
        String sanitizedName = QueueUtils.sanitizeTopicName(name);
        
        return topics.computeIfAbsent(sanitizedName, topicName -> {
            try {
                return new Topic(topicName, config, getOrCreateLog(topicName, config));
            } catch (IOException e) {
                throw new RuntimeException("Failed to create topic: " + topicName, e);
            }
        });
    }
    
    /**
     * Gets or creates a Log instance for the specified topic name.
     * 
     * @param topicName The topic name
     * @param config The queue configuration
     * @return The Log instance
     * @throws IOException if the log cannot be created
     */
    private static synchronized Log getOrCreateLog(String topicName, QueueConfig config) throws IOException {
        return logs.computeIfAbsent(topicName, name -> {
            try {
                // Create topic directory
                Path topicDir = config.getDataDirectory().resolve("topics").resolve(name);
                QueueUtils.createDirectoryIfNotExists(topicDir);
                
                // Create and return log for this topic
                return new Log(topicDir, config.getMaxSegmentSize(), config.getRetentionPeriodMs());
            } catch (IOException e) {
                throw new RuntimeException("Failed to create log for topic: " + name, e);
            }
        });
    }
    
    /**
     * Closes all topics and logs in the registry.
     * This should be called when shutting down the application.
     */
    public static synchronized void closeAll() {
        topics.values().forEach(topic -> {
            try {
                topic.close();
            } catch (Exception e) {
                System.err.println("Error closing topic: " + e.getMessage());
            }
        });
        topics.clear();
        
        logs.values().forEach(log -> {
            try {
                log.close();
            } catch (Exception e) {
                System.err.println("Error closing log: " + e.getMessage());
            }
        });
        logs.clear();
    }
    
    /**
     * Gets the number of active topics.
     * 
     * @return The number of topics
     */
    public static int getTopicCount() {
        return topics.size();
    }
    
    /**
     * Gets the number of active logs.
     * 
     * @return The number of logs
     */
    public static int getLogCount() {
        return logs.size();
    }
}
