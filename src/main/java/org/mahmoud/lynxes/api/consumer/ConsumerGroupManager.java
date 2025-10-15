package org.mahmoud.lynxes.api.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages consumer groups for topics.
 * Provides thread-safe operations for creating, managing, and coordinating consumer groups.
 */
public class ConsumerGroupManager {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupManager.class);
    
    // Map: topicName -> Map: groupId -> ConsumerGroup
    private final Map<String, Map<String, ConsumerGroup>> topicGroups;
    private final ReadWriteLock lock;
    
    public ConsumerGroupManager() {
        this.topicGroups = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        logger.info("ConsumerGroupManager initialized");
    }
    
    /**
     * Creates or gets a consumer group for a topic.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @return The consumer group
     */
    public ConsumerGroup getOrCreateConsumerGroup(String topicName, String groupId) {
        lock.writeLock().lock();
        try {
            Map<String, ConsumerGroup> groups = topicGroups.computeIfAbsent(topicName, 
                k -> new ConcurrentHashMap<>());
            
            ConsumerGroup group = groups.get(groupId);
            if (group == null) {
                group = new ConsumerGroup(groupId, topicName);
                groups.put(groupId, group);
                logger.info("Created new consumer group: {} for topic: {}", groupId, topicName);
            }
            
            return group;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Adds a consumer to a specific consumer group for a given topic.
     * If the group or topic does not exist, they will be created.
     *
     * @param topicName The name of the topic.
     * @param groupId The ID of the consumer group.
     * @param consumerId The unique ID of the consumer.
     * @param consumer The Consumer instance.
     * @return true if consumer was added successfully
     */
    public boolean addConsumerToGroup(String topicName, String groupId, String consumerId, Consumer consumer) {
        ConsumerGroup group = getOrCreateConsumerGroup(topicName, groupId);
        group.addConsumer(consumerId, consumer);
        logger.debug("Consumer {} added to group {} for topic {}", consumerId, groupId, topicName);
        return true;
    }
    
    /**
     * Gets an existing consumer group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @return The consumer group, or null if not found
     */
    public ConsumerGroup getConsumerGroup(String topicName, String groupId) {
        lock.readLock().lock();
        try {
            Map<String, ConsumerGroup> groups = topicGroups.get(topicName);
            if (groups == null) {
                return null;
            }
            return groups.get(groupId);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Removes a consumer group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @return true if the group was removed, false if not found
     */
    public boolean removeConsumerGroup(String topicName, String groupId) {
        lock.writeLock().lock();
        try {
            Map<String, ConsumerGroup> groups = topicGroups.get(topicName);
            if (groups == null) {
                return false;
            }
            
            ConsumerGroup removed = groups.remove(groupId);
            if (removed != null) {
                logger.info("Removed consumer group: {} from topic: {}", groupId, topicName);
                
                // Clean up empty topic entries
                if (groups.isEmpty()) {
                    topicGroups.remove(topicName);
                }
                return true;
            }
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    
    /**
     * Removes a consumer from a group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @param consumerId The consumer ID
     * @return true if the consumer was removed, false if not found
     */
    public boolean removeConsumerFromGroup(String topicName, String groupId, String consumerId) {
        ConsumerGroup group = getConsumerGroup(topicName, groupId);
        if (group == null) {
            return false;
        }
        
        group.removeConsumer(consumerId);
        if (group.getGroupSize() == 0) {
            // Remove empty groups
            removeConsumerGroup(topicName, groupId);
        }
        
        return true;
    }
    
    /**
     * Gets all consumer groups for a topic.
     * @param topicName The topic name
     * @return A map of group ID to consumer group
     */
    public Map<String, ConsumerGroup> getConsumerGroupsForTopic(String topicName) {
        lock.readLock().lock();
        try {
            Map<String, ConsumerGroup> groups = topicGroups.get(topicName);
            if (groups == null) {
                return new HashMap<>();
            }
            return new HashMap<>(groups);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Gets all topics with consumer groups.
     * @return A set of topic names
     */
    public Set<String> getTopicsWithGroups() {
        lock.readLock().lock();
        try {
            return new HashSet<>(topicGroups.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Checks if a message should be delivered to a specific consumer in a group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @param messageOffset The message offset
     * @param consumerId The consumer ID
     * @return true if the message should be delivered to this consumer
     */
    public boolean shouldDeliverToConsumer(String topicName, String groupId, 
                                        long messageOffset, String consumerId) {
        ConsumerGroup group = getConsumerGroup(topicName, groupId);
        if (group == null) {
            return false;
        }
        
        // Leader-based model: only leader consumes messages
        return group.getCurrentLeaderId() != null && group.getCurrentLeaderId().equals(consumerId);
    }
    
    /**
     * Updates the group offset for a consumer group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @param newOffset The new offset
     */
    public void updateGroupOffset(String topicName, String groupId, long newOffset) {
        ConsumerGroup group = getConsumerGroup(topicName, groupId);
        if (group != null) {
            group.updateGroupOffset(newOffset);
        }
    }
    
    /**
     * Gets statistics about all consumer groups.
     * @return A map of statistics
     */
    public Map<String, Object> getStatistics() {
        lock.readLock().lock();
        try {
            Map<String, Object> stats = new HashMap<>();
            int totalGroups = 0;
            int totalConsumers = 0;
            
            for (Map<String, ConsumerGroup> topicGroups : topicGroups.values()) {
                totalGroups += topicGroups.size();
                for (ConsumerGroup group : topicGroups.values()) {
                    totalConsumers += group.getGroupSize();
                }
            }
            
            stats.put("totalTopics", topicGroups.size());
            stats.put("totalGroups", totalGroups);
            stats.put("totalConsumers", totalConsumers);
            
            return stats;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Gets detailed information about all consumer groups.
     * @return A detailed information string
     */
    public String getDetailedInfo() {
        lock.readLock().lock();
        try {
            StringBuilder info = new StringBuilder();
            info.append("ConsumerGroupManager Statistics:\n");
            
            for (Map.Entry<String, Map<String, ConsumerGroup>> topicEntry : topicGroups.entrySet()) {
                String topicName = topicEntry.getKey();
                Map<String, ConsumerGroup> groups = topicEntry.getValue();
                
                info.append("  Topic: ").append(topicName).append("\n");
                for (ConsumerGroup group : groups.values()) {
                    info.append("    ").append(group.toString()).append("\n");
                }
            }
            
            return info.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Cleans up inactive consumer groups (groups with no consumers).
     * @return The number of groups cleaned up
     */
    public int cleanupInactiveGroups() {
        lock.writeLock().lock();
        try {
            int cleanedUp = 0;
            List<String> topicsToRemove = new ArrayList<>();
            
            for (Map.Entry<String, Map<String, ConsumerGroup>> topicEntry : topicGroups.entrySet()) {
                String topicName = topicEntry.getKey();
                Map<String, ConsumerGroup> groups = topicEntry.getValue();
                
                List<String> groupsToRemove = new ArrayList<>();
                for (Map.Entry<String, ConsumerGroup> groupEntry : groups.entrySet()) {
                    String groupId = groupEntry.getKey();
                    ConsumerGroup group = groupEntry.getValue();
                    
                    if (group.getGroupSize() == 0) {
                        groupsToRemove.add(groupId);
                        cleanedUp++;
                    }
                }
                
                for (String groupId : groupsToRemove) {
                    groups.remove(groupId);
                }
                
                if (groups.isEmpty()) {
                    topicsToRemove.add(topicName);
                }
            }
            
            for (String topicName : topicsToRemove) {
                topicGroups.remove(topicName);
            }
            
            if (cleanedUp > 0) {
                logger.info("Cleaned up {} inactive consumer groups", cleanedUp);
            }
            
            return cleanedUp;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
