package org.mahmoud.lynxes.service;

import org.mahmoud.lynxes.domain.consumer.Consumer;
import org.mahmoud.lynxes.domain.consumer.ConsumerGroup;
import org.mahmoud.lynxes.domain.consumer.ConsumerGroupManager;
import org.mahmoud.lynxes.domain.topic.TopicRegistry;
import org.mahmoud.lynxes.core.Record;
import org.mahmoud.lynxes.config.QueueConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Service for managing consumer groups and message distribution.
 * Handles the business logic for consumer group operations.
 */
public class ConsumerGroupService {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupService.class);
    
    private final ConsumerGroupManager groupManager;
    private final QueueConfig config;
    
    public ConsumerGroupService(ConsumerGroupManager groupManager, QueueConfig config) {
        this.groupManager = groupManager;
        this.config = config;
        logger.info("ConsumerGroupService initialized");
    }
    
    /**
     * Registers a consumer with a consumer group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @param consumerId The consumer ID
     * @return true if consumer was registered successfully
     */
    public boolean registerConsumer(String topicName, String groupId, String consumerId) {
        logger.info("Registering consumer {} to group {} for topic {}", consumerId, groupId, topicName);
        
        // Create a consumer instance
        Consumer consumer = new Consumer(config);
        
        // Add consumer to group
        groupManager.addConsumerToGroup(topicName, groupId, consumerId, consumer);
        boolean success = true;
        
        logger.info("Consumer {} registered successfully in group {} for topic {}", 
                   consumerId, groupId, topicName);
        
        return success;
    }
    
    /**
     * Unregisters a consumer from a consumer group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @param consumerId The consumer ID
     * @return true if the consumer was unregistered, false if not found
     */
    public boolean unregisterConsumer(String topicName, String groupId, String consumerId) {
        logger.info("Unregistering consumer {} from group {} for topic {}", consumerId, groupId, topicName);
        
        boolean removed = groupManager.removeConsumerFromGroup(topicName, groupId, consumerId);
        
        if (removed) {
            logger.info("Consumer {} unregistered from group {} for topic {}", consumerId, groupId, topicName);
        } else {
            logger.warn("Failed to unregister consumer {} from group {} for topic {} - not found", 
                       consumerId, groupId, topicName);
        }
        
        return removed;
    }
    
    /**
     * Consumes messages for a specific consumer in a group.
     * LEADER-BASED MODEL: Only the leader consumer consumes messages directly from the log.
     * Other consumers get empty results. If leader is stale, new leader is elected.
     *
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @param consumerId The consumer ID
     * @param offset The starting offset (ignored, uses group's last consumed offset)
     * @param maxMessages Maximum number of messages to return
     * @return List of messages for this consumer (only if it's the leader)
     */
    public List<Record> consumeMessages(String topicName, String groupId, String consumerId,
                                      long offset, int maxMessages) {
        logger.debug("Consumer {} requesting messages from group {} for topic {} (leader-based model)",
                    consumerId, groupId, topicName);

        // Get the consumer group
        ConsumerGroup group = groupManager.getConsumerGroup(topicName, groupId);
        if (group == null) {
            logger.warn("Consumer group {} not found for topic {}", groupId, topicName);
            return Collections.emptyList();
        }

        // Try to become leader or get current leader
        String leaderId = group.getOrAssignLeader(consumerId);
        if (leaderId == null) {
            logger.warn("Consumer {} not found in group {}, cannot assign as leader", consumerId, groupId);
            return Collections.emptyList();
        }

        // Only the leader consumes messages
        if (!consumerId.equals(leaderId)) {
            logger.debug("Consumer {} is not the leader ({}), returning empty result", consumerId, leaderId);
            return Collections.emptyList();
        }

        // Leader consumes messages directly from the log
        logger.info("Leader {} consuming messages from topic {} for group {}", consumerId, topicName, groupId);
        
        List<Record> messages = new ArrayList<>();
        long currentOffset = group.getGroupOffset(); // Start from group's last consumed offset
        long lastProcessedOffset = currentOffset - 1;

        while (messages.size() < maxMessages) {
            try {
                Record message = TopicRegistry.getOrCreateTopic(topicName, config).consume(currentOffset);
                if (message == null) {
                    break; // No more messages
                }

                messages.add(message);
                lastProcessedOffset = Math.max(lastProcessedOffset, message.getOffset());
                currentOffset++;
            } catch (Exception e) {
                logger.debug("No more messages at offset {}", currentOffset);
                break;
            }
        }

        // Update group offset and heartbeat
        if (!messages.isEmpty()) {
            group.updateGroupOffset(lastProcessedOffset + 1);
            group.updateHeartbeat(consumerId);
            logger.info("Leader {} consumed {} messages from topic {} for group {}. Next offset: {}", 
                       consumerId, messages.size(), topicName, groupId, lastProcessedOffset + 1);
        } else {
            // Still update heartbeat even if no messages
            group.updateHeartbeat(consumerId);
            logger.debug("Leader {} found no new messages, updated heartbeat", consumerId);
        }

        return messages;
    }
    
    /**
     * Gets information about a consumer group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @return Consumer group information, or null if not found
     */
    public ConsumerGroup getConsumerGroupInfo(String topicName, String groupId) {
        return groupManager.getConsumerGroup(topicName, groupId);
    }
    
    /**
     * Gets all consumer groups for a topic.
     * @param topicName The topic name
     * @return Map of group ID to consumer group
     */
    public Map<String, ConsumerGroup> getConsumerGroupsForTopic(String topicName) {
        return groupManager.getConsumerGroupsForTopic(topicName);
    }
    
    /**
     * Gets all topics with consumer groups.
     * @return Set of topic names
     */
    public Set<String> getTopicsWithGroups() {
        return groupManager.getTopicsWithGroups();
    }
    
    /**
     * Gets statistics about consumer groups.
     * @return Statistics map
     */
    public Map<String, Object> getStatistics() {
        return groupManager.getStatistics();
    }
    
    /**
     * Gets detailed information about all consumer groups.
     * @return Detailed information string
     */
    public String getDetailedInfo() {
        return groupManager.getDetailedInfo();
    }
    
    /**
     * Cleans up inactive consumer groups.
     * @return Number of groups cleaned up
     */
    public int cleanupInactiveGroups() {
        return groupManager.cleanupInactiveGroups();
    }
    
    /**
     * Gets the next offset for a consumer group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @return The next offset, or 0 if group not found
     */
    public long getNextOffset(String topicName, String groupId) {
        ConsumerGroup group = groupManager.getConsumerGroup(topicName, groupId);
        if (group == null) {
            return 0L;
        }
        return group.getGroupOffset();
    }
    
    /**
     * Checks if a consumer group exists.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @return true if the group exists, false otherwise
     */
    public boolean consumerGroupExists(String topicName, String groupId) {
        return groupManager.getConsumerGroup(topicName, groupId) != null;
    }
    
    /**
     * Gets the consumer ID for a consumer in a group.
     * @param topicName The topic name
     * @param groupId The consumer group ID
     * @param consumerId The consumer ID
     * @return The assigned consumer ID (0-based index), or -1 if not found
     */
    public int getConsumerId(String topicName, String groupId, String consumerId) {
        ConsumerGroup group = groupManager.getConsumerGroup(topicName, groupId);
        if (group == null) {
            return -1;
        }
        // In leader-based model, we don't need assigned IDs
        return 0;
    }
}
