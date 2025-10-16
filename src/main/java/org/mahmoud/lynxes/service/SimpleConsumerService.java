package org.mahmoud.lynxes.service;

import org.mahmoud.lynxes.api.consumer.Consumer;
import org.mahmoud.lynxes.api.topic.TopicRegistry;
import org.mahmoud.lynxes.core.Record;
import org.mahmoud.lynxes.config.QueueConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple consumer service without consumer groups.
 * Each consumer ID represents a different client/application.
 * All consumers get ALL messages from topics (broadcast model).
 */
public class SimpleConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerService.class);

    private final TopicRegistry topicRegistry;
    private final QueueConfig config;
    
    // Map: consumerId -> Consumer instance
    private final Map<String, Consumer> consumers;
    
    // Map: consumerId -> current offset for each topic
    private final Map<String, Map<String, Long>> consumerOffsets;

    @Inject
    public SimpleConsumerService(TopicRegistry topicRegistry, QueueConfig config) {
        this.topicRegistry = topicRegistry;
        this.config = config;
        this.consumers = new ConcurrentHashMap<>();
        this.consumerOffsets = new ConcurrentHashMap<>();
        logger.info("SimpleConsumerService initialized");
    }

    /**
     * Registers a consumer with a unique consumer ID.
     * @param consumerId The unique consumer ID (acts as client identifier)
     * @return true if registered successfully, false if already exists
     */
    public boolean registerConsumer(String consumerId) {
        if (consumers.containsKey(consumerId)) {
            logger.warn("Consumer {} already exists", consumerId);
            return false;
        }
        
        Consumer consumer = new Consumer(config);
        consumers.put(consumerId, consumer);
        consumerOffsets.put(consumerId, new ConcurrentHashMap<>());
        
        logger.info("Consumer {} registered successfully", consumerId);
        return true;
    }

    /**
     * Unregisters a consumer.
     * @param consumerId The consumer ID to unregister
     * @return true if unregistered successfully, false if not found
     */
    public boolean unregisterConsumer(String consumerId) {
        Consumer removed = consumers.remove(consumerId);
        consumerOffsets.remove(consumerId);
        
        if (removed != null) {
            logger.info("Consumer {} unregistered successfully", consumerId);
            return true;
        } else {
            logger.warn("Consumer {} not found", consumerId);
            return false;
        }
    }

    /**
     * Consumes messages for a specific consumer from a topic.
     * Each consumer gets ALL messages (broadcast model).
     * 
     * @param consumerId The consumer ID
     * @param topicName The topic name
     * @param offset The starting offset
     * @param maxMessages Maximum number of messages to return
     * @return List of messages for this consumer
     */
    public List<Record> consumeMessages(String consumerId, String topicName, 
                                       long offset, int maxMessages) {
        logger.debug("Consumer {} consuming messages from topic {} starting at offset {}", 
                    consumerId, topicName, offset);
        
        // Check if consumer exists
        if (!consumers.containsKey(consumerId)) {
            logger.warn("Consumer {} not found", consumerId);
            return Collections.emptyList();
        }
        
        // Consume messages one by one - ALL messages for this consumer (broadcast)
        List<Record> consumerMessages = new ArrayList<>();
        long currentOffset = offset;
        long lastProcessedOffset = offset - 1;
        
        while (consumerMessages.size() < maxMessages) {
            try {
                Record message = topicRegistry.getOrCreateTopic(topicName, config).consume(currentOffset);
                if (message == null) {
                    break; // No more messages
                }
                
                // Add ALL messages to this consumer (broadcast)
                consumerMessages.add(message);
                lastProcessedOffset = Math.max(lastProcessedOffset, message.getOffset());
                
                currentOffset++;
            } catch (Exception e) {
                logger.debug("No more messages at offset {}", currentOffset);
                break;
            }
        }
        
        // Update this consumer's offset for this topic
        if (lastProcessedOffset >= offset) {
            updateConsumerOffset(consumerId, topicName, lastProcessedOffset + 1);
        }
        
        logger.debug("Consumer {} consumed {} messages from topic {}", 
                    consumerId, consumerMessages.size(), topicName);
        
        return consumerMessages;
    }

    /**
     * Gets the current offset for a consumer on a specific topic.
     * @param consumerId The consumer ID
     * @param topicName The topic name
     * @return The consumer's offset, or 0 if not found
     */
    public long getConsumerOffset(String consumerId, String topicName) {
        Map<String, Long> topicOffsets = consumerOffsets.get(consumerId);
        if (topicOffsets == null) {
            return 0L;
        }
        return topicOffsets.getOrDefault(topicName, 0L);
    }

    /**
     * Updates the offset for a consumer on a specific topic.
     * @param consumerId The consumer ID
     * @param topicName The topic name
     * @param newOffset The new offset
     */
    private void updateConsumerOffset(String consumerId, String topicName, long newOffset) {
        Map<String, Long> topicOffsets = consumerOffsets.get(consumerId);
        if (topicOffsets != null) {
            long currentOffset = topicOffsets.getOrDefault(topicName, 0L);
            if (newOffset > currentOffset) {
                topicOffsets.put(topicName, newOffset);
                logger.debug("Updated consumer {} offset to {} for topic {}", consumerId, newOffset, topicName);
            }
        }
    }

    /**
     * Lists all registered consumers.
     * @return List of consumer IDs
     */
    public List<String> listConsumers() {
        return new ArrayList<>(consumers.keySet());
    }

    /**
     * Gets information about a specific consumer.
     * @param consumerId The consumer ID
     * @return Consumer info, or null if not found
     */
    public ConsumerInfo getConsumerInfo(String consumerId) {
        if (!consumers.containsKey(consumerId)) {
            return null;
        }
        
        Map<String, Long> topicOffsets = consumerOffsets.get(consumerId);
        return new ConsumerInfo(consumerId, topicOffsets != null ? new HashMap<>(topicOffsets) : new HashMap<>());
    }

    /**
     * Gets the total number of registered consumers.
     * @return The total consumer count
     */
    public int getTotalConsumerCount() {
        return consumers.size();
    }

    /**
     * Checks if a consumer exists.
     * @param consumerId The consumer ID
     * @return true if exists, false otherwise
     */
    public boolean consumerExists(String consumerId) {
        return consumers.containsKey(consumerId);
    }

    /**
     * Data class for consumer information.
     */
    public static class ConsumerInfo {
        public final String consumerId;
        public final Map<String, Long> topicOffsets;
        public final long registeredAt;

        public ConsumerInfo(String consumerId, Map<String, Long> topicOffsets) {
            this.consumerId = consumerId;
            this.topicOffsets = topicOffsets;
            this.registeredAt = System.currentTimeMillis();
        }
    }
}
