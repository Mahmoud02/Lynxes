package org.mahmoud.lynxes.api.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsumerGroup {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);
    
    private final String groupId;
    private final String topicName;
    private final Map<String, Consumer> consumers; // All registered consumers
    private final AtomicLong groupOffset; // Last consumed offset by the group
    private final ReadWriteLock lock;
    
    // Leader management
    private volatile String currentLeaderId;
    private volatile long lastHeartbeat; // Last time leader consumed messages
    private static final long HEARTBEAT_TIMEOUT_MS = 10_000; // 10 seconds
    
    public ConsumerGroup(String groupId, String topicName) {
        this.groupId = groupId;
        this.topicName = topicName;
        this.consumers = new ConcurrentHashMap<>();
        this.groupOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.currentLeaderId = null;
        this.lastHeartbeat = 0;
        logger.info("ConsumerGroup {} for topic {} created with leader-based model.", groupId, topicName);
    }
    
    /**
     * Adds a consumer to the group. Does not automatically make it leader.
     */
    public void addConsumer(String consumerId, Consumer consumer) {
        lock.writeLock().lock();
        try {
            consumers.put(consumerId, consumer);
            logger.info("Consumer {} added to group {} for topic {}. Current group size: {}", 
                       consumerId, groupId, topicName, getGroupSize());
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Removes a consumer from the group. If it was the leader, triggers leader election.
     */
    public void removeConsumer(String consumerId) {
        lock.writeLock().lock();
        try {
            if (consumers.remove(consumerId) != null) {
                if (consumerId.equals(currentLeaderId)) {
                    logger.info("Leader {} removed from group {}. Triggering leader election.", 
                               consumerId, groupId);
                    currentLeaderId = null;
                    lastHeartbeat = 0;
                }
                logger.info("Consumer {} removed from group {} for topic {}. Current group size: {}", 
                           consumerId, groupId, topicName, getGroupSize());
            } else {
                logger.warn("Consumer {} not found in group {} for topic {}.", consumerId, groupId, topicName);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Attempts to become the leader or returns current leader.
     * If no leader exists or current leader is stale, assigns new leader.
     */
    public String getOrAssignLeader(String requestingConsumerId) {
        lock.writeLock().lock();
        try {
            long currentTime = System.currentTimeMillis();
            
            // Check if current leader is still active
            if (currentLeaderId != null && (currentTime - lastHeartbeat) < HEARTBEAT_TIMEOUT_MS) {
                logger.debug("Consumer {} requesting leadership, but {} is still active leader", 
                           requestingConsumerId, currentLeaderId);
                return currentLeaderId;
            }
            
            // No leader or leader is stale - assign new leader
            if (consumers.containsKey(requestingConsumerId)) {
                currentLeaderId = requestingConsumerId;
                lastHeartbeat = currentTime;
                logger.info("Consumer {} assigned as new leader for group {} (previous leader was stale or none)", 
                           requestingConsumerId, groupId);
                return requestingConsumerId;
            } else {
                logger.warn("Consumer {} not found in group {}, cannot assign as leader", 
                           requestingConsumerId, groupId);
                return null;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Updates the heartbeat for the current leader.
     */
    public void updateHeartbeat(String consumerId) {
        lock.writeLock().lock();
        try {
            if (consumerId.equals(currentLeaderId)) {
                lastHeartbeat = System.currentTimeMillis();
                logger.debug("Updated heartbeat for leader {} in group {}", consumerId, groupId);
            } else {
                logger.warn("Consumer {} attempted to update heartbeat but is not the current leader {}", 
                           consumerId, currentLeaderId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Gets the current group offset (last consumed offset).
     */
    public long getGroupOffset() {
        return groupOffset.get();
    }
    
    /**
     * Updates the group offset after consuming messages.
     */
    public void updateGroupOffset(long newOffset) {
        groupOffset.updateAndGet(current -> Math.max(current, newOffset));
        logger.debug("Updated group {} offset to {}", groupId, groupOffset.get());
    }
    
    /**
     * Gets the current leader ID.
     */
    public String getCurrentLeaderId() {
        lock.readLock().lock();
        try {
            return currentLeaderId;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Gets the last heartbeat time.
     */
    public long getLastHeartbeat() {
        lock.readLock().lock();
        try {
            return lastHeartbeat;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Checks if the current leader is stale (hasn't consumed for too long).
     */
    public boolean isLeaderStale() {
        lock.readLock().lock();
        try {
            if (currentLeaderId == null) return true;
            long currentTime = System.currentTimeMillis();
            return (currentTime - lastHeartbeat) >= HEARTBEAT_TIMEOUT_MS;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public int getGroupSize() {
        lock.readLock().lock();
        try {
            return consumers.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public String getGroupId() {
        return groupId;
    }
    
    public String getTopicName() {
        return topicName;
    }
    
    public List<String> getConsumerIds() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(consumers.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public String toString() {
        return "ConsumerGroup{" +
               "groupId='" + groupId + '\'' +
               ", topicName='" + topicName + '\'' +
               ", groupSize=" + getGroupSize() +
               ", groupOffset=" + groupOffset.get() +
               ", currentLeader='" + currentLeaderId + '\'' +
               ", lastHeartbeat=" + lastHeartbeat +
               ", consumers=" + consumers.keySet() +
               '}';
    }
}