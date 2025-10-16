package org.mahmoud.lynxes.api.consumer;

import org.mahmoud.lynxes.api.topic.Topic;
import org.mahmoud.lynxes.api.topic.TopicRegistry;
import org.mahmoud.lynxes.core.Record;
import org.mahmoud.lynxes.config.QueueConfig;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer for reading messages from topics.
 * Manages topic connections and provides offset tracking.
 */
public class Consumer {
    private final QueueConfig config;
    private final ConcurrentHashMap<String, Topic> topics;
    private final ConcurrentHashMap<String, AtomicLong> offsets;
    private final AtomicLong messageCounter;
    private volatile boolean closed;

    /**
     * Creates a new consumer with the specified configuration.
     * 
     * @param config The queue configuration
     */
    @Inject
    public Consumer(QueueConfig config) {
        this.config = config;
        this.topics = new ConcurrentHashMap<>();
        this.offsets = new ConcurrentHashMap<>();
        this.messageCounter = new AtomicLong(0);
        this.closed = false;
    }

    /**
     * Consumes a message from the specified topic.
     * 
     * @param topicName The topic name
     * @return The consumed record, or null if no message available
     * @throws IOException if consumption fails
     * @throws IllegalStateException if the consumer is closed
     */
    public Record consume(String topicName) throws IOException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        
        Topic topic = getOrCreateTopic(topicName);
        AtomicLong offset = getOrCreateOffset(topicName);
        
        long currentOffset = offset.get();
        Record record = topic.consume(currentOffset);
        
        if (record != null) {
            offset.incrementAndGet();
            messageCounter.incrementAndGet();
        }
        
        return record;
    }

    /**
     * Consumes a message from the specified topic by offset.
     * 
     * @param topicName The topic name
     * @param offset The message offset
     * @return The consumed record, or null if not found
     * @throws IOException if consumption fails
     * @throws IllegalStateException if the consumer is closed
     */
    public Record consume(String topicName, long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        
        Topic topic = getOrCreateTopic(topicName);
        return topic.consume(offset);
    }

    /**
     * Consumes a message from the specified topic without deserializing.
     * 
     * @param topicName The topic name
     * @return The raw message data, or null if no message available
     * @throws IOException if consumption fails
     * @throws IllegalStateException if the consumer is closed
     */
    public byte[] consumeRaw(String topicName) throws IOException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        
        Topic topic = getOrCreateTopic(topicName);
        AtomicLong offset = getOrCreateOffset(topicName);
        
        long currentOffset = offset.get();
        byte[] data = topic.consumeRaw(currentOffset);
        
        if (data != null) {
            offset.incrementAndGet();
            messageCounter.incrementAndGet();
        }
        
        return data;
    }

    /**
     * Consumes a message from the specified topic by offset without deserializing.
     * 
     * @param topicName The topic name
     * @param offset The message offset
     * @return The raw message data, or null if not found
     * @throws IOException if consumption fails
     * @throws IllegalStateException if the consumer is closed
     */
    public byte[] consumeRaw(String topicName, long offset) throws IOException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        
        Topic topic = getOrCreateTopic(topicName);
        return topic.consumeRaw(offset);
    }

    /**
     * Consumes multiple messages from the specified topic.
     * 
     * @param topicName The topic name
     * @param maxMessages Maximum number of messages to consume
     * @return Array of consumed records
     * @throws IOException if consumption fails
     * @throws IllegalStateException if the consumer is closed
     */
    public Record[] consumeBatch(String topicName, int maxMessages) throws IOException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        
        if (maxMessages <= 0) {
            return new Record[0];
        }
        
        Topic topic = getOrCreateTopic(topicName);
        AtomicLong offset = getOrCreateOffset(topicName);
        
        Record[] records = new Record[maxMessages];
        int count = 0;
        
        for (int i = 0; i < maxMessages; i++) {
            long currentOffset = offset.get();
            Record record = topic.consume(currentOffset);
            
            if (record == null) {
                break;
            }
            
            records[count++] = record;
            offset.incrementAndGet();
            messageCounter.incrementAndGet();
        }
        
        // Resize array if we got fewer messages than requested
        if (count < maxMessages) {
            Record[] result = new Record[count];
            System.arraycopy(records, 0, result, 0, count);
            return result;
        }
        
        return records;
    }

    /**
     * Gets the current offset for a topic.
     * 
     * @param topicName The topic name
     * @return The current offset
     */
    public long getOffset(String topicName) {
        AtomicLong offset = offsets.get(topicName);
        return offset != null ? offset.get() : 0;
    }

    /**
     * Sets the offset for a topic.
     * 
     * @param topicName The topic name
     * @param offset The new offset
     */
    public void setOffset(String topicName, long offset) {
        getOrCreateOffset(topicName).set(offset);
    }

    /**
     * Gets or creates a topic connection.
     * 
     * @param topicName The topic name
     * @return The topic instance
     * @throws IOException if the topic cannot be created
     */
    private Topic getOrCreateTopic(String topicName) throws IOException {
        return TopicRegistry.getOrCreateTopic(topicName, config);
    }

    /**
     * Gets or creates an offset tracker for a topic.
     * 
     * @param topicName The topic name
     * @return The offset tracker
     */
    private AtomicLong getOrCreateOffset(String topicName) {
        return offsets.computeIfAbsent(topicName, _ -> new AtomicLong(0));
    }

    /**
     * Gets a topic by name.
     * 
     * @param topicName The topic name
     * @return The topic instance, or null if not found
     */
    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    /**
     * Gets all topic names.
     * 
     * @return Array of topic names
     */
    public String[] getTopicNames() {
        return topics.keySet().toArray(new String[0]);
    }

    /**
     * Gets the number of topics.
     * 
     * @return The topic count
     */
    public int getTopicCount() {
        return topics.size();
    }

    /**
     * Gets the total number of messages consumed.
     * 
     * @return The message count
     */
    public long getMessageCount() {
        return messageCounter.get();
    }

    /**
     * Checks if the consumer is closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes the consumer and releases resources.
     * 
     * @throws IOException if the operation fails
     */
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        try {
            // Close all topics
            for (Topic topic : topics.values()) {
                topic.close();
            }
            
            topics.clear();
            offsets.clear();
            closed = true;
        } catch (IOException e) {
            // Close remaining topics even if some fail
            for (Topic topic : topics.values()) {
                try {
                    topic.close();
                } catch (IOException ignored) {
                    // Ignore errors during cleanup
                }
            }
            throw e;
        }
    }

    @Override
    public String toString() {
        return String.format("Consumer{topics=%d, messageCount=%d, closed=%s}", 
                           getTopicCount(), getMessageCount(), closed);
    }
}
