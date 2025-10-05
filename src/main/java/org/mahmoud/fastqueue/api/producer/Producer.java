package org.mahmoud.fastqueue.api.producer;

import org.mahmoud.fastqueue.api.topic.Topic;
import org.mahmoud.fastqueue.api.topic.TopicRegistry;
import org.mahmoud.fastqueue.core.Record;
import org.mahmoud.fastqueue.config.QueueConfig;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer for publishing messages to topics.
 * Manages topic connections and provides batching capabilities.
 */
public class Producer {
    private final QueueConfig config;
    private final ConcurrentHashMap<String, Topic> topics;
    private final AtomicLong messageCounter;
    private volatile boolean closed;

    /**
     * Creates a new producer with the specified configuration.
     * 
     * @param config The queue configuration
     */
    @Inject
    public Producer(QueueConfig config) {
        this.config = config;
        this.topics = new ConcurrentHashMap<>();
        this.messageCounter = new AtomicLong(0);
        this.closed = false;
    }

    /**
     * Publishes a message to the specified topic.
     * 
     * @param topicName The topic name
     * @param data The message data
     * @return The published record
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the producer is closed
     */
    public Record publish(String topicName, byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Producer is closed");
        }
        
        Topic topic = getOrCreateTopic(topicName);
        Record record = topic.publish(data);
        messageCounter.incrementAndGet();
        return record;
    }

    /**
     * Publishes a message to the specified topic with a specific offset.
     * 
     * @param topicName The topic name
     * @param offset The specific offset to use
     * @param data The message data
     * @return The published record
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the producer is closed
     */
    public Record publish(String topicName, long offset, byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Producer is closed");
        }
        
        Topic topic = getOrCreateTopic(topicName);
        return topic.publish(offset, data);
    }

    /**
     * Publishes a string message to the specified topic.
     * 
     * @param topicName The topic name
     * @param message The string message
     * @return The published record
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the producer is closed
     */
    public Record publishString(String topicName, String message) throws IOException {
        Record record = publish(topicName, message.getBytes());
        // messageCounter is already incremented in publish()
        return record;
    }

    /**
     * Publishes a JSON message to the specified topic.
     * 
     * @param topicName The topic name
     * @param jsonMessage The JSON message
     * @return The published record
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the producer is closed
     */
    public Record publishJson(String topicName, String jsonMessage) throws IOException {
        return publish(topicName, jsonMessage.getBytes());
    }

    /**
     * Publishes multiple messages to the specified topic in a batch.
     * 
     * @param topicName The topic name
     * @param messages The array of message data
     * @return Array of published records
     * @throws IOException if publishing fails
     * @throws IllegalStateException if the producer is closed
     */
    public Record[] publishBatch(String topicName, byte[][] messages) throws IOException {
        if (closed) {
            throw new IllegalStateException("Producer is closed");
        }
        
        if (messages == null || messages.length == 0) {
            return new Record[0];
        }
        
        Topic topic = getOrCreateTopic(topicName);
        Record[] records = new Record[messages.length];
        
        for (int i = 0; i < messages.length; i++) {
            records[i] = topic.publish(messages[i]);
            messageCounter.incrementAndGet();
        }
        
        return records;
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
     * Gets the total number of messages published.
     * 
     * @return The message count
     */
    public long getMessageCount() {
        return messageCounter.get();
    }

    /**
     * Flushes all pending changes to disk.
     * 
     * @throws IOException if the operation fails
     */
    public void flush() throws IOException {
        if (closed) {
            throw new IllegalStateException("Producer is closed");
        }
        
        for (Topic topic : topics.values()) {
            topic.flush();
        }
    }

    /**
     * Checks if the producer is closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes the producer and releases resources.
     * 
     * @throws IOException if the operation fails
     */
    public void close() throws IOException {
        if (closed) {
            return;
        }
        
        try {
            // Flush all topics
            flush();
            
            // Close all topics
            for (Topic topic : topics.values()) {
                topic.close();
            }
            
            topics.clear();
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
        return String.format("Producer{topics=%d, messageCount=%d, closed=%s}", 
                           getTopicCount(), getMessageCount(), closed);
    }
}
