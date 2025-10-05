package org.mahmoud.fastqueue.service;

import org.mahmoud.fastqueue.api.producer.Producer;
import org.mahmoud.fastqueue.api.consumer.Consumer;
import org.mahmoud.fastqueue.core.Record;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * High-level message service that coordinates between Producer and Consumer.
 * This service acts as a facade for message operations.
 */
public class MessageService {
    private final Producer producer;
    private final Consumer consumer;
    
    @Inject
    public MessageService(Producer producer, Consumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }
    
    /**
     * Publishes a message to a topic.
     */
    public Record publishMessage(String topicName, byte[] data) throws IOException {
        return producer.publish(topicName, data);
    }
    
    /**
     * Consumes a message from a topic at a specific offset.
     */
    public Record consumeMessage(String topicName, long offset) throws IOException {
        return consumer.consume(topicName, offset);
    }
    
    /**
     * Gets producer metrics.
     */
    public long getProducerMessageCount() {
        return producer.getMessageCount();
    }
    
    /**
     * Gets consumer metrics.
     */
    public long getConsumerMessageCount() {
        return consumer.getMessageCount();
    }
}
