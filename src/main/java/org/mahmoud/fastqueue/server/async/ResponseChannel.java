package org.mahmoud.fastqueue.server.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A channel for passing responses from I/O threads to network threads.
 * This decouples response processing from response sending, similar to Kafka's response queue.
 */
public class ResponseChannel {
    private static final Logger logger = LoggerFactory.getLogger(ResponseChannel.class);

    private final BlockingQueue<AsyncResponse> responseQueue;
    private final int maxQueueSize;
    private final AtomicLong totalResponses;
    private final AtomicLong sentResponses;
    private final AtomicLong rejectedResponses;

    public ResponseChannel(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        this.responseQueue = new ArrayBlockingQueue<>(maxQueueSize);
        this.totalResponses = new AtomicLong(0);
        this.sentResponses = new AtomicLong(0);
        this.rejectedResponses = new AtomicLong(0);
        logger.info("ResponseChannel initialized with max queue size: {}", maxQueueSize);
    }

    /**
     * Adds a response to the channel.
     * @param response The AsyncResponse to add.
     * @return true if the response was added, false if the queue is full.
     */
    public boolean addResponse(AsyncResponse response) {
        totalResponses.incrementAndGet();
        if (responseQueue.offer(response)) {
            logger.debug("Response {} added to queue. Current size: {}", response.getRequestId(), responseQueue.size());
            return true;
        } else {
            rejectedResponses.incrementAndGet();
            logger.warn("Response queue is full, rejecting response: {}", response.getRequestId());
            return false;
        }
    }

    /**
     * Takes a response from the channel, blocking if the queue is empty.
     * @return The next AsyncResponse.
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    public AsyncResponse takeResponse() throws InterruptedException {
        AsyncResponse response = responseQueue.take();
        logger.debug("Response {} taken from queue. Current size: {}", response.getRequestId(), responseQueue.size());
        return response;
    }

    /**
     * Marks a response as sent.
     */
    public void markResponseSent() {
        sentResponses.incrementAndGet();
    }

    public int queueSize() {
        return responseQueue.size();
    }

    public int maxQueueSize() {
        return maxQueueSize;
    }

    public long getTotalResponses() {
        return totalResponses.get();
    }

    public long getSentResponses() {
        return sentResponses.get();
    }

    public long getRejectedResponses() {
        return rejectedResponses.get();
    }

    public double getUtilization() {
        return (double) responseQueue.size() / maxQueueSize;
    }

    @Override
    public String toString() {
        return String.format("ResponseChannel{queueSize=%d/%d, responses=%d, sent=%d, rejected=%d, utilization=%.2f%%}",
                responseQueue.size(), maxQueueSize, totalResponses.get(), sentResponses.get(), rejectedResponses.get(), getUtilization() * 100);
    }
}
