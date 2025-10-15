package org.mahmoud.lynxes.server.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Request channel that decouples network threads from I/O processing threads.
 * Similar to Kafka's RequestChannel but adapted for HTTP requests.
 */
public class RequestChannel {
    private static final Logger logger = LoggerFactory.getLogger(RequestChannel.class);
    
    private final BlockingQueue<AsyncRequest> requestQueue;
    private final int maxQueueSize;
    private final AtomicLong requestCount;
    private final AtomicLong completedCount;
    private final AtomicLong rejectedCount;
    
    public RequestChannel(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        this.requestQueue = new LinkedBlockingQueue<>(maxQueueSize);
        this.requestCount = new AtomicLong(0);
        this.completedCount = new AtomicLong(0);
        this.rejectedCount = new AtomicLong(0);
        
        logger.info("RequestChannel initialized with max queue size: {}", maxQueueSize);
    }
    
    /**
     * Adds a request to the queue.
     * Returns true if added successfully, false if queue is full.
     */
    public boolean addRequest(AsyncRequest request) {
        boolean added = requestQueue.offer(request);
        if (added) {
            requestCount.incrementAndGet();
            logger.debug("Request added to queue: {}", request.getRequestId());
        } else {
            rejectedCount.incrementAndGet();
            logger.warn("Request queue is full, rejecting request: {}", request.getRequestId());
        }
        return added;
    }
    
    /**
     * Takes a request from the queue.
     * Blocks until a request is available.
     */
    public AsyncRequest takeRequest() throws InterruptedException {
        AsyncRequest request = requestQueue.take();
        logger.debug("Request taken from queue: {}", request.getRequestId());
        return request;
    }
    
    /**
     * Polls for a request with timeout.
     * Returns null if no request available within timeout.
     */
    public AsyncRequest pollRequest(long timeoutMs) throws InterruptedException {
        AsyncRequest request = requestQueue.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (request != null) {
            logger.debug("Request polled from queue: {}", request.getRequestId());
        }
        return request;
    }
    
    /**
     * Marks a request as completed.
     */
    public void markCompleted(AsyncRequest request) {
        completedCount.incrementAndGet();
        logger.debug("Request completed: {}", request.getRequestId());
    }
    
    // Metrics getters
    public int getQueueSize() {
        return requestQueue.size();
    }
    
    public int getMaxQueueSize() {
        return maxQueueSize;
    }
    
    public long getRequestCount() {
        return requestCount.get();
    }
    
    public long getCompletedCount() {
        return completedCount.get();
    }
    
    public long getRejectedCount() {
        return rejectedCount.get();
    }
    
    public double getQueueUtilization() {
        return (double) getQueueSize() / maxQueueSize;
    }
    
    /**
     * Gets channel metrics as a string.
     */
    public String getMetrics() {
        return String.format("RequestChannel{queueSize=%d/%d, requests=%d, completed=%d, rejected=%d, utilization=%.2f%%}",
                           getQueueSize(), getMaxQueueSize(), getRequestCount(), 
                           getCompletedCount(), getRejectedCount(), getQueueUtilization() * 100);
    }
}
