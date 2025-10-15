package org.mahmoud.lynxes.server.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Processes responses from the ResponseChannel using a pool of network threads.
 * Similar to Kafka's network thread handling of responses.
 */
public class ResponseProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ResponseProcessor.class);

    private final ResponseChannel responseChannel;
    private final ExecutorService networkThreadPool;
    private final AtomicBoolean running;
    private final AtomicLong processedCount;
    private final AtomicLong errorCount;

    public ResponseProcessor(ResponseChannel responseChannel, ExecutorService networkThreadPool) {
        this.responseChannel = responseChannel;
        this.networkThreadPool = networkThreadPool;
        this.running = new AtomicBoolean(false);
        this.processedCount = new AtomicLong(0);
        this.errorCount = new AtomicLong(0);
        
        logger.info("ResponseProcessor initialized");
    }

    /**
     * Starts the response processor.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting ResponseProcessor...");
            
            // Start network thread for response handling
            networkThreadPool.submit(new NetworkWorker());
            
            logger.info("ResponseProcessor started successfully");
        } else {
            logger.warn("ResponseProcessor is already running");
        }
    }

    /**
     * Stops the response processor.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping ResponseProcessor...");
            networkThreadPool.shutdown();
            try {
                if (!networkThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Network thread pool did not terminate in time, forcing shutdown.");
                    networkThreadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for network thread pool to terminate.", e);
                networkThreadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("ResponseProcessor stopped.");
        } else {
            logger.warn("ResponseProcessor is not running.");
        }
    }

    /**
     * Worker thread that continuously takes responses from the ResponseChannel and sends them.
     */
    private class NetworkWorker implements Runnable {
        @Override
        public void run() {
            logger.info("Network worker thread started: {}", Thread.currentThread().getName());
            while (running.get()) {
                try {
                    AsyncResponse response = responseChannel.takeResponse();
                    processResponse(response);
                } catch (InterruptedException e) {
                    logger.warn("Network worker thread interrupted: {}", Thread.currentThread().getName());
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Unhandled exception in network worker thread: {}", Thread.currentThread().getName(), e);
                }
            }
            logger.info("Network worker thread stopped: {}", Thread.currentThread().getName());
        }
    }

    private void processResponse(AsyncResponse response) {
        try {
            response.send();
            responseChannel.markResponseSent();
            processedCount.incrementAndGet();
            logger.debug("Response processed successfully: {}", response.getRequestId());
        } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("Error processing response {}: {}", response.getRequestId(), e.getMessage(), e);
        }
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }
}
