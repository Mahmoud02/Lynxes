package org.mahmoud.lynxes.server.pipeline.orchestration;

import com.google.inject.Inject;
import org.mahmoud.lynxes.server.pipeline.channels.RequestChannel;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrator for async request processing.
 * Routes requests to appropriate processors using the strategy pattern.
 * This class focuses purely on orchestration - all business logic is handled by individual processors.
 */
public class AsyncRequestProcessorOrchestrator {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRequestProcessorOrchestrator.class);
    
    private final RequestChannel requestChannel;
    private final RequestProcessorFactory processorFactory;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    
    @Inject
    public AsyncRequestProcessorOrchestrator(RequestChannel requestChannel, RequestProcessorFactory processorFactory, ExecutorService executorService) {
        this.requestChannel = requestChannel;
        this.processorFactory = processorFactory;
        this.executorService = executorService;
    }
    
    /**
     * Starts the orchestrator.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting AsyncRequestProcessorOrchestrator...");
            
            // Start I/O worker
            executorService.submit(new IoWorker());
            
            logger.info("AsyncRequestProcessorOrchestrator started successfully");
        } else {
            logger.warn("AsyncRequestProcessorOrchestrator is already running");
        }
    }
    
    /**
     * Stops the orchestrator.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping AsyncRequestProcessorOrchestrator...");
            executorService.shutdown();
            logger.info("AsyncRequestProcessorOrchestrator stopped successfully");
        } else {
            logger.warn("AsyncRequestProcessorOrchestrator is not running");
        }
    }
    
    /**
     * I/O worker thread that processes requests from the queue.
     */
    private class IoWorker implements Runnable {
        @Override
        public void run() {
            logger.info("I/O worker thread started: {}", Thread.currentThread().getName());
            
            while (running.get()) {
                try {
                    AsyncRequest request = requestChannel.takeRequest();
                    processRequest(request);
                } catch (InterruptedException e) {
                    logger.info("I/O worker thread interrupted: {}", Thread.currentThread().getName());
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in I/O worker thread", e);
                    errorCount.incrementAndGet();
                }
            }
            
            logger.info("I/O worker thread stopped: {}", Thread.currentThread().getName());
        }
    }
    
    /**
     * Processes an async request by routing it to the appropriate processor.
     * 
     * @param request The async request to process
     */
    private void processRequest(AsyncRequest request) {
        try {
            logger.debug("Processing request: {} of type: {}", request.getRequestId(), request.getType());
            
            // Get the appropriate processor for this request type
            RequestProcessor processor = processorFactory.getProcessor(request.getType());
            
            // Process the request
            processor.process(request);
            
            // Update metrics
            processedCount.incrementAndGet();
            logger.debug("Successfully processed request: {}", request.getRequestId());
            
        } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("Error processing request: {} of type: {}", request.getRequestId(), request.getType(), e);
            
            // Send error response
            try {
                sendErrorResponse(request, 500, "Internal server error: " + e.getMessage());
            } catch (IOException ioException) {
                logger.error("Failed to send error response for request: {}", request.getRequestId(), ioException);
            }
        }
    }
    
    /**
     * Gets the number of processed requests.
     * 
     * @return The processed count
     */
    public long getProcessedCount() {
        return processedCount.get();
    }
    
    /**
     * Gets the number of errors.
     * 
     * @return The error count
     */
    public long getErrorCount() {
        return errorCount.get();
    }
    
    /**
     * Sends an error response back to the client.
     */
    private void sendErrorResponse(AsyncRequest request, int statusCode, String message) throws IOException {
        String errorBody = String.format("{\"error\":\"%s\"}", message);
        request.getResponse().setStatus(statusCode);
        request.getResponse().setContentType("application/json");
        request.getResponse().getWriter().write(errorBody);
        request.getAsyncContext().complete();
    }
}
