package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.RequestProcessor;
import org.mahmoud.lynxes.service.MessageService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Processor for metrics requests.
 * Handles METRICS request type.
 */
public class MetricsRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MetricsRequestProcessor.class);
    
    private final MessageService messageService;
    private final AtomicLong processedCount;
    private final AtomicLong errorCount;
    
    @Inject
    public MetricsRequestProcessor(MessageService messageService, AtomicLong processedCount, AtomicLong errorCount) {
        this.messageService = messageService;
        this.processedCount = processedCount;
        this.errorCount = errorCount;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing metrics request: {}", request.getRequestId());
        
        String responseBody = String.format("{\"producerMessages\":%d,\"consumerMessages\":%d,\"processedRequests\":%d,\"errorCount\":%d}",
                                      messageService.getProducerMessageCount(),
                                      messageService.getConsumerMessageCount(),
                                      processedCount.get(), errorCount.get());
        
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.METRICS;
    }
    
    /**
     * Sends a response back to the client.
     */
    private void sendResponse(AsyncRequest request, int statusCode, String contentType, String body) throws IOException {
        request.getResponse().setStatus(statusCode);
        request.getResponse().setContentType(contentType);
        request.getResponse().getWriter().write(body);
        request.getAsyncContext().complete();
    }
}
