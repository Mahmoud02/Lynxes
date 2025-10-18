package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.ResponseUtils;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import org.mahmoud.lynxes.service.MessageService;
import org.mahmoud.lynxes.service.TopicService;
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
    private final TopicService topicService;
    private final AtomicLong processedCount;
    private final AtomicLong errorCount;
    
    @Inject
    public MetricsRequestProcessor(MessageService messageService, TopicService topicService, AtomicLong processedCount, AtomicLong errorCount) {
        this.messageService = messageService;
        this.topicService = topicService;
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
        
        ResponseUtils.sendSuccessResponse(request, responseBody);
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.METRICS;
    }
}
