package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.ResponseUtils;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Processor for metrics requests.
 * Handles METRICS request type.
 */
public class MetricsRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MetricsRequestProcessor.class);
    
    @Inject
    public MetricsRequestProcessor() {
        // No dependencies needed for simple metrics
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing metrics request: {}", request.getRequestId());
        
        // Return simple empty metrics response
        String responseBody = "{}";
        
        ResponseUtils.sendSuccessResponse(request, responseBody);
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.METRICS;
    }
}
