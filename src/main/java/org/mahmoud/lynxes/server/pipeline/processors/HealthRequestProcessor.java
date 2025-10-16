package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import org.mahmoud.lynxes.service.HealthService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Processor for health check requests.
 * Handles HEALTH request type.
 */
public class HealthRequestProcessor implements RequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HealthRequestProcessor.class);
    
    private final HealthService healthService;
    
    @Inject
    public HealthRequestProcessor(HealthService healthService) {
        this.healthService = healthService;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing health check request: {}", request.getRequestId());
        
        HealthService.HealthStatus health = healthService.checkHealth();
        String responseBody = String.format("{\"status\":\"%s\",\"message\":\"%s\"}", 
                                      health.getStatus(), health.getMessage());
        
        sendResponse(request, 200, "application/json", responseBody);
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.HEALTH;
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
