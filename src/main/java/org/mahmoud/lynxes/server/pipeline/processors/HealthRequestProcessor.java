package org.mahmoud.lynxes.server.pipeline.processors;

import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.mahmoud.lynxes.server.pipeline.core.ResponseUtils;
import org.mahmoud.lynxes.server.pipeline.orchestration.RequestProcessor;
import org.mahmoud.lynxes.service.HealthService;
import org.mahmoud.lynxes.service.ServerUptimeService;
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
    private final ServerUptimeService serverUptimeService;
    
    @Inject
    public HealthRequestProcessor(HealthService healthService, ServerUptimeService serverUptimeService) {
        this.healthService = healthService;
        this.serverUptimeService = serverUptimeService;
    }
    
    @Override
    public void process(AsyncRequest request) throws IOException {
        logger.debug("Processing health check request: {}", request.getRequestId());
        
        HealthService.HealthStatus health = healthService.checkHealth();
        String uptime = serverUptimeService.getFormattedUptime();
        
        String responseBody = String.format("{\"status\":\"%s\",\"message\":\"%s\",\"uptime\":\"%s\"}", 
                                      health.getStatus(), health.getMessage(), uptime);
        
        ResponseUtils.sendSuccessResponse(request, responseBody);
    }
    
    @Override
    public boolean canProcess(AsyncRequest.RequestType type) {
        return type == AsyncRequest.RequestType.HEALTH;
    }
}
