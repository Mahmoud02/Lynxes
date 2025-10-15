package org.mahmoud.lynxes.service;

import com.google.inject.Inject;

/**
 * Health check service for monitoring application status.
 */
public class HealthService {
    private final MessageService messageService;
    
    @Inject
    public HealthService(MessageService messageService) {
        this.messageService = messageService;
    }
    
    /**
     * Performs a health check.
     */
    public HealthStatus checkHealth() {
        try {
            // Simple health check - could be more sophisticated
            return new HealthStatus("ok", "FastQueue2 is running");
        } catch (Exception e) {
            return new HealthStatus("error", "Health check failed: " + e.getMessage());
        }
    }
    
    /**
     * Health status result.
     */
    public static class HealthStatus {
        private final String status;
        private final String message;
        
        public HealthStatus(String status, String message) {
            this.status = status;
            this.message = message;
        }
        
        public String getStatus() { return status; }
        public String getMessage() { return message; }
    }
}
