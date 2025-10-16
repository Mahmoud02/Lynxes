package org.mahmoud.lynxes.service;


/**
 * Health check service for monitoring application status.
 */
public class HealthService {
    
   
    /**
     * Performs a health check.
     */
    public HealthStatus checkHealth() {
        try {
            // Simple health check - could be more sophisticated
            return new HealthStatus("ok", "Lynxes is running");
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
