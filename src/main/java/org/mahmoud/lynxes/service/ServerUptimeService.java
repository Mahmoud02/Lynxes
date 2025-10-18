package org.mahmoud.lynxes.service;

import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * Service to track server uptime in RAM.
 * This ensures uptime persists across browser sessions and page refreshes.
 */
@Singleton
public class ServerUptimeService {
    private static final Logger logger = LoggerFactory.getLogger(ServerUptimeService.class);
    
    private final Instant serverStartTime;
    
    public ServerUptimeService() {
        this.serverStartTime = Instant.now();
        logger.info("ServerUptimeService initialized. Server started at: {}", serverStartTime);
    }
    
    /**
     * Get the current server uptime in milliseconds.
     * @return uptime in milliseconds
     */
    public long getUptimeMillis() {
        return Duration.between(serverStartTime, Instant.now()).toMillis();
    }
    
    /**
     * Get the current server uptime formatted as a human-readable string.
     * @return formatted uptime string (e.g., "2h 15m 30s")
     */
    public String getFormattedUptime() {
        Duration uptime = Duration.between(serverStartTime, Instant.now());
        
        long hours = uptime.toHours();
        long minutes = uptime.toMinutesPart();
        long seconds = uptime.toSecondsPart();
        
        if (hours > 0) {
            return String.format("%dh %dm %ds", hours, minutes, seconds);
        } else if (minutes > 0) {
            return String.format("%dm %ds", minutes, seconds);
        } else {
            return String.format("%ds", seconds);
        }
    }
    
    /**
     * Get the server start time.
     * @return server start time as Instant
     */
    public Instant getServerStartTime() {
        return serverStartTime;
    }
}
