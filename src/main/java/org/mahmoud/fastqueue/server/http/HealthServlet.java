package org.mahmoud.fastqueue.server.http;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mahmoud.fastqueue.service.HealthService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Health check servlet with Swagger annotations.
 */
@Tag(name = "Health", description = "Health check and system status endpoints")
public class HealthServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(HealthServlet.class);
    
    private final HealthService healthService;
    private final ObjectMapper objectMapper;
    
    public HealthServlet(HealthService healthService, ObjectMapper objectMapper) {
        this.healthService = healthService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    @Operation(
        summary = "Health Check",
        description = "Check the health status of the FastQueue2 server",
        tags = {"Health"}
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Server is healthy",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = HealthResponse.class)
            )
        )
    })
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        logger.debug("Health check requested from {}", request.getRemoteAddr());
        
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        
        HealthService.HealthStatus health = healthService.checkHealth();
        HealthResponse healthResponse = new HealthResponse(health.getStatus(), health.getMessage());
        String json = objectMapper.writeValueAsString(healthResponse);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
        
        logger.debug("Health check completed successfully");
    }
    
    /**
     * Health response model.
     */
    @Schema(description = "Health check response")
    public static class HealthResponse {
        @Schema(description = "Health status", example = "ok")
        public String status;
        
        @Schema(description = "Status message", example = "FastQueue2 is running")
        public String message;
        
        public HealthResponse(String status, String message) {
            this.status = status;
            this.message = message;
        }
    }
}
