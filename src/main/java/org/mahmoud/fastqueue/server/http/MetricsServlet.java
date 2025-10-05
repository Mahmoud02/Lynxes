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
import org.mahmoud.fastqueue.service.MessageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Metrics servlet with Swagger annotations.
 */
@Tag(name = "Metrics", description = "System metrics and monitoring")
public class MetricsServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServlet.class);
    
    private final MessageService messageService;
    private final ObjectMapper objectMapper;
    
    public MetricsServlet(MessageService messageService, ObjectMapper objectMapper) {
        this.messageService = messageService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    @Operation(
        summary = "Get Metrics",
        description = "Get system metrics and statistics"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Metrics retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = MetricsResponse.class)
            )
        )
    })
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("producerMessages", messageService.getProducerMessageCount());
        metrics.put("consumerMessages", messageService.getConsumerMessageCount());
        metrics.put("processedRequests", 0); // TODO: Implement request counting
        metrics.put("errorCount", 0); // TODO: Implement error counting
        
        String json = objectMapper.writeValueAsString(metrics);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }
    
    /**
     * Metrics response model.
     */
    @Schema(description = "System metrics response")
    public static class MetricsResponse {
        @Schema(description = "Number of messages published", example = "1234")
        public long producerMessages;
        
        @Schema(description = "Number of messages consumed", example = "1200")
        public long consumerMessages;
        
        @Schema(description = "Number of processed requests", example = "5678")
        public long processedRequests;
        
        @Schema(description = "Number of errors", example = "5")
        public long errorCount;
    }
}
