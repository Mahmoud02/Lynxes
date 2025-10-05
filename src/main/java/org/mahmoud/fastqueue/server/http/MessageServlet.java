package org.mahmoud.fastqueue.server.http;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mahmoud.fastqueue.service.MessageService;
import org.mahmoud.fastqueue.core.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message handling servlet with Swagger annotations.
 */
@Tag(name = "Messages", description = "Message publishing and consumption")
public class MessageServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(MessageServlet.class);
    
    private final MessageService messageService;
    private final ObjectMapper objectMapper;
    private final Pattern topicPattern = Pattern.compile("/topics/([^/]+)");
    
    public MessageServlet(MessageService messageService, ObjectMapper objectMapper) {
        this.messageService = messageService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    @Operation(
        summary = "Publish Message",
        description = "Publish a message to a topic"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Message published successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = PublishResponse.class)
            )
        ),
        @ApiResponse(responseCode = "400", description = "Invalid request"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        String pathInfo = request.getPathInfo();
        Matcher matcher = topicPattern.matcher(pathInfo);
        
        if (!matcher.find()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        
        String topicName = matcher.group(1);
        logger.debug("Publishing message to topic: {}", topicName);
        
        try {
            // Read request body
            StringBuilder requestBody = new StringBuilder();
            String line;
            while ((line = request.getReader().readLine()) != null) {
                requestBody.append(line);
            }
            
            PublishRequest publishRequest = objectMapper.readValue(requestBody.toString(), PublishRequest.class);
            
            // Publish message
            Record record = messageService.publishMessage(topicName, publishRequest.data.getBytes());
            
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            PublishResponse publishResponse = new PublishResponse(record.getOffset(), record.getTimestamp());
            String json = objectMapper.writeValueAsString(publishResponse);
            
            try (PrintWriter out = response.getWriter()) {
                out.print(json);
            }
            
            logger.debug("Message published successfully to topic: {}, offset: {}", topicName, record.getOffset());
            
        } catch (Exception e) {
            logger.error("Failed to publish message to topic: {}", topicName, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
    
    @Override
    @Operation(
        summary = "Consume Message",
        description = "Consume a message from a topic at a specific offset"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Message retrieved successfully",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = ConsumeResponse.class)
            )
        ),
        @ApiResponse(responseCode = "404", description = "Message not found"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        String pathInfo = request.getPathInfo();
        Matcher matcher = topicPattern.matcher(pathInfo);
        
        if (!matcher.find()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        
        String topicName = matcher.group(1);
        String offsetParam = request.getParameter("offset");
        
        if (offsetParam == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        
        try {
            long offset = Long.parseLong(offsetParam);
            logger.debug("Consuming message from topic: {}, offset: {}", topicName, offset);
            
            Record record = messageService.consumeMessage(topicName, offset);
            
            if (record == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            ConsumeResponse consumeResponse = new ConsumeResponse(
                record.getOffset(), 
                record.getTimestamp(), 
                new String(record.getData())
            );
            String json = objectMapper.writeValueAsString(consumeResponse);
            
            try (PrintWriter out = response.getWriter()) {
                out.print(json);
            }
            
            logger.debug("Message consumed successfully from topic: {}, offset: {}", topicName, offset);
            
        } catch (NumberFormatException e) {
            logger.error("Invalid offset parameter: {}", offsetParam);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } catch (Exception e) {
            logger.error("Failed to consume message from topic: {}, offset: {}", topicName, offsetParam, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
    
    /**
     * Publish request model.
     */
    @Schema(description = "Message publish request")
    public static class PublishRequest {
        @Schema(description = "Message data", required = true)
        public String data;
        
        @Schema(description = "Optional message key")
        public String key;
    }
    
    /**
     * Publish response model.
     */
    @Schema(description = "Message publish response")
    public static class PublishResponse {
        @Schema(description = "Message offset", example = "12345")
        public long offset;
        
        @Schema(description = "Message timestamp", example = "1640995200000")
        public long timestamp;
        
        public PublishResponse(long offset, long timestamp) {
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }
    
    /**
     * Consume response model.
     */
    @Schema(description = "Message consume response")
    public static class ConsumeResponse {
        @Schema(description = "Message offset", example = "12345")
        public long offset;
        
        @Schema(description = "Message timestamp", example = "1640995200000")
        public long timestamp;
        
        @Schema(description = "Message data")
        public String data;
        
        public ConsumeResponse(long offset, long timestamp, String data) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.data = data;
        }
    }
}
