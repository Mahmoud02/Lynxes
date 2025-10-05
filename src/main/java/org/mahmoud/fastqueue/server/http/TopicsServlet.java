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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Topics management servlet with Swagger annotations.
 */
@Tag(name = "Topics", description = "Topic management operations")
public class TopicsServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(TopicsServlet.class);
    
    private final ObjectMapper objectMapper;
    
    public TopicsServlet(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
    
    @Override
    @Operation(
        summary = "List Topics",
        description = "Get a list of all topics"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "List of topics",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = TopicsResponse.class)
            )
        )
    })
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        
        TopicsResponse topicsResponse = new TopicsResponse(new String[0]); // TODO: Implement topic listing
        String json = objectMapper.writeValueAsString(topicsResponse);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }
    
    @Override
    @Operation(
        summary = "Create Topic",
        description = "Create a new topic (topics are created automatically on first message)"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Topic created or already exists",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = MessageResponse.class)
            )
        )
    })
    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        
        MessageResponse messageResponse = new MessageResponse("Topic will be created on first use");
        String json = objectMapper.writeValueAsString(messageResponse);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }
    
    /**
     * Topics response model.
     */
    @Schema(description = "Topics list response")
    public static class TopicsResponse {
        @Schema(description = "Array of topic names")
        public String[] topics;
        
        public TopicsResponse(String[] topics) {
            this.topics = topics;
        }
    }
    
    /**
     * Message response model.
     */
    @Schema(description = "Generic message response")
    public static class MessageResponse {
        @Schema(description = "Response message")
        public String message;
        
        public MessageResponse(String message) {
            this.message = message;
        }
    }
}
