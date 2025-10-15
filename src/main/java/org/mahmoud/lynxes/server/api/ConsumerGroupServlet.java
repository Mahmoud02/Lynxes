package org.mahmoud.lynxes.server.api;

import org.mahmoud.lynxes.api.consumer.ConsumerGroup;
import org.mahmoud.lynxes.core.Record;
import org.mahmoud.lynxes.service.ConsumerGroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Servlet for handling consumer group operations.
 * Provides REST endpoints for consumer group management and message consumption.
 */
public class ConsumerGroupServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupServlet.class);
    
    private final ConsumerGroupService consumerGroupService;
    private final ObjectMapper objectMapper;
    
    public ConsumerGroupServlet(ConsumerGroupService consumerGroupService, ObjectMapper objectMapper) {
        this.consumerGroupService = consumerGroupService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid path\"}");
            return;
        }
        
        String[] pathParts = pathInfo.substring(1).split("/");
        
        logger.debug("POST pathInfo: {}, pathParts: {}", pathInfo, Arrays.toString(pathParts));
        
        if (pathParts.length >= 2 && "consumers".equals(pathParts[1])) {
            // POST /consumer-groups/{groupId}/consumers
            handleRegisterConsumer(request, response, pathParts[0]);
        } else {
            logger.debug("POST endpoint not found for path: {}", pathInfo);
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\": \"Endpoint not found\"}");
        }
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid path\"}");
            return;
        }
        
        // Handle root path
        if (pathInfo.equals("/")) {
            handleListConsumerGroups(request, response);
            return;
        }
        
        String[] pathParts = pathInfo.substring(1).split("/");
        
        if (pathParts.length >= 1) {
            String groupId = pathParts[0];
            
            if (pathParts.length >= 2 && "consumers".equals(pathParts[1])) {
                // GET /consumer-groups/{groupId}/consumers/{consumerId}/messages
                if (pathParts.length >= 4 && "messages".equals(pathParts[3])) {
                    String consumerId = pathParts[2];
                    handleConsumeMessages(request, response, groupId, consumerId);
                } else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.getWriter().write("{\"error\": \"Endpoint not found\"}");
                }
            } else {
                // GET /consumer-groups/{groupId}
                handleGetConsumerGroup(request, response, groupId);
            }
        } else {
            // GET /consumer-groups
            handleListConsumerGroups(request, response);
        }
    }
    
    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid path\"}");
            return;
        }
        
        String[] pathParts = pathInfo.substring(1).split("/");
        
        if (pathParts.length >= 2 && "consumers".equals(pathParts[1])) {
            // DELETE /consumer-groups/{groupId}/consumers/{consumerId}
            if (pathParts.length >= 3) {
                String groupId = pathParts[0];
                String consumerId = pathParts[2];
                handleUnregisterConsumer(request, response, groupId, consumerId);
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\": \"Consumer ID required\"}");
            }
        } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\": \"Endpoint not found\"}");
        }
    }
    
    private void handleRegisterConsumer(HttpServletRequest request, HttpServletResponse response, 
                                      String groupId) throws IOException {
        try {
            // Parse request body
            Map<String, Object> requestBody = objectMapper.readValue(request.getInputStream(), Map.class);
            String consumerId = (String) requestBody.get("consumerId");
            String topicName = (String) requestBody.get("topicName");
            
            if (consumerId == null || topicName == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"consumerId and topicName are required\"}");
                return;
            }
            
            // Register consumer
            boolean success = consumerGroupService.registerConsumer(topicName, groupId, consumerId);
            
            // Prepare response
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("consumerId", consumerId);
            responseBody.put("groupId", groupId);
            responseBody.put("topicName", topicName);
            responseBody.put("success", success);
            responseBody.put("groupSize", consumerGroupService.getConsumerGroupInfo(topicName, groupId).getGroupSize());
            responseBody.put("message", "Consumer registered successfully");
            
            // Get group info for additional details
            ConsumerGroup group = consumerGroupService.getConsumerGroupInfo(topicName, groupId);
            if (group != null) {
                responseBody.put("currentLeader", group.getCurrentLeaderId());
            }
            
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            PrintWriter writer = response.getWriter();
            writer.write(objectMapper.writeValueAsString(responseBody));
            writer.flush();
            
            logger.info("Consumer {} registered to group {} for topic {}", consumerId, groupId, topicName);
            
        } catch (Exception e) {
            logger.error("Error registering consumer", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Internal server error\"}");
        }
    }
    
    private void handleUnregisterConsumer(HttpServletRequest request, HttpServletResponse response, 
                                        String groupId, String consumerId) throws IOException {
        try {
            // Get topic name from query parameter
            String topicName = request.getParameter("topicName");
            if (topicName == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"topicName query parameter is required\"}");
                return;
            }
            
            // Unregister consumer
            boolean removed = consumerGroupService.unregisterConsumer(topicName, groupId, consumerId);
            
            if (removed) {
                Map<String, Object> responseBody = new HashMap<>();
                responseBody.put("consumerId", consumerId);
                responseBody.put("groupId", groupId);
                responseBody.put("topicName", topicName);
                responseBody.put("message", "Consumer unregistered successfully");
                
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_OK);
                
                PrintWriter writer = response.getWriter();
                writer.write(objectMapper.writeValueAsString(responseBody));
                writer.flush();
                
                logger.info("Consumer {} unregistered from group {} for topic {}", consumerId, groupId, topicName);
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\": \"Consumer not found\"}");
            }
            
        } catch (Exception e) {
            logger.error("Error unregistering consumer", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Internal server error\"}");
        }
    }
    
    private void handleConsumeMessages(HttpServletRequest request, HttpServletResponse response, 
                                     String groupId, String consumerId) throws IOException {
        try {
            // Get parameters
            String topicName = request.getParameter("topicName");
            String offsetParam = request.getParameter("offset");
            String maxMessagesParam = request.getParameter("maxMessages");
            
            if (topicName == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"topicName query parameter is required\"}");
                return;
            }
            
            long offset = 0L;
            if (offsetParam != null) {
                try {
                    offset = Long.parseLong(offsetParam);
                } catch (NumberFormatException e) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().write("{\"error\": \"Invalid offset format\"}");
                    return;
                }
            }
            
            int maxMessages = 10; // Default
            if (maxMessagesParam != null) {
                try {
                    maxMessages = Integer.parseInt(maxMessagesParam);
                    maxMessages = Math.min(maxMessages, 100); // Limit to 100
                } catch (NumberFormatException e) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().write("{\"error\": \"Invalid maxMessages format\"}");
                    return;
                }
            }
            
            // Consume messages
            List<Record> messages = consumerGroupService.consumeMessages(topicName, groupId, consumerId, offset, maxMessages);
            
            // Prepare response
            List<Map<String, Object>> messageList = new ArrayList<>();
            for (Record record : messages) {
                Map<String, Object> message = new HashMap<>();
                message.put("offset", record.getOffset());
                message.put("timestamp", record.getTimestamp());
                message.put("message", new String(record.getData()));
                message.put("checksum", record.getChecksum());
                messageList.add(message);
            }
            
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("groupId", groupId);
            responseBody.put("consumerId", consumerId);
            responseBody.put("topicName", topicName);
            responseBody.put("messages", messageList);
            responseBody.put("count", messages.size());
            responseBody.put("nextOffset", consumerGroupService.getNextOffset(topicName, groupId));
            
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            PrintWriter writer = response.getWriter();
            writer.write(objectMapper.writeValueAsString(responseBody));
            writer.flush();
            
            logger.debug("Returned {} messages for consumer {} in group {} from topic {}", 
                        messages.size(), consumerId, groupId, topicName);
            
        } catch (Exception e) {
            logger.error("Error consuming messages", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Internal server error\"}");
        }
    }
    
    private void handleGetConsumerGroup(HttpServletRequest request, HttpServletResponse response, 
                                     String groupId) throws IOException {
        try {
            String topicName = request.getParameter("topicName");
            if (topicName == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"topicName query parameter is required\"}");
                return;
            }
            
            ConsumerGroup group = consumerGroupService.getConsumerGroupInfo(topicName, groupId);
            if (group == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\": \"Consumer group not found\"}");
                return;
            }
            
            // Prepare response
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("groupId", group.getGroupId());
            responseBody.put("topicName", group.getTopicName());
            responseBody.put("groupSize", group.getGroupSize());
            responseBody.put("groupOffset", group.getGroupOffset());
            responseBody.put("lastHeartbeat", group.getLastHeartbeat());
            responseBody.put("consumers", group.getConsumerIds());
            
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            PrintWriter writer = response.getWriter();
            writer.write(objectMapper.writeValueAsString(responseBody));
            writer.flush();
            
        } catch (Exception e) {
            logger.error("Error getting consumer group info", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Internal server error\"}");
        }
    }
    
    private void handleListConsumerGroups(HttpServletRequest request, HttpServletResponse response) 
            throws IOException {
        try {
            String topicName = request.getParameter("topicName");
            
            Map<String, Object> responseBody = new HashMap<>();
            
            if (topicName != null) {
                // List groups for specific topic
                Map<String, ConsumerGroup> groups = consumerGroupService.getConsumerGroupsForTopic(topicName);
                List<Map<String, Object>> groupList = new ArrayList<>();
                
                for (ConsumerGroup group : groups.values()) {
                    Map<String, Object> groupInfo = new HashMap<>();
                    groupInfo.put("groupId", group.getGroupId());
                    groupInfo.put("topicName", group.getTopicName());
                    groupInfo.put("groupSize", group.getGroupSize());
                    groupInfo.put("groupOffset", group.getGroupOffset());
                    groupInfo.put("lastHeartbeat", group.getLastHeartbeat());
                    groupList.add(groupInfo);
                }
                
                responseBody.put("topicName", topicName);
                responseBody.put("groups", groupList);
                responseBody.put("count", groupList.size());
            } else {
                // List all topics with groups
                Set<String> topics = consumerGroupService.getTopicsWithGroups();
                Map<String, Object> statistics = consumerGroupService.getStatistics();
                
                responseBody.put("topics", topics);
                responseBody.put("statistics", statistics);
            }
            
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            
            PrintWriter writer = response.getWriter();
            writer.write(objectMapper.writeValueAsString(responseBody));
            writer.flush();
            
        } catch (Exception e) {
            logger.error("Error listing consumer groups", e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Internal server error\"}");
        }
    }
}
