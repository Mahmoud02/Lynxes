package org.mahmoud.fastqueue.server.async;

import org.mahmoud.fastqueue.core.Record;
import org.mahmoud.fastqueue.service.SimpleConsumerService;
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
 * Servlet for handling simple consumer operations without consumer groups.
 * Provides REST endpoints for consumer management and message consumption.
 */
public class SimpleConsumerServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerServlet.class);

    private final SimpleConsumerService consumerService;
    private final ObjectMapper objectMapper;

    public SimpleConsumerServlet(SimpleConsumerService consumerService, ObjectMapper objectMapper) {
        this.consumerService = consumerService;
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

        if (pathParts.length >= 1) {
            String consumerId = pathParts[0];
            // POST /consumers/{consumerId}
            handleRegisterConsumer(request, response, consumerId);
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
            handleListConsumers(request, response);
            return;
        }

        String[] pathParts = pathInfo.substring(1).split("/");

        if (pathParts.length >= 1) {
            String consumerId = pathParts[0];

            if (pathParts.length >= 2 && "messages".equals(pathParts[1])) {
                // GET /consumers/{consumerId}/messages
                handleConsumeMessages(request, response, consumerId);
            } else {
                // GET /consumers/{consumerId}
                handleGetConsumer(request, response, consumerId);
            }
        } else {
            // GET /consumers
            handleListConsumers(request, response);
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

        if (pathParts.length >= 1) {
            // DELETE /consumers/{consumerId}
            String consumerId = pathParts[0];
            handleUnregisterConsumer(request, response, consumerId);
        } else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\": \"Endpoint not found\"}");
        }
    }

    private void handleRegisterConsumer(HttpServletRequest request, HttpServletResponse response, String consumerId) throws IOException {
        try {
            boolean registered = consumerService.registerConsumer(consumerId);
            if (registered) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("application/json");
                response.getWriter().write(String.format(
                        "{\"consumerId\":\"%s\",\"message\":\"Consumer registered successfully\"}",
                        consumerId));
            } else {
                response.setStatus(HttpServletResponse.SC_CONFLICT);
                response.getWriter().write("{\"error\": \"Consumer already exists\"}");
            }
        } catch (Exception e) {
            logger.error("Error registering consumer {}: {}", consumerId, e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Failed to register consumer: " + e.getMessage() + "\"}");
        }
    }

    private void handleUnregisterConsumer(HttpServletRequest request, HttpServletResponse response, String consumerId) throws IOException {
        try {
            boolean unregistered = consumerService.unregisterConsumer(consumerId);
            if (unregistered) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("application/json");
                response.getWriter().write(String.format(
                        "{\"consumerId\":\"%s\",\"message\":\"Consumer unregistered successfully\"}",
                        consumerId));
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().write("{\"error\": \"Consumer not found\"}");
            }
        } catch (Exception e) {
            logger.error("Error unregistering consumer {}: {}", consumerId, e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Failed to unregister consumer: " + e.getMessage() + "\"}");
        }
    }

    private void handleGetConsumer(HttpServletRequest request, HttpServletResponse response, String consumerId) throws IOException {
        SimpleConsumerService.ConsumerInfo consumerInfo = consumerService.getConsumerInfo(consumerId);
        if (consumerInfo == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("{\"error\": \"Consumer not found\"}");
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json");
        response.getWriter().write(objectMapper.writeValueAsString(consumerInfo));
    }

    private void handleListConsumers(HttpServletRequest request, HttpServletResponse response) throws IOException {
        List<String> consumers = consumerService.listConsumers();
        List<SimpleConsumerService.ConsumerInfo> consumerInfos = new ArrayList<>();
        for (String consumerId : consumers) {
            SimpleConsumerService.ConsumerInfo info = consumerService.getConsumerInfo(consumerId);
            if (info != null) {
                consumerInfos.add(info);
            }
        }

        Map<String, Object> responseBody = new HashMap<>();
        responseBody.put("consumers", consumerInfos);
        responseBody.put("totalConsumers", consumerService.getTotalConsumerCount());

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json");
        response.getWriter().write(objectMapper.writeValueAsString(responseBody));
    }

    private void handleConsumeMessages(HttpServletRequest request, HttpServletResponse response, String consumerId) throws IOException {
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
                } catch (NumberFormatException e) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().write("{\"error\": \"Invalid maxMessages format\"}");
                    return;
                }
            }

            // Consume messages
            List<Record> messages = consumerService.consumeMessages(consumerId, topicName, offset, maxMessages);

            // Prepare response
            List<Map<String, Object>> messageList = new ArrayList<>();
            for (Record record : messages) {
                Map<String, Object> message = new HashMap<>();
                message.put("offset", record.getOffset());
                message.put("timestamp", record.getTimestamp());
                message.put("checksum", record.getChecksum());
                message.put("message", new String(record.getData()));
                messageList.add(message);
            }

            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("consumerId", consumerId);
            responseBody.put("topicName", topicName);
            responseBody.put("messages", messageList);
            responseBody.put("count", messages.size());
            responseBody.put("nextOffset", consumerService.getConsumerOffset(consumerId, topicName));

            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);

            PrintWriter writer = response.getWriter();
            writer.write(objectMapper.writeValueAsString(responseBody));
            writer.flush();

        } catch (Exception e) {
            logger.error("Error consuming messages for consumer {}: {}", consumerId, e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Failed to consume messages: " + e.getMessage() + "\"}");
        }
    }
}
