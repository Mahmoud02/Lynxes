package org.mahmoud.fastqueue.server.http;

import org.mahmoud.fastqueue.service.MessageService;
import org.mahmoud.fastqueue.service.ObjectMapperService;
import org.mahmoud.fastqueue.service.HealthService;
import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.server.swagger.AutoSwaggerServlet;
import org.mahmoud.fastqueue.server.ui.WebUIServlet;
import com.google.inject.Inject;
import org.mahmoud.fastqueue.core.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * HTTP server using embedded Jetty for FastQueue2 message queue.
 * Provides REST API endpoints for message publishing and consumption.
 */
public class JettyHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(JettyHttpServer.class);
    
    private final QueueConfig config;
    private final MessageService messageService;
    private final ObjectMapper objectMapper;
    private final HealthService healthService;
    private final AutoSwaggerServlet swaggerServlet;
    private final WebUIServlet webUIServlet;
    private Server server;
    private volatile boolean running;

    /**
     * Creates a new Jetty HTTP server with injected dependencies.
     * 
     * @param config The queue configuration
     * @param messageService The message service for handling operations
     * @param objectMapperService The object mapper service
     * @param healthService The health service
     * @param swaggerServlet The automatic Swagger documentation servlet
     * @param webUIServlet The Web UI servlet
     */
    @Inject
    public JettyHttpServer(QueueConfig config, MessageService messageService, ObjectMapperService objectMapperService,
                         HealthService healthService, AutoSwaggerServlet swaggerServlet, WebUIServlet webUIServlet) {
        this.config = config;
        this.messageService = messageService;
        this.objectMapper = objectMapperService.getObjectMapper();
        this.healthService = healthService;
        this.swaggerServlet = swaggerServlet;
        this.webUIServlet = webUIServlet;
        this.running = false;
    }

    /**
     * Starts the HTTP server.
     * 
     * @throws Exception if the server cannot be started
     */
    public void start() throws Exception {
        if (running) {
            throw new IllegalStateException("Server is already running");
        }
        
        // Create Jetty server
        this.server = new Server(config.getServerPort());
        
        // Create servlet context
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        
        // Add servlets with Swagger annotations
        context.addServlet(new ServletHolder(new HealthServlet(healthService, objectMapper)), "/health");
        context.addServlet(new ServletHolder(new TopicsServlet(objectMapper)), "/topics");
        context.addServlet(new ServletHolder(new MessageServlet(messageService, objectMapper)), "/topics/*");
        context.addServlet(new ServletHolder(new MetricsServlet(messageService, objectMapper)), "/metrics");
        
        // Add new servlets for Web UI and API documentation
        context.addServlet(new ServletHolder(swaggerServlet), "/swagger/*");
        context.addServlet(new ServletHolder(swaggerServlet), "/swagger-ui/*");
        context.addServlet(new ServletHolder(webUIServlet), "/ui/*");
        
        // Set the servlet context
        server.setHandler(context);
        
        // Start server
        server.start();
        running = true;
        logger.info("FastQueue2 Jetty HTTP Server started on port {}", config.getServerPort());
    }

    /**
     * Stops the HTTP server.
     * 
     * @throws Exception if the server cannot be stopped
     */
    public void stop() throws Exception {
        if (!running) {
            return;
        }
        
        server.stop();
        running = false;
        logger.info("FastQueue2 Jetty HTTP Server stopped");
    }

    /**
     * Checks if the server is running.
     * 
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Gets the server port.
     * 
     * @return The server port
     */
    public int getPort() {
        return config.getServerPort();
    }



}
