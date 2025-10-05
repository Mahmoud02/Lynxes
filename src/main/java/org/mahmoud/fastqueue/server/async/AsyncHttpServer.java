package org.mahmoud.fastqueue.server.async;

import org.mahmoud.fastqueue.config.QueueConfig;
import org.mahmoud.fastqueue.service.MessageService;
import org.mahmoud.fastqueue.service.HealthService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async HTTP server that uses Jetty as network threads and async processing for I/O.
 * Implements Kafka-inspired architecture with request channel and async processing.
 */
public class AsyncHttpServer {
    private static final Logger logger = LoggerFactory.getLogger(AsyncHttpServer.class);
    
    private final QueueConfig config;
    private final RequestChannel requestChannel;
    private final ResponseChannel responseChannel;
    private final AsyncProcessor asyncProcessor;
    private final ResponseProcessor responseProcessor;
    private final MessageService messageService;
    private final HealthService healthService;
    private final Server server;
    private final AtomicLong requestIdCounter;
    private volatile boolean running;
    
    @Inject
    public AsyncHttpServer(QueueConfig config, RequestChannel requestChannel, ResponseChannel responseChannel,
                          MessageService messageService, HealthService healthService,
                          ExecutorService executorService) {
        this.config = config;
        this.requestChannel = requestChannel;
        this.responseChannel = responseChannel;
        this.messageService = messageService;
        this.healthService = healthService;
        
        // Create processors with injected dependencies
        this.asyncProcessor = new AsyncProcessor(requestChannel, responseChannel, messageService, healthService, executorService);
        this.responseProcessor = new ResponseProcessor(responseChannel, executorService);
        
        this.server = new Server(config.getServerPort());
        this.requestIdCounter = new AtomicLong(0);
        this.running = false;
        
        setupServlets();
        logger.info("AsyncHttpServer initialized on port {}", config.getServerPort());
    }
    
    /**
     * Sets up the servlet context and servlets.
     */
    private void setupServlets() {
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        
        // Register servlets
        context.addServlet(new ServletHolder(new HealthServlet()), "/health");
        context.addServlet(new ServletHolder(new TopicsServlet()), "/topics");
        context.addServlet(new ServletHolder(new TopicServlet()), "/topics/*");
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        
        server.setHandler(context);
    }
    
    /**
     * Starts the async HTTP server.
     */
    public void start() throws Exception {
        if (!running) {
            logger.info("Starting AsyncHttpServer...");
            
            // Start processors first
            asyncProcessor.start();
            responseProcessor.start();
            
            // Start Jetty server
            server.start();
            running = true;
            
            logger.info("AsyncHttpServer started successfully on port {}", config.getServerPort());
            logger.info("RequestChannel metrics: {}", requestChannel.toString());
            logger.info("ResponseChannel metrics: {}", responseChannel.toString());
        } else {
            logger.warn("AsyncHttpServer is already running");
        }
    }
    
    /**
     * Stops the async HTTP server.
     */
    public void stop() throws Exception {
        if (running) {
            logger.info("Stopping AsyncHttpServer...");
            
            // Stop Jetty server
            server.stop();
            
            // Stop processors
            asyncProcessor.shutdown();
            responseProcessor.stop();
            
            running = false;
            logger.info("AsyncHttpServer stopped successfully");
        } else {
            logger.warn("AsyncHttpServer is not running");
        }
    }
    
    /**
     * Checks if the server is running.
     */
    public boolean isRunning() {
        return running && server.isRunning();
    }
    
    /**
     * Gets the server port.
     */
    public int getPort() {
        return config.getServerPort();
    }
    
    /**
     * Generates a unique request ID.
     */
    private String generateRequestId() {
        return "req-" + requestIdCounter.incrementAndGet() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Base servlet for async operations.
     */
    private abstract class BaseAsyncServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            handleRequest(request, response, AsyncRequest.RequestType.HEALTH);
        }
        
        @Override
        protected void doPost(HttpServletRequest request, HttpServletResponse response) 
                throws ServletException, IOException {
            handleRequest(request, response, AsyncRequest.RequestType.PUBLISH);
        }
        
        protected abstract void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                                            AsyncRequest.RequestType defaultType) throws ServletException, IOException;
        
        protected void processAsyncRequest(HttpServletRequest request, HttpServletResponse response, 
                                         AsyncRequest.RequestType type, String topicName, Long offset, String message) {
            // Enable async processing
            AsyncContext asyncContext = request.startAsync();
            asyncContext.setTimeout(30000); // 30 second timeout
            
            // Create async request
            String requestId = generateRequestId();
            AsyncRequest asyncRequest = new AsyncRequest(requestId, request, response, asyncContext, 
                                                       type, topicName, offset, message);
            
            // Add to request channel
            if (requestChannel.addRequest(asyncRequest)) {
                logger.debug("Request queued: {}", requestId);
            } else {
                logger.warn("Request queue full, rejecting: {}", requestId);
                sendErrorResponse(response, 503, "Service temporarily unavailable");
                asyncContext.complete();
            }
        }
        
        protected void sendErrorResponse(HttpServletResponse response, int statusCode, String message) {
            try {
                response.setStatus(statusCode);
                response.setContentType("application/json");
                response.getWriter().print(String.format("{\"error\":\"%s\",\"code\":%d}", message, statusCode));
            } catch (IOException e) {
                logger.error("Error sending error response", e);
            }
        }
    }
    
    /**
     * Health check servlet.
     */
    private class HealthServlet extends BaseAsyncServlet {
        @Override
        protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                                   AsyncRequest.RequestType defaultType) throws ServletException, IOException {
            processAsyncRequest(request, response, AsyncRequest.RequestType.HEALTH, null, null, null);
        }
    }
    
    /**
     * Topics list servlet.
     */
    private class TopicsServlet extends BaseAsyncServlet {
        @Override
        protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                                   AsyncRequest.RequestType defaultType) throws ServletException, IOException {
            processAsyncRequest(request, response, AsyncRequest.RequestType.TOPICS, null, null, null);
        }
    }
    
    /**
     * Topic servlet for both GET and POST /topics/{name}.
     * GET: Consume messages (with offset parameter)
     * POST: Publish messages
     */
    private class TopicServlet extends BaseAsyncServlet {
        @Override
        protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                                   AsyncRequest.RequestType defaultType) throws ServletException, IOException {
            String pathInfo = request.getPathInfo();
            if (pathInfo == null || pathInfo.equals("/")) {
                sendErrorResponse(response, 400, "Topic name required");
                return;
            }
            
            String topicName = pathInfo.substring(1); // Remove leading slash
            String method = request.getMethod();
            
            if ("GET".equals(method)) {
                // Consume request
                String offsetParam = request.getParameter("offset");
                if (offsetParam == null) {
                    sendErrorResponse(response, 400, "Offset parameter required");
                    return;
                }
                
                try {
                    Long offset = Long.parseLong(offsetParam);
                    processAsyncRequest(request, response, AsyncRequest.RequestType.CONSUME, topicName, offset, null);
                } catch (NumberFormatException e) {
                    sendErrorResponse(response, 400, "Invalid offset parameter");
                }
            } else if ("POST".equals(method)) {
                // Publish request
                String message = new String(request.getInputStream().readAllBytes());
                processAsyncRequest(request, response, AsyncRequest.RequestType.PUBLISH, topicName, null, message);
            } else {
                sendErrorResponse(response, 405, "Method not allowed: " + method);
            }
        }
    }
    
    /**
     * Metrics servlet.
     */
    private class MetricsServlet extends BaseAsyncServlet {
        @Override
        protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                                   AsyncRequest.RequestType defaultType) throws ServletException, IOException {
            processAsyncRequest(request, response, AsyncRequest.RequestType.METRICS, null, null, null);
        }
    }
}
