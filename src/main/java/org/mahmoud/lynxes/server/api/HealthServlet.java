package org.mahmoud.lynxes.server.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.slf4j.LoggerFactory;

/**
 * Servlet for health check endpoint.
 * Handles GET requests to /health and processes them asynchronously.
 */
public class HealthServlet extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(HealthServlet.class);
    
    @Override
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                               AsyncRequest.RequestType defaultType) throws ServletException {
        logger.debug("Processing health check request");
        processAsyncRequest(request, response, AsyncRequest.RequestType.HEALTH, null, null, null);
    }
}
