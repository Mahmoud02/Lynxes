package org.mahmoud.lynxes.server.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.slf4j.LoggerFactory;

/**
 * Route handler for health check endpoint.
 * Handles GET requests to /health and processes them asynchronously.
 * Follows the async architecture pattern where servlets are thin wrappers that delegate to the processing pipeline.
 */
public class HealthRouteHandler extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(HealthRouteHandler.class);
    
    @Override
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                               AsyncRequest.RequestType defaultType) throws ServletException {
        logger.debug("Processing health check request");
        processAsyncRequest(request, response, AsyncRequest.RequestType.HEALTH, null, null, null);
    }
}
