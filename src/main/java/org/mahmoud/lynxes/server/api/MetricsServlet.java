package org.mahmoud.lynxes.server.api;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.mahmoud.lynxes.server.pipeline.AsyncRequest;
import org.slf4j.LoggerFactory;

/**
 * Servlet for metrics endpoint.
 * Handles GET requests to /metrics and processes them asynchronously.
 */
public class MetricsServlet extends BaseAsyncServlet {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServlet.class);
    
    @Override
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, 
                               AsyncRequest.RequestType defaultType) throws ServletException {
        logger.debug("Processing metrics request");
        processAsyncRequest(request, response, AsyncRequest.RequestType.METRICS, null, null, null);
    }
}
