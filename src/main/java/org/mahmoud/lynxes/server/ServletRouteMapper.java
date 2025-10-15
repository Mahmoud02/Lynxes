package org.mahmoud.lynxes.server;

import org.mahmoud.lynxes.server.ui.DashboardServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mahmoud.lynxes.server.api.HealthRouteHandler;
import org.mahmoud.lynxes.server.api.TopicsRouteHandler;
import org.mahmoud.lynxes.server.api.TopicRouteHandler;
import org.mahmoud.lynxes.server.api.MetricsRouteHandler;
import org.mahmoud.lynxes.server.api.ConsumerGroupRouteHandler;
import org.mahmoud.lynxes.server.api.ConsumerRouteHandler;
import com.google.inject.Inject;

/**
 * Maps HTTP routes to servlet handlers for the AsyncHttpServer.
 * This class is responsible for configuring all HTTP endpoints and mapping them to their corresponding servlets.
 */
public class ServletRouteMapper {
    private static final Logger logger = LoggerFactory.getLogger(ServletRouteMapper.class);
    
    private final HealthRouteHandler healthRouteHandler;
    private final TopicsRouteHandler topicsRouteHandler;
    private final TopicRouteHandler topicRouteHandler;
    private final MetricsRouteHandler metricsRouteHandler;
    private final ConsumerGroupRouteHandler consumerGroupRouteHandler;
    private final ConsumerRouteHandler consumerRouteHandler;
    
    // Route path constants
    private static final String HEALTH_PATH = "/health";
    private static final String TOPICS_PATH = "/topics";
    private static final String TOPIC_PATH = "/topics/*";
    private static final String METRICS_PATH = "/metrics";
    private static final String CONSUMER_GROUPS_PATH = "/consumer-groups/*";
    private static final String CONSUMERS_PATH = "/consumers/*";
    private static final String UI_PATH = "/ui/*";
    
    /**
     * Constructs a ServletRouteMapper with injected servlets.
     * 
     * @param healthRouteHandler The health check route handler
     * @param topicsRouteHandler The topics list route handler
     * @param topicRouteHandler The individual topic route handler
     * @param metricsRouteHandler The metrics route handler
     * @param consumerGroupRouteHandler The consumer group route handler
     * @param consumerRouteHandler The consumer route handler
     */
    @Inject
    public ServletRouteMapper(HealthRouteHandler healthRouteHandler,
                          TopicsRouteHandler topicsRouteHandler,
                          TopicRouteHandler topicRouteHandler,
                          MetricsRouteHandler metricsRouteHandler,
                          ConsumerGroupRouteHandler consumerGroupRouteHandler,
                          ConsumerRouteHandler consumerRouteHandler) {
        this.healthRouteHandler = healthRouteHandler;
        this.topicsRouteHandler = topicsRouteHandler;
        this.topicRouteHandler = topicRouteHandler;
        this.metricsRouteHandler = metricsRouteHandler;
        this.consumerGroupRouteHandler = consumerGroupRouteHandler;
        this.consumerRouteHandler = consumerRouteHandler;
    }
    
    /**
     * Maps all routes to their corresponding servlets in the given servlet context handler.
     * 
     * @param context The servlet context handler to register servlets with
     */
    public void mapRoutes(ServletContextHandler context) {
        logger.debug("Mapping routes to servlets");
        
        // Map core API routes
        mapHealthRoute(context);
        mapTopicsRoute(context);
        mapTopicRoute(context);
        mapMetricsRoute(context);
        
        // Map consumer-related routes
        mapConsumerGroupRoute(context);
        mapConsumerRoute(context);
        
        // Map UI route
        mapDashboardRoute(context);
        
        logger.info("Successfully mapped {} routes to servlets", getRouteCount());
    }
    
    /**
     * Maps the health check route to its servlet.
     */
    private void mapHealthRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(healthRouteHandler), HEALTH_PATH);
        logger.debug("Mapped health route {} to HealthRouteHandler", HEALTH_PATH);
    }
    
    /**
     * Maps the topics list route to its servlet.
     */
    private void mapTopicsRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(topicsRouteHandler), TOPICS_PATH);
        logger.debug("Mapped topics route {} to TopicsRouteHandler", TOPICS_PATH);
    }
    
    /**
     * Maps the individual topic route to its servlet for GET/POST/DELETE operations.
     */
    private void mapTopicRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(topicRouteHandler), TOPIC_PATH);
        logger.debug("Mapped topic route {} to TopicRouteHandler", TOPIC_PATH);
    }
    
    /**
     * Maps the metrics route to its servlet.
     */
    private void mapMetricsRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(metricsRouteHandler), METRICS_PATH);
        logger.debug("Mapped metrics route {} to MetricsRouteHandler", METRICS_PATH);
    }
    
    /**
     * Maps the consumer group route to its servlet.
     */
    private void mapConsumerGroupRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(consumerGroupRouteHandler), CONSUMER_GROUPS_PATH);
        logger.debug("Mapped consumer group route {} to ConsumerGroupRouteHandler", CONSUMER_GROUPS_PATH);
    }
    
    /**
     * Maps the simple consumer route to its servlet.
     */
    private void mapConsumerRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(consumerRouteHandler), CONSUMERS_PATH);
        logger.debug("Mapped consumer route {} to ConsumerRouteHandler", CONSUMERS_PATH);
    }
    
    /**
     * Maps the dashboard UI route to its servlet.
     */
    private void mapDashboardRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(new DashboardServlet()), UI_PATH);
        logger.debug("Mapped dashboard route {} to DashboardServlet", UI_PATH);
    }
    
    /**
     * Gets the total number of routes mapped.
     * 
     * @return The number of routes
     */
    private int getRouteCount() {
        return 7; // Health, Topics, Topic, Metrics, ConsumerGroups, Consumers, UI
    }
    
    /**
     * Gets a list of all mapped routes for logging/debugging purposes.
     * 
     * @return Array of route paths
     */
    public String[] getMappedRoutes() {
        return new String[]{
            HEALTH_PATH,
            TOPICS_PATH,
            TOPIC_PATH,
            METRICS_PATH,
            CONSUMER_GROUPS_PATH,
            CONSUMERS_PATH,
            UI_PATH
        };
    }
}
