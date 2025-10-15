package org.mahmoud.lynxes.server;

import org.mahmoud.lynxes.service.ConsumerGroupService;
import org.mahmoud.lynxes.service.SimpleConsumerService;
import org.mahmoud.lynxes.service.TopicService;
import org.mahmoud.lynxes.server.ui.DashboardServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mahmoud.lynxes.server.api.HealthServlet;
import org.mahmoud.lynxes.server.api.TopicsServlet;
import org.mahmoud.lynxes.server.api.TopicServlet;
import org.mahmoud.lynxes.server.api.MetricsServlet;
import org.mahmoud.lynxes.server.api.ConsumerGroupServlet;
import org.mahmoud.lynxes.server.api.SimpleConsumerServlet;
import com.google.inject.Inject;

/**
 * Maps HTTP routes to servlet handlers for the AsyncHttpServer.
 * This class is responsible for configuring all HTTP endpoints and mapping them to their corresponding servlets.
 */
public class ServletRouteMapper {
    private static final Logger logger = LoggerFactory.getLogger(ServletRouteMapper.class);
    
    private final HealthServlet healthServlet;
    private final TopicsServlet topicsServlet;
    private final TopicServlet topicServlet;
    private final MetricsServlet metricsServlet;
    private final ConsumerGroupServlet consumerGroupServlet;
    private final SimpleConsumerServlet simpleConsumerServlet;
    
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
     * @param healthServlet The health check servlet
     * @param topicsServlet The topics list servlet
     * @param topicServlet The individual topic servlet
     * @param metricsServlet The metrics servlet
     * @param consumerGroupServlet The consumer group servlet
     * @param simpleConsumerServlet The simple consumer servlet
     */
    @Inject
    public ServletRouteMapper(HealthServlet healthServlet,
                          TopicsServlet topicsServlet,
                          TopicServlet topicServlet,
                          MetricsServlet metricsServlet,
                          ConsumerGroupServlet consumerGroupServlet,
                          SimpleConsumerServlet simpleConsumerServlet) {
        this.healthServlet = healthServlet;
        this.topicsServlet = topicsServlet;
        this.topicServlet = topicServlet;
        this.metricsServlet = metricsServlet;
        this.consumerGroupServlet = consumerGroupServlet;
        this.simpleConsumerServlet = simpleConsumerServlet;
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
        mapSimpleConsumerRoute(context);
        
        // Map UI route
        mapDashboardRoute(context);
        
        logger.info("Successfully mapped {} routes to servlets", getRouteCount());
    }
    
    /**
     * Maps the health check route to its servlet.
     */
    private void mapHealthRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(healthServlet), HEALTH_PATH);
        logger.debug("Mapped health route {} to HealthServlet", HEALTH_PATH);
    }
    
    /**
     * Maps the topics list route to its servlet.
     */
    private void mapTopicsRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(topicsServlet), TOPICS_PATH);
        logger.debug("Mapped topics route {} to TopicsServlet", TOPICS_PATH);
    }
    
    /**
     * Maps the individual topic route to its servlet for GET/POST/DELETE operations.
     */
    private void mapTopicRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(topicServlet), TOPIC_PATH);
        logger.debug("Mapped topic route {} to TopicServlet", TOPIC_PATH);
    }
    
    /**
     * Maps the metrics route to its servlet.
     */
    private void mapMetricsRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(metricsServlet), METRICS_PATH);
        logger.debug("Mapped metrics route {} to MetricsServlet", METRICS_PATH);
    }
    
    /**
     * Maps the consumer group route to its servlet.
     */
    private void mapConsumerGroupRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(consumerGroupServlet), CONSUMER_GROUPS_PATH);
        logger.debug("Mapped consumer group route {} to ConsumerGroupServlet", CONSUMER_GROUPS_PATH);
    }
    
    /**
     * Maps the simple consumer route to its servlet.
     */
    private void mapSimpleConsumerRoute(ServletContextHandler context) {
        context.addServlet(new ServletHolder(simpleConsumerServlet), CONSUMERS_PATH);
        logger.debug("Mapped simple consumer route {} to SimpleConsumerServlet", CONSUMERS_PATH);
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
