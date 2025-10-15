package org.mahmoud.lynxes;

import org.mahmoud.lynxes.config.QueueConfig;
import org.mahmoud.lynxes.di.ServiceContainer;
import org.mahmoud.lynxes.server.async.AsyncHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;

/**
 * Lynxes - High-Performance Message Queue Server
 *
 * @author mahmoudreda
 */
public class Lynxes {
    private static Logger logger;

    public static void main(String[] args) {
        try {
            // Parse command line arguments
            String environment = parseEnvironment(args);
            
            // Load configuration and configure Logback BEFORE any logging
            loadConfigurationAndConfigureLogback(environment);
            
            // Initialize logger AFTER configuration is loaded
            logger = LoggerFactory.getLogger(Lynxes.class);
            
            logger.info("Lynxes - High-Performance Message Queue Server");
            logger.info("=============================================");
            
            // Start the HTTP server with configuration
            startHttpServer(environment);
            
        } catch (Exception e) {
            logger.error("Failed to start Lynxes server", e);
            System.exit(1);
        }
    }
    
    /**
     * Loads configuration and configures Logback programmatically BEFORE any logging occurs.
     */
    private static void loadConfigurationAndConfigureLogback(String environment) {
        try {
            // Load configuration using Typesafe Config directly
            com.typesafe.config.Config config = com.typesafe.config.ConfigFactory.load("application");
            
            // Get logging configuration
            String logLevel = config.getString("lynxes.logging.level");
            String logFile = config.getString("lynxes.logging.file");
            
            // Configure Logback programmatically
            configureLogbackProgrammatically(logLevel, logFile);
            
            // Debug output
            System.out.println("DEBUG: Configured Logback programmatically:");
            System.out.println("  Log Level = " + logLevel);
            System.out.println("  Log File = " + logFile);
            
        } catch (Exception e) {
            // If configuration loading fails, use defaults
            configureLogbackProgrammatically("INFO", "logs/fastqueue2.log");
        }
    }
    
    /**
     * Configures Logback programmatically with the specified log level and file.
     */
    private static void configureLogbackProgrammatically(String logLevel, String logFile) {
        try {
            // Get the LoggerContext
            ch.qos.logback.classic.LoggerContext context = 
                (ch.qos.logback.classic.LoggerContext) LoggerFactory.getILoggerFactory();
            
            // Reset the context to clear any existing configuration
            context.reset();
            
            // Create console appender
            ch.qos.logback.core.ConsoleAppender consoleAppender = new ch.qos.logback.core.ConsoleAppender();
            consoleAppender.setContext(context);
            consoleAppender.setName("CONSOLE");
            
            ch.qos.logback.classic.encoder.PatternLayoutEncoder consoleEncoder = new ch.qos.logback.classic.encoder.PatternLayoutEncoder();
            consoleEncoder.setContext(context);
            consoleEncoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
            consoleEncoder.start();
            
            consoleAppender.setEncoder(consoleEncoder);
            consoleAppender.start();
            
            // Create file appender
            ch.qos.logback.core.rolling.RollingFileAppender fileAppender = new ch.qos.logback.core.rolling.RollingFileAppender();
            fileAppender.setContext(context);
            fileAppender.setName("FILE");
            fileAppender.setFile(logFile);
            
            // Configure rolling policy
            ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy rollingPolicy = new ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy();
            rollingPolicy.setContext(context);
            rollingPolicy.setParent(fileAppender);
            rollingPolicy.setFileNamePattern(logFile + ".%d{yyyy-MM-dd}.%i.log");
            rollingPolicy.setMaxFileSize(ch.qos.logback.core.util.FileSize.valueOf("100MB"));
            rollingPolicy.setMaxHistory(30);
            rollingPolicy.setTotalSizeCap(ch.qos.logback.core.util.FileSize.valueOf("1GB"));
            rollingPolicy.start();
            
            fileAppender.setRollingPolicy(rollingPolicy);
            
            // Configure file encoder
            ch.qos.logback.classic.encoder.PatternLayoutEncoder fileEncoder = new ch.qos.logback.classic.encoder.PatternLayoutEncoder();
            fileEncoder.setContext(context);
            fileEncoder.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
            fileEncoder.start();
            
            fileAppender.setEncoder(fileEncoder);
            fileAppender.start();
            
            // Configure root logger
            ch.qos.logback.classic.Logger rootLogger = context.getLogger("ROOT");
            rootLogger.setLevel(ch.qos.logback.classic.Level.WARN);
            rootLogger.addAppender(consoleAppender);
            rootLogger.addAppender(fileAppender);
            
            // Configure Lynxes logger
            ch.qos.logback.classic.Logger lynxesLogger = context.getLogger("org.mahmoud.lynxes");
            lynxesLogger.setLevel(ch.qos.logback.classic.Level.toLevel(logLevel));
            lynxesLogger.setAdditive(false);
            lynxesLogger.addAppender(consoleAppender);
            lynxesLogger.addAppender(fileAppender);
            
            // Configure Jetty loggers to reduce noise
            ch.qos.logback.classic.Logger jettyLogger = context.getLogger("org.eclipse.jetty");
            jettyLogger.setLevel(ch.qos.logback.classic.Level.WARN);
            jettyLogger.setAdditive(false);
            jettyLogger.addAppender(consoleAppender);
            jettyLogger.addAppender(fileAppender);
            
        } catch (Exception e) {
            System.err.println("Failed to configure Logback programmatically: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Parses command line arguments to determine environment.
     */
    private static String parseEnvironment(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--env") && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return "default"; // Default environment
    }
    
    /**
     * Starts the HTTP server with dependency injection.
     */
    private static void startHttpServer(String environment) throws Exception {
        // Initialize dependency injection container
        ServiceContainer container = ServiceContainer.getInstance(environment);
        
        // Get AsyncHttpServer from DI container
        AsyncHttpServer httpServer = container.getService(AsyncHttpServer.class);
        httpServer.start();
        startServerLoop(httpServer, "AsyncHttpServer");
    }
    
    
    /**
     * Starts the server loop and handles shutdown.
     */
    private static void startServerLoop(AsyncHttpServer httpServer, String serverType) throws Exception {
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Lynxes Server...");
            try {
                httpServer.stop();
                logger.info("Lynxes Server shutdown completed");
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
        
        int port = httpServer.getPort();
        
        logger.info("Lynxes {} started on port {}", serverType, port);
        logger.info("API Endpoints:");
        logger.info("  GET  /health - Health check");
        logger.info("  GET  /topics - List topics");
        logger.info("  POST /topics - Create topic");
        logger.info("  POST /topics/{name} - Publish message");
        logger.info("  GET  /topics/{name}?offset={n} - Consume message");
        logger.info("  GET  /metrics - Server metrics");
        logger.info("Press Ctrl+C to stop the server");
        
        // Keep server running
        while (httpServer.isRunning()) {
            Thread.sleep(1000);
        }
    }
    
}
