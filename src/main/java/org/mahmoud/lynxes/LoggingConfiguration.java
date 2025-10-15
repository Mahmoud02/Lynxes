package org.mahmoud.lynxes;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.LoggerFactory;

/**
 * Handles programmatic configuration of Logback logging.
 * This class is responsible for setting up console and file appenders,
 * configuring log levels, and managing rolling file policies.
 */
public class LoggingConfiguration {
    
    private static final String LYNXES_PACKAGE = "org.mahmoud.lynxes";
    private static final String JETTY_PACKAGE = "org.eclipse.jetty";
    private static final String ROOT_LOGGER = "ROOT";
    private static final String CONSOLE_APPENDER_NAME = "CONSOLE";
    private static final String FILE_APPENDER_NAME = "FILE";
    private static final String LOG_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n";
    private static final String DEFAULT_LOG_LEVEL = "INFO";
    private static final String DEFAULT_LOG_FILE = "logs/lynxes.log";
    private static final String MAX_FILE_SIZE = "100MB";
    private static final int MAX_HISTORY_DAYS = 30;
    private static final String TOTAL_SIZE_CAP = "1GB";
    
    /**
     * Configures Logback programmatically based on the application configuration.
     * 
     * @param environment The environment to load configuration for
     */
    public static void configureLogging(String environment) {
        try {
            Config config = ConfigFactory.load("application");
            String logLevel = getLogLevel(config);
            String logFile = getLogFile(config);
            
            configureLogback(logLevel, logFile);
        } catch (Exception e) {
            // Fallback to default configuration if loading fails
            configureLogback(DEFAULT_LOG_LEVEL, DEFAULT_LOG_FILE);
        }
    }
    
    /**
     * Configures Logback with the specified log level and file path.
     * 
     * @param logLevel The log level to use
     * @param logFile The log file path
     */
    private static void configureLogback(String logLevel, String logFile) {
        try {
            LoggerContext context = getLoggerContext();
            context.reset();
            
            ConsoleAppender<ILoggingEvent> consoleAppender = createConsoleAppender(context);
            RollingFileAppender<ILoggingEvent> fileAppender = createFileAppender(context, logFile);
            
            configureLoggers(context, logLevel, consoleAppender, fileAppender);
        } catch (Exception e) {
            System.err.println("Failed to configure Logback: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Gets the logger context from SLF4J.
     * 
     * @return The LoggerContext instance
     */
    private static LoggerContext getLoggerContext() {
        return (LoggerContext) LoggerFactory.getILoggerFactory();
    }
    
    /**
     * Creates and configures a console appender.
     * 
     * @param context The logger context
     * @return Configured console appender
     */
    private static ConsoleAppender<ILoggingEvent> createConsoleAppender(LoggerContext context) {
        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
        consoleAppender.setContext(context);
        consoleAppender.setName(CONSOLE_APPENDER_NAME);
        
        PatternLayoutEncoder encoder = createPatternEncoder(context);
        consoleAppender.setEncoder(encoder);
        consoleAppender.start();
        
        return consoleAppender;
    }
    
    /**
     * Creates and configures a rolling file appender.
     * 
     * @param context The logger context
     * @param logFile The log file path
     * @return Configured file appender
     */
    private static RollingFileAppender<ILoggingEvent> createFileAppender(LoggerContext context, String logFile) {
        RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
        fileAppender.setContext(context);
        fileAppender.setName(FILE_APPENDER_NAME);
        fileAppender.setFile(logFile);
        
        SizeAndTimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = createRollingPolicy(context, fileAppender, logFile);
        fileAppender.setRollingPolicy(rollingPolicy);
        
        PatternLayoutEncoder encoder = createPatternEncoder(context);
        fileAppender.setEncoder(encoder);
        fileAppender.start();
        
        return fileAppender;
    }
    
    /**
     * Creates a pattern layout encoder with the standard log pattern.
     * 
     * @param context The logger context
     * @return Configured pattern encoder
     */
    private static PatternLayoutEncoder createPatternEncoder(LoggerContext context) {
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern(LOG_PATTERN);
        encoder.start();
        return encoder;
    }
    
    /**
     * Creates a rolling policy for file appender.
     * 
     * @param context The logger context
     * @param fileAppender The file appender
     * @param logFile The log file path
     * @return Configured rolling policy
     */
    private static SizeAndTimeBasedRollingPolicy<ILoggingEvent> createRollingPolicy(
            LoggerContext context, RollingFileAppender<ILoggingEvent> fileAppender, String logFile) {
        
        SizeAndTimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new SizeAndTimeBasedRollingPolicy<>();
        rollingPolicy.setContext(context);
        rollingPolicy.setParent(fileAppender);
        rollingPolicy.setFileNamePattern(logFile + ".%d{yyyy-MM-dd}.%i.log");
        rollingPolicy.setMaxFileSize(FileSize.valueOf(MAX_FILE_SIZE));
        rollingPolicy.setMaxHistory(MAX_HISTORY_DAYS);
        rollingPolicy.setTotalSizeCap(FileSize.valueOf(TOTAL_SIZE_CAP));
        rollingPolicy.start();
        
        return rollingPolicy;
    }
    
    /**
     * Configures all loggers with the specified appenders and log level.
     * 
     * @param context The logger context
     * @param logLevel The log level for Lynxes logger
     * @param consoleAppender The console appender
     * @param fileAppender The file appender
     */
    private static void configureLoggers(LoggerContext context, String logLevel,
                                       ConsoleAppender<ILoggingEvent> consoleAppender, 
                                       RollingFileAppender<ILoggingEvent> fileAppender) {
        
        // Configure root logger
        Logger rootLogger = context.getLogger(ROOT_LOGGER);
        rootLogger.setLevel(Level.WARN);
        addAppenders(rootLogger, consoleAppender, fileAppender);
        
        // Configure Lynxes logger
        Logger lynxesLogger = context.getLogger(LYNXES_PACKAGE);
        lynxesLogger.setLevel(Level.toLevel(logLevel));
        lynxesLogger.setAdditive(false);
        addAppenders(lynxesLogger, consoleAppender, fileAppender);
        
        // Configure Jetty logger to reduce noise
        Logger jettyLogger = context.getLogger(JETTY_PACKAGE);
        jettyLogger.setLevel(Level.WARN);
        jettyLogger.setAdditive(false);
        addAppenders(jettyLogger, consoleAppender, fileAppender);
    }
    
    /**
     * Adds console and file appenders to a logger.
     * 
     * @param logger The logger to configure
     * @param consoleAppender The console appender
     * @param fileAppender The file appender
     */
    private static void addAppenders(Logger logger, 
                                   ConsoleAppender<ILoggingEvent> consoleAppender, 
                                   RollingFileAppender<ILoggingEvent> fileAppender) {
        logger.addAppender((Appender<ILoggingEvent>) consoleAppender);
        logger.addAppender((Appender<ILoggingEvent>) fileAppender);
    }
    
    /**
     * Gets the log level from configuration, with fallback to default.
     * 
     * @param config The application configuration
     * @return The log level string
     */
    private static String getLogLevel(Config config) {
        try {
            return config.getString("lynxes.logging.level");
        } catch (Exception e) {
            return DEFAULT_LOG_LEVEL;
        }
    }
    
    /**
     * Gets the log file path from configuration, with fallback to default.
     * 
     * @param config The application configuration
     * @return The log file path
     */
    private static String getLogFile(Config config) {
        try {
            return config.getString("lynxes.logging.file");
        } catch (Exception e) {
            return DEFAULT_LOG_FILE;
        }
    }
}
