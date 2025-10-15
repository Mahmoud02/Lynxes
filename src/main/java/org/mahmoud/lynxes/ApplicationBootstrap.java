package org.mahmoud.lynxes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the bootstrap process for the Lynxes application.
 * This class is responsible for parsing command line arguments,
 * initializing the application, and starting the server.
 * 
 * @author mahmoudreda
 */
public class ApplicationBootstrap {
    
    private static final String DEFAULT_ENVIRONMENT = "default";
    private static final String ENVIRONMENT_ARGUMENT = "--env";
    
    private static final Logger logger = LoggerFactory.getLogger(ApplicationBootstrap.class);
    
    /**
     * Bootstraps the Lynxes application with the given command line arguments.
     * 
     * @param args Command line arguments. Supports --env <environment> for environment selection.
     */
    public static void bootstrap(String[] args) {
        try {
            String environment = parseEnvironment(args);
            initializeApplication(environment);
            startServer(environment);
        } catch (Exception e) {
            handleStartupError(e);
        }
    }
    
    /**
     * Initializes the application by configuring logging and setting up the logger.
     * 
     * @param environment The environment to load configuration for
     */
    private static void initializeApplication(String environment) {
        LoggingConfiguration.configureLogging(environment);
        logStartupBanner();
    }
    
    /**
     * Starts the HTTP server using the ServerManager.
     * 
     * @param environment The environment configuration to use
     * @throws Exception if server startup fails
     */
    private static void startServer(String environment) throws Exception {
        ServerManager serverManager = new ServerManager(environment);
        serverManager.startServer();
    }
    
    /**
     * Parses command line arguments to determine the environment.
     * 
     * @param args Command line arguments
     * @return The environment name, or "default" if not specified
     */
    private static String parseEnvironment(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (ENVIRONMENT_ARGUMENT.equals(args[i]) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return DEFAULT_ENVIRONMENT;
    }
    
    /**
     * Logs the application startup banner.
     */
    private static void logStartupBanner() {
        logger.info("Lynxes - High-Performance Message Queue Server");
        logger.info("=============================================");
    }
    
    /**
     * Handles startup errors by logging them and exiting the application.
     * 
     * @param e The exception that occurred during startup
     */
    private static void handleStartupError(Exception e) {
        logger.error("Failed to start Lynxes server", e);
        System.exit(1);
    }
}
