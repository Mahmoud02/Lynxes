package org.mahmoud.lynxes;

/**
 * Lynxes - High-Performance Message Queue Server
 * 
 * This is the main entry point for the Lynxes message queue server.
 * It delegates all initialization and startup logic to ApplicationBootstrap.
 *
 * @author mahmoudreda
 */
public class Lynxes {

    /**
     * Main entry point for the Lynxes message queue server.
     * 
     * @param args Command line arguments. Supports --env <environment> for environment selection.
     */
    public static void main(String[] args) {
        ApplicationBootstrap.bootstrap(args);
    }
}