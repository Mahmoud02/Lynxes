package org.mahmoud.fastqueue.service;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.mahmoud.fastqueue.config.QueueConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provider for creating ExecutorService instances.
 * This ensures proper thread pool management through DI.
 */
public class ExecutorServiceProvider implements Provider<ExecutorService> {
    private final QueueConfig config;
    
    @Inject
    public ExecutorServiceProvider(QueueConfig config) {
        this.config = config;
    }
    
    @Override
    public ExecutorService get() {
        return Executors.newFixedThreadPool(config.getThreadPoolSize(), r -> {
            Thread t = new Thread(r, "FastQueue2-IO-Thread");
            t.setDaemon(true);
            return t;
        });
    }
}
