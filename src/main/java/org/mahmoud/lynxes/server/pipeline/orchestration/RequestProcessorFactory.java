package org.mahmoud.lynxes.server.pipeline.orchestration;


import com.google.inject.Inject;
import org.mahmoud.lynxes.server.pipeline.core.AsyncRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Factory for mapping request types to appropriate processors.
 * Uses strategy pattern to route requests to the correct processor.
 */
public class RequestProcessorFactory {
    private static final Logger logger = LoggerFactory.getLogger(RequestProcessorFactory.class);
    
    private final List<RequestProcessor> processors;
    
    @Inject
    public RequestProcessorFactory(List<RequestProcessor> processors) {
        this.processors = processors;
        logger.debug("Initialized RequestProcessorFactory with {} processors", processors.size());
    }
    
    /**
     * Gets the appropriate processor for the given request type.
     * 
     * @param type The request type
     * @return The processor that can handle this request type
     * @throws IllegalArgumentException if no processor can handle the request type
     */
    public RequestProcessor getProcessor(AsyncRequest.RequestType type) {
        for (RequestProcessor processor : processors) {
            if (processor.canProcess(type)) {
                logger.debug("Found processor {} for request type {}", processor.getClass().getSimpleName(), type);
                return processor;
            }
        }
        
        logger.error("No processor found for request type: {}", type);
        throw new IllegalArgumentException("No processor found for request type: " + type);
    }
    
    /**
     * Gets all registered processors.
     * 
     * @return List of all processors
     */
    public List<RequestProcessor> getAllProcessors() {
        return processors;
    }
}
