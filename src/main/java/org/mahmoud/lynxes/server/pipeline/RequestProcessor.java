package org.mahmoud.lynxes.server.pipeline;

import java.io.IOException;

/**
 * Interface for processing specific types of async requests.
 * Each processor handles one or more related request types.
 */
public interface RequestProcessor {
    
    /**
     * Processes the given async request.
     * 
     * @param request The async request to process
     * @throws IOException if processing fails
     */
    void process(AsyncRequest request) throws IOException;
    
    /**
     * Checks if this processor can handle the given request type.
     * 
     * @param type The request type to check
     * @return true if this processor can handle the request type
     */
    boolean canProcess(AsyncRequest.RequestType type);
}
