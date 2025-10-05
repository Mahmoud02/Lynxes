package org.mahmoud.fastqueue.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

/**
 * Service for providing ObjectMapper instances.
 * This ensures consistent JSON serialization/deserialization across the application.
 */
public class ObjectMapperService {
    private final ObjectMapper objectMapper;
    
    @Inject
    public ObjectMapperService() {
        this.objectMapper = new ObjectMapper();
    }
    
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
