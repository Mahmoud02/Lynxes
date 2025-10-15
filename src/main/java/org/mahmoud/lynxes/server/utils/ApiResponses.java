package org.mahmoud.lynxes.server.utils;

/**
 * Response classes for HTTP API endpoints.
 * These classes represent the JSON response structures returned by the Lynxes HTTP API.
 */
public class ApiResponses {
    
    /**
     * Response for the topics list endpoint.
     * Contains an array of topic names.
     */
    public static class TopicsResponse {
        public String[] topics;
        
        public TopicsResponse(String[] topics) {
            this.topics = topics;
        }
        
        public String[] getTopics() {
            return topics;
        }
        
        public void setTopics(String[] topics) {
            this.topics = topics;
        }
    }
    
    /**
     * Generic message response for various endpoints.
     * Contains a single message string.
     */
    public static class MessageResponse {
        public String message;
        
        public MessageResponse(String message) {
            this.message = message;
        }
        
        public String getMessage() {
            return message;
        }
        
        public void setMessage(String message) {
            this.message = message;
        }
    }
    
    /**
     * Response for successful operations.
     * Contains a success message and optional data.
     */
    public static class SuccessResponse {
        public String message;
        public Object data;
        
        public SuccessResponse(String message) {
            this.message = message;
        }
        
        public SuccessResponse(String message, Object data) {
            this.message = message;
            this.data = data;
        }
        
        public String getMessage() {
            return message;
        }
        
        public void setMessage(String message) {
            this.message = message;
        }
        
        public Object getData() {
            return data;
        }
        
        public void setData(Object data) {
            this.data = data;
        }
    }
    
    /**
     * Response for error conditions.
     * Contains error message and error code.
     */
    public static class ErrorResponse {
        public String error;
        public int code;
        
        public ErrorResponse(String error, int code) {
            this.error = error;
            this.code = code;
        }
        
        public String getError() {
            return error;
        }
        
        public void setError(String error) {
            this.error = error;
        }
        
        public int getCode() {
            return code;
        }
        
        public void setCode(int code) {
            this.code = code;
        }
    }
}
