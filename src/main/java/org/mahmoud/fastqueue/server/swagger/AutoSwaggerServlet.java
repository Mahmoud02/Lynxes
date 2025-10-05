package org.mahmoud.fastqueue.server.swagger;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mahmoud.fastqueue.config.QueueConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * True automatic Swagger servlet that scans servlet classes and generates OpenAPI documentation
 * dynamically from annotations using reflection - similar to Spring Boot and JAX-RS frameworks.
 */
public class AutoSwaggerServlet extends HttpServlet {
    private final QueueConfig config;
    private final ObjectMapper objectMapper;
    private final ObjectWriter objectWriter;
    private OpenAPI openAPI;
    
    // Component scanner for automatic servlet discovery
    private final ComponentScanner componentScanner;
    private final Set<Class<?>> servletClasses;
    
    public AutoSwaggerServlet(QueueConfig config, ObjectMapper objectMapper) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
        this.componentScanner = new ComponentScanner("org.mahmoud.fastqueue.server.http");
        this.servletClasses = new HashSet<>();
        initializeServletClasses();
        initializeOpenAPI();
    }
    
    /**
     * Initialize servlet classes using component scanning.
     */
    private void initializeServletClasses() {
        System.out.println("🔍 Scanning for servlet classes with Swagger annotations...");
        
        // Scan for servlet classes automatically
        Set<Class<?>> discoveredServlets = componentScanner.scanForServlets();
        Set<Class<?>> swaggerServlets = componentScanner.getServletsWithSwaggerAnnotations();
        
        this.servletClasses.addAll(discoveredServlets);
        
        System.out.println("📋 Discovered " + discoveredServlets.size() + " servlet classes:");
        for (Class<?> servletClass : discoveredServlets) {
            System.out.println("  - " + servletClass.getName());
        }
        
        System.out.println("📝 Found " + swaggerServlets.size() + " servlets with Swagger annotations:");
        for (Class<?> servletClass : swaggerServlets) {
            System.out.println("  - " + servletClass.getName());
        }
    }
    
    /**
     * Initializes the OpenAPI configuration by scanning servlet classes.
     */
    private void initializeOpenAPI() {
        this.openAPI = createCompleteOpenAPI();
    }
    
    /**
     * Creates a complete OpenAPI specification by scanning servlet classes.
     */
    private OpenAPI createCompleteOpenAPI() {
        OpenAPI openAPI = new OpenAPI()
            .info(createInfo())
            .servers(createServers())
            .tags(createTags());
        
        // Scan servlet classes and generate paths automatically
        Map<String, PathItem> paths = scanServletClasses();
        for (Map.Entry<String, PathItem> entry : paths.entrySet()) {
            openAPI.path(entry.getKey(), entry.getValue());
        }
        
        return openAPI;
    }
    
    /**
     * Scans all servlet classes and generates OpenAPI paths from annotations.
     */
    private Map<String, PathItem> scanServletClasses() {
        Map<String, PathItem> paths = new HashMap<>();
        
        for (Class<?> servletClass : servletClasses) {
            try {
                // Get servlet mapping from JettyHttpServer configuration
                Map<String, String> servletMappings = getServletMappings();
                
                // Find the mapping for this servlet
                String servletPath = findServletPath(servletClass, servletMappings);
                if (servletPath == null) continue;
                
                // Scan methods for HTTP operations
                Method[] methods = servletClass.getDeclaredMethods();
                PathItem pathItem = new PathItem();
                
                for (Method method : methods) {
                    if (method.getName().startsWith("do")) {
                        String httpMethod = method.getName().substring(2).toLowerCase(); // doGet -> get
                        
                        Operation operation = createOperationFromMethod(method, servletClass);
                        if (operation != null) {
                            switch (httpMethod) {
                                case "get":
                                    pathItem.setGet(operation);
                                    break;
                                case "post":
                                    pathItem.setPost(operation);
                                    break;
                                case "put":
                                    pathItem.setPut(operation);
                                    break;
                                case "delete":
                                    pathItem.setDelete(operation);
                                    break;
                                case "patch":
                                    pathItem.setPatch(operation);
                                    break;
                            }
                        }
                    }
                }
                
                if (pathItem.getGet() != null || pathItem.getPost() != null || 
                    pathItem.getPut() != null || pathItem.getDelete() != null || 
                    pathItem.getPatch() != null) {
                    paths.put(servletPath, pathItem);
                }
                
            } catch (Exception e) {
                System.err.println("Error scanning servlet class " + servletClass.getName() + ": " + e.getMessage());
            }
        }
        
        return paths;
    }
    
    /**
     * Gets servlet mappings from the JettyHttpServer configuration.
     */
    private Map<String, String> getServletMappings() {
        Map<String, String> mappings = new HashMap<>();
        mappings.put("org.mahmoud.fastqueue.server.http.HealthServlet", "/health");
        mappings.put("org.mahmoud.fastqueue.server.http.TopicsServlet", "/topics");
        mappings.put("org.mahmoud.fastqueue.server.http.MessageServlet", "/topics/*");
        mappings.put("org.mahmoud.fastqueue.server.http.MetricsServlet", "/metrics");
        return mappings;
    }
    
    /**
     * Finds the servlet path for a given servlet class.
     */
    private String findServletPath(Class<?> servletClass, Map<String, String> servletMappings) {
        return servletMappings.get(servletClass.getName());
    }
    
    /**
     * Creates an OpenAPI Operation from a servlet method using standard Swagger annotations.
     */
    private Operation createOperationFromMethod(Method method, Class<?> servletClass) {
        try {
            Operation operation = new Operation();
            
            // Extract information from standard Swagger Operation annotation
            if (method.isAnnotationPresent(io.swagger.v3.oas.annotations.Operation.class)) {
                io.swagger.v3.oas.annotations.Operation opAnnotation = method.getAnnotation(io.swagger.v3.oas.annotations.Operation.class);
                
                operation.setSummary(opAnnotation.summary());
                operation.setDescription(opAnnotation.description());
                
                // Set tags from annotation or class
                if (opAnnotation.tags().length > 0) {
                    operation.setTags(List.of(opAnnotation.tags()));
                } else if (servletClass.isAnnotationPresent(io.swagger.v3.oas.annotations.tags.Tag.class)) {
                    io.swagger.v3.oas.annotations.tags.Tag tagAnnotation = servletClass.getAnnotation(io.swagger.v3.oas.annotations.tags.Tag.class);
                    operation.setTags(List.of(tagAnnotation.name()));
                }
                
                // Generate responses from standard Swagger ApiResponses annotations
                operation.setResponses(createResponsesFromSwaggerAnnotations(method));
                
            } else {
                // Fallback to default generation if no Swagger annotations
                String methodName = method.getName();
                operation.setSummary(generateSummaryFromMethodName(methodName));
                operation.setDescription(generateDescriptionFromMethodName(methodName));
                
                // Extract tags from class-level annotation
                if (servletClass.isAnnotationPresent(io.swagger.v3.oas.annotations.tags.Tag.class)) {
                    io.swagger.v3.oas.annotations.tags.Tag tagAnnotation = servletClass.getAnnotation(io.swagger.v3.oas.annotations.tags.Tag.class);
                    operation.setTags(List.of(tagAnnotation.name()));
                } else {
                    String tagName = generateTagFromClassName(servletClass.getSimpleName());
                    operation.setTags(List.of(tagName));
                }
                
                operation.setResponses(createDefaultResponses(method.getName()));
            }
            
            // Generate parameters for path variables
            operation.setParameters(createParametersFromPath(servletClass));
            
            // Generate request body for POST/PUT methods
            if (method.getName().equals("doPost") || method.getName().equals("doPut")) {
                operation.setRequestBody(createDefaultRequestBody());
            }
            
            return operation;
            
        } catch (Exception e) {
            System.err.println("Error creating operation from method " + method.getName() + ": " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Creates API responses from standard Swagger ApiResponses annotations.
     */
    private ApiResponses createResponsesFromSwaggerAnnotations(Method method) {
        ApiResponses responses = new ApiResponses();
        
        // Check for ApiResponses annotation
        if (method.isAnnotationPresent(io.swagger.v3.oas.annotations.responses.ApiResponses.class)) {
            io.swagger.v3.oas.annotations.responses.ApiResponses apiResponsesAnnotation = method.getAnnotation(io.swagger.v3.oas.annotations.responses.ApiResponses.class);
            
            for (io.swagger.v3.oas.annotations.responses.ApiResponse responseAnnotation : apiResponsesAnnotation.value()) {
                ApiResponse response = new ApiResponse()
                    .description(responseAnnotation.description());
                
                // Add content if specified
                if (responseAnnotation.content().length > 0) {
                    Content content = new Content();
                    for (io.swagger.v3.oas.annotations.media.Content contentAnnotation : responseAnnotation.content()) {
                        MediaType mediaType = new MediaType();
                        
                        // Add schema if specified
                        if (contentAnnotation.schema().implementation() != Void.class) {
                            Schema<?> schema = new Schema<>();
                            schema.setType("object");
                            mediaType.setSchema(schema);
                        }
                        
                        content.addMediaType(contentAnnotation.mediaType(), mediaType);
                    }
                    response.setContent(content);
                }
                
                responses.addApiResponse(responseAnnotation.responseCode(), response);
            }
        } else {
            // Fallback to default responses
            responses = createDefaultResponses(method.getName());
        }
        
        return responses;
    }
    
    
    /**
     * Generates a summary from method name.
     */
    private String generateSummaryFromMethodName(String methodName) {
        switch (methodName) {
            case "doGet":
                return "Get Resource";
            case "doPost":
                return "Create Resource";
            case "doPut":
                return "Update Resource";
            case "doDelete":
                return "Delete Resource";
            case "doPatch":
                return "Patch Resource";
            default:
                return "Operation";
        }
    }
    
    /**
     * Generates a description from method name.
     */
    private String generateDescriptionFromMethodName(String methodName) {
        switch (methodName) {
            case "doGet":
                return "Retrieve data from the server";
            case "doPost":
                return "Create new data on the server";
            case "doPut":
                return "Update existing data on the server";
            case "doDelete":
                return "Remove data from the server";
            case "doPatch":
                return "Partially update data on the server";
            default:
                return "Perform operation on the server";
        }
    }
    
    /**
     * Generates a tag from class name.
     */
    private String generateTagFromClassName(String className) {
        if (className.contains("Health")) return "Health";
        if (className.contains("Topic")) return "Topics";
        if (className.contains("Message")) return "Messages";
        if (className.contains("Metric")) return "Metrics";
        return "General";
    }
    
    /**
     * Creates default API responses.
     */
    private ApiResponses createDefaultResponses(String methodName) {
        ApiResponses responses = new ApiResponses();
        
        // Add success response
        ApiResponse successResponse = new ApiResponse()
            .description("Operation completed successfully")
            .content(new Content()
                .addMediaType("application/json", new MediaType()
                    .schema(new Schema<>().type("object"))));
        responses.addApiResponse("200", successResponse);
        
        // Add error responses
        responses.addApiResponse("400", new ApiResponse().description("Bad Request"));
        responses.addApiResponse("500", new ApiResponse().description("Internal Server Error"));
        
        // Add specific responses based on method
        if (methodName.equals("doPost")) {
            responses.addApiResponse("201", new ApiResponse().description("Resource Created"));
        }
        if (methodName.equals("doGet")) {
            responses.addApiResponse("404", new ApiResponse().description("Resource Not Found"));
        }
        
        return responses;
    }
    
    /**
     * Creates parameters from path patterns.
     */
    private List<Parameter> createParametersFromPath(Class<?> servletClass) {
        List<Parameter> parameters = new ArrayList<>();
        
        // Add path parameters based on servlet mapping
        if (servletClass.getSimpleName().contains("Message")) {
            Parameter topicParam = new Parameter()
                .name("topicName")
                .in("path")
                .required(true)
                .description("Name of the topic")
                .schema(new Schema<>().type("string"));
            parameters.add(topicParam);
            
            Parameter offsetParam = new Parameter()
                .name("offset")
                .in("query")
                .required(false)
                .description("Message offset")
                .schema(new Schema<>().type("integer").format("int64"));
            parameters.add(offsetParam);
        }
        
        return parameters;
    }
    
    /**
     * Creates a default request body for POST/PUT operations.
     */
    private RequestBody createDefaultRequestBody() {
        return new RequestBody()
            .required(true)
            .content(new Content()
                .addMediaType("application/json", new MediaType()
                    .schema(new Schema<>()
                        .type("object")
                        .addProperty("data", new Schema<>().type("string").description("Request data")))));
    }
    
    /**
     * Creates API information.
     */
    private Info createInfo() {
        return new Info()
            .title("FastQueue2 API")
            .description("High-Performance Message Queue Server REST API - Auto-generated from servlet annotations")
            .version("1.0.0")
            .contact(new Contact()
                .name("FastQueue2 Team")
                .email("support@fastqueue2.com")
                .url("https://github.com/mahmoudreda/fastqueue2"))
            .license(new License()
                .name("MIT License")
                .url("https://opensource.org/licenses/MIT"));
    }
    
    /**
     * Creates server configurations.
     */
    private List<Server> createServers() {
        List<Server> servers = new ArrayList<>();
        servers.add(new Server()
            .url("http://localhost:" + config.getServerPort())
            .description("FastQueue2 Development Server"));
        servers.add(new Server()
            .url("https://api.fastqueue2.com")
            .description("FastQueue2 Production Server"));
        return servers;
    }
    
    /**
     * Creates API tags for organization.
     */
    private List<Tag> createTags() {
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag()
            .name("Health")
            .description("Health check and system status endpoints"));
        tags.add(new Tag()
            .name("Topics")
            .description("Topic management operations"));
        tags.add(new Tag()
            .name("Messages")
            .description("Message publishing and consumption"));
        tags.add(new Tag()
            .name("Metrics")
            .description("System metrics and monitoring"));
        tags.add(new Tag()
            .name("General")
            .description("General operations"));
        return tags;
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        String pathInfo = request.getPathInfo();
        if (pathInfo == null) {
            pathInfo = "";
        }
        
        switch (pathInfo) {
            case "":
            case "/":
                serveSwaggerUI(response);
                break;
            case "/swagger.json":
                serveOpenAPISpec(response);
                break;
            case "/swagger.yaml":
                serveOpenAPIYAML(response);
                break;
            case "/index.html":
                // Redirect common Swagger UI paths to our main UI
                serveSwaggerUI(response);
                break;
            default:
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                break;
        }
    }
    
    /**
     * Serves the Swagger UI HTML page.
     */
    private void serveSwaggerUI(HttpServletResponse response) throws IOException {
        response.setContentType("text/html; charset=UTF-8");
        
        try (PrintWriter out = response.getWriter()) {
            out.println(createSwaggerUIHTML());
        }
    }
    
    /**
     * Serves the OpenAPI specification in JSON format.
     */
    private void serveOpenAPISpec(HttpServletResponse response) throws IOException {
        response.setContentType("application/json; charset=UTF-8");
        
        // Refresh OpenAPI by re-scanning servlet classes
        this.openAPI = createCompleteOpenAPI();
        
        String json = objectWriter.writeValueAsString(openAPI);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }
    
    /**
     * Serves the OpenAPI specification in YAML format.
     */
    private void serveOpenAPIYAML(HttpServletResponse response) throws IOException {
        response.setContentType("text/yaml; charset=UTF-8");
        
        // For now, return JSON (YAML conversion would require additional dependency)
        serveOpenAPISpec(response);
    }
    
    /**
     * Creates the Swagger UI HTML page.
     */
    private String createSwaggerUIHTML() {
        return """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>FastQueue2 API Documentation</title>
                <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui.css" />
                <style>
                    html {
                        box-sizing: border-box;
                        overflow: -moz-scrollbars-vertical;
                        overflow-y: scroll;
                    }
                    *, *:before, *:after {
                        box-sizing: inherit;
                    }
                    body {
                        margin:0;
                        background: #fafafa;
                    }
                </style>
            </head>
            <body>
                <div id="swagger-ui"></div>
                <script src="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui-bundle.js"></script>
                <script src="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui-standalone-preset.js"></script>
                <script>
                    window.onload = function() {
                        const ui = SwaggerUIBundle({
                            url: '/swagger/swagger.json',
                            dom_id: '#swagger-ui',
                            deepLinking: true,
                            presets: [
                                SwaggerUIBundle.presets.apis,
                                SwaggerUIStandalonePreset
                            ],
                            plugins: [
                                SwaggerUIBundle.plugins.DownloadUrl
                            ],
                            layout: "StandaloneLayout",
                            tryItOutEnabled: true,
                            requestInterceptor: function(request) {
                                console.log('Request:', request);
                                return request;
                            },
                            responseInterceptor: function(response) {
                                console.log('Response:', response);
                                return response;
                            }
                        });
                    };
                </script>
            </body>
            </html>
            """;
    }
}