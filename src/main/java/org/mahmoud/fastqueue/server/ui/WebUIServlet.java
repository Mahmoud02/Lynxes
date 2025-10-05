package org.mahmoud.fastqueue.server.ui;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mahmoud.fastqueue.service.MessageService;
import org.mahmoud.fastqueue.service.HealthService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Web UI servlet for FastQueue2 management interface.
 * Provides a modern, responsive web interface for managing topics, messages, and monitoring.
 */
public class WebUIServlet extends HttpServlet {
    private final MessageService messageService;
    private final HealthService healthService;
    private final ObjectMapper objectMapper;
    
    @Inject
    public WebUIServlet(MessageService messageService, HealthService healthService, ObjectMapper objectMapper) {
        this.messageService = messageService;
        this.healthService = healthService;
        this.objectMapper = objectMapper;
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
                serveDashboard(response);
                break;
            case "/api/health":
                serveHealthAPI(response);
                break;
            case "/api/metrics":
                serveMetricsAPI(response);
                break;
            case "/api/topics":
                serveTopicsAPI(response);
                break;
            default:
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                break;
        }
    }
    
    /**
     * Serves the main dashboard HTML page.
     */
    private void serveDashboard(HttpServletResponse response) throws IOException {
        response.setContentType("text/html; charset=UTF-8");
        
        try (PrintWriter out = response.getWriter()) {
            out.println(createDashboardHTML());
        }
    }
    
    /**
     * Serves health check API endpoint.
     */
    private void serveHealthAPI(HttpServletResponse response) throws IOException {
        response.setContentType("application/json; charset=UTF-8");
        
        HealthService.HealthStatus health = healthService.checkHealth();
        String json = objectMapper.writeValueAsString(health);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }
    
    /**
     * Serves metrics API endpoint.
     */
    private void serveMetricsAPI(HttpServletResponse response) throws IOException {
        response.setContentType("application/json; charset=UTF-8");
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("producerMessages", messageService.getProducerMessageCount());
        metrics.put("consumerMessages", messageService.getConsumerMessageCount());
        metrics.put("uptime", System.currentTimeMillis()); // TODO: Calculate actual uptime
        metrics.put("status", "running");
        
        String json = objectMapper.writeValueAsString(metrics);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }
    
    /**
     * Serves topics API endpoint.
     */
    private void serveTopicsAPI(HttpServletResponse response) throws IOException {
        response.setContentType("application/json; charset=UTF-8");
        
        // TODO: Implement actual topic listing
        Map<String, Object> topics = new HashMap<>();
        topics.put("topics", new String[0]);
        topics.put("count", 0);
        
        String json = objectMapper.writeValueAsString(topics);
        
        try (PrintWriter out = response.getWriter()) {
            out.print(json);
        }
    }
    
    /**
     * Creates the main dashboard HTML page.
     */
    private String createDashboardHTML() {
        return """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>FastQueue2 Management Dashboard</title>
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
                <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
                <style>
                    .sidebar {
                        min-height: 100vh;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    }
                    .main-content {
                        background-color: #f8f9fa;
                        min-height: 100vh;
                    }
                    .card {
                        border: none;
                        box-shadow: 0 0.125rem 0.25rem rgba(0, 0, 0, 0.075);
                        transition: all 0.3s ease;
                    }
                    .card:hover {
                        transform: translateY(-2px);
                        box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15);
                    }
                    .metric-card {
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                    }
                    .status-indicator {
                        width: 12px;
                        height: 12px;
                        border-radius: 50%;
                        display: inline-block;
                        margin-right: 8px;
                    }
                    .status-online {
                        background-color: #28a745;
                        animation: pulse 2s infinite;
                    }
                    @keyframes pulse {
                        0% { opacity: 1; }
                        50% { opacity: 0.5; }
                        100% { opacity: 1; }
                    }
                    .nav-link {
                        color: rgba(255, 255, 255, 0.8);
                        transition: all 0.3s ease;
                    }
                    .nav-link:hover, .nav-link.active {
                        color: white;
                        background-color: rgba(255, 255, 255, 0.1);
                        border-radius: 0.375rem;
                    }
                </style>
            </head>
            <body>
                <div class="container-fluid">
                    <div class="row">
                        <!-- Sidebar -->
                        <div class="col-md-3 col-lg-2 sidebar p-0">
                            <div class="p-3">
                                <h4 class="text-white mb-4">
                                    <i class="fas fa-rocket"></i> FastQueue2
                                </h4>
                                <nav class="nav flex-column">
                                    <a class="nav-link active" href="#" onclick="showSection('dashboard')">
                                        <i class="fas fa-tachometer-alt"></i> Dashboard
                                    </a>
                                    <a class="nav-link" href="#" onclick="showSection('topics')">
                                        <i class="fas fa-list"></i> Topics
                                    </a>
                                    <a class="nav-link" href="#" onclick="showSection('messages')">
                                        <i class="fas fa-envelope"></i> Messages
                                    </a>
                                    <a class="nav-link" href="#" onclick="showSection('metrics')">
                                        <i class="fas fa-chart-line"></i> Metrics
                                    </a>
                                    <a class="nav-link" href="/swagger/" target="_blank">
                                        <i class="fas fa-book"></i> API Docs
                                    </a>
                                </nav>
                            </div>
                        </div>
                        
                        <!-- Main Content -->
                        <div class="col-md-9 col-lg-10 main-content">
                            <div class="p-4">
                                <!-- Header -->
                                <div class="d-flex justify-content-between align-items-center mb-4">
                                    <h2 id="page-title">Dashboard</h2>
                                    <div class="d-flex align-items-center">
                                        <span class="status-indicator status-online"></span>
                                        <span class="text-muted">System Online</span>
                                    </div>
                                </div>
                                
                                <!-- Dashboard Section -->
                                <div id="dashboard-section">
                                    <div class="row mb-4">
                                        <div class="col-md-3">
                                            <div class="card metric-card text-center p-3">
                                                <h3 id="producer-count">0</h3>
                                                <p class="mb-0">Messages Published</p>
                                            </div>
                                        </div>
                                        <div class="col-md-3">
                                            <div class="card metric-card text-center p-3">
                                                <h3 id="consumer-count">0</h3>
                                                <p class="mb-0">Messages Consumed</p>
                                            </div>
                                        </div>
                                        <div class="col-md-3">
                                            <div class="card metric-card text-center p-3">
                                                <h3 id="topic-count">0</h3>
                                                <p class="mb-0">Active Topics</p>
                                            </div>
                                        </div>
                                        <div class="col-md-3">
                                            <div class="card metric-card text-center p-3">
                                                <h3 id="uptime">0s</h3>
                                                <p class="mb-0">Uptime</p>
                                            </div>
                                        </div>
                                    </div>
                                    
                                    <div class="row">
                                        <div class="col-md-8">
                                            <div class="card">
                                                <div class="card-header">
                                                    <h5 class="mb-0">System Status</h5>
                                                </div>
                                                <div class="card-body">
                                                    <div id="system-status">
                                                        <div class="d-flex justify-content-between align-items-center mb-2">
                                                            <span>Health Status</span>
                                                            <span class="badge bg-success" id="health-status">OK</span>
                                                        </div>
                                                        <div class="d-flex justify-content-between align-items-center mb-2">
                                                            <span>Server Port</span>
                                                            <span class="text-muted">8080</span>
                                                        </div>
                                                        <div class="d-flex justify-content-between align-items-center">
                                                            <span>Last Updated</span>
                                                            <span class="text-muted" id="last-updated">-</span>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                        <div class="col-md-4">
                                            <div class="card">
                                                <div class="card-header">
                                                    <h5 class="mb-0">Quick Actions</h5>
                                                </div>
                                                <div class="card-body">
                                                    <div class="d-grid gap-2">
                                                        <button class="btn btn-primary" onclick="showSection('topics')">
                                                            <i class="fas fa-plus"></i> Create Topic
                                                        </button>
                                                        <button class="btn btn-outline-primary" onclick="showSection('messages')">
                                                            <i class="fas fa-paper-plane"></i> Send Message
                                                        </button>
                                                        <a href="/swagger/" class="btn btn-outline-secondary" target="_blank">
                                                            <i class="fas fa-book"></i> View API Docs
                                                        </a>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                
                                <!-- Topics Section -->
                                <div id="topics-section" style="display: none;">
                                    <div class="card">
                                        <div class="card-header d-flex justify-content-between align-items-center">
                                            <h5 class="mb-0">Topics Management</h5>
                                            <button class="btn btn-primary" onclick="createTopic()">
                                                <i class="fas fa-plus"></i> Create Topic
                                            </button>
                                        </div>
                                        <div class="card-body">
                                            <div id="topics-list">
                                                <p class="text-muted">No topics found. Create your first topic to get started!</p>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                                
                                <!-- Messages Section -->
                                <div id="messages-section" style="display: none;">
                                    <div class="card">
                                        <div class="card-header">
                                            <h5 class="mb-0">Message Publishing</h5>
                                        </div>
                                        <div class="card-body">
                                            <form id="message-form">
                                                <div class="mb-3">
                                                    <label for="topic-name" class="form-label">Topic Name</label>
                                                    <input type="text" class="form-control" id="topic-name" placeholder="Enter topic name" required>
                                                </div>
                                                <div class="mb-3">
                                                    <label for="message-data" class="form-label">Message Data</label>
                                                    <textarea class="form-control" id="message-data" rows="3" placeholder="Enter message content" required></textarea>
                                                </div>
                                                <button type="submit" class="btn btn-primary">
                                                    <i class="fas fa-paper-plane"></i> Publish Message
                                                </button>
                                            </form>
                                        </div>
                                    </div>
                                </div>
                                
                                <!-- Metrics Section -->
                                <div id="metrics-section" style="display: none;">
                                    <div class="card">
                                        <div class="card-header">
                                            <h5 class="mb-0">System Metrics</h5>
                                        </div>
                                        <div class="card-body">
                                            <div id="metrics-content">
                                                <p class="text-muted">Loading metrics...</p>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
                <script>
                    // Global variables
                    let metricsInterval;
                    
                    // Initialize dashboard
                    document.addEventListener('DOMContentLoaded', function() {
                        loadMetrics();
                        metricsInterval = setInterval(loadMetrics, 5000); // Update every 5 seconds
                    });
                    
                    // Show different sections
                    function showSection(sectionName) {
                        // Hide all sections
                        document.querySelectorAll('[id$="-section"]').forEach(section => {
                            section.style.display = 'none';
                        });
                        
                        // Show selected section
                        document.getElementById(sectionName + '-section').style.display = 'block';
                        
                        // Update page title
                        document.getElementById('page-title').textContent = 
                            sectionName.charAt(0).toUpperCase() + sectionName.slice(1);
                        
                        // Update active nav link
                        document.querySelectorAll('.nav-link').forEach(link => {
                            link.classList.remove('active');
                        });
                        event.target.classList.add('active');
                        
                        // Load section-specific data
                        if (sectionName === 'topics') {
                            loadTopics();
                        } else if (sectionName === 'metrics') {
                            loadDetailedMetrics();
                        }
                    }
                    
                    // Load metrics
                    async function loadMetrics() {
                        try {
                            const response = await fetch('/ui/api/metrics');
                            const data = await response.json();
                            
                            document.getElementById('producer-count').textContent = data.producerMessages || 0;
                            document.getElementById('consumer-count').textContent = data.consumerMessages || 0;
                            document.getElementById('uptime').textContent = formatUptime(data.uptime);
                            document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
                        } catch (error) {
                            console.error('Error loading metrics:', error);
                        }
                    }
                    
                    // Load topics
                    async function loadTopics() {
                        try {
                            const response = await fetch('/ui/api/topics');
                            const data = await response.json();
                            
                            document.getElementById('topic-count').textContent = data.count || 0;
                            
                            const topicsList = document.getElementById('topics-list');
                            if (data.topics && data.topics.length > 0) {
                                topicsList.innerHTML = data.topics.map(topic => 
                                    `<div class="d-flex justify-content-between align-items-center p-2 border-bottom">
                                        <span>${topic}</span>
                                        <div>
                                            <button class="btn btn-sm btn-outline-primary me-2" onclick="viewTopic('${topic}')">
                                                <i class="fas fa-eye"></i> View
                                            </button>
                                            <button class="btn btn-sm btn-outline-danger" onclick="deleteTopic('${topic}')">
                                                <i class="fas fa-trash"></i> Delete
                                            </button>
                                        </div>
                                    </div>`
                                ).join('');
                            } else {
                                topicsList.innerHTML = '<p class="text-muted">No topics found. Create your first topic to get started!</p>';
                            }
                        } catch (error) {
                            console.error('Error loading topics:', error);
                        }
                    }
                    
                    // Load detailed metrics
                    async function loadDetailedMetrics() {
                        try {
                            const response = await fetch('/ui/api/metrics');
                            const data = await response.json();
                            
                            const metricsContent = document.getElementById('metrics-content');
                            metricsContent.innerHTML = `
                                <div class="row">
                                    <div class="col-md-6">
                                        <h6>Message Statistics</h6>
                                        <ul class="list-unstyled">
                                            <li>Producer Messages: <strong>${data.producerMessages || 0}</strong></li>
                                            <li>Consumer Messages: <strong>${data.consumerMessages || 0}</strong></li>
                                            <li>Processed Requests: <strong>${data.processedRequests || 0}</strong></li>
                                            <li>Error Count: <strong>${data.errorCount || 0}</strong></li>
                                        </ul>
                                    </div>
                                    <div class="col-md-6">
                                        <h6>System Information</h6>
                                        <ul class="list-unstyled">
                                            <li>Status: <span class="badge bg-success">${data.status || 'Unknown'}</span></li>
                                            <li>Uptime: <strong>${formatUptime(data.uptime)}</strong></li>
                                            <li>Last Updated: <strong>${new Date().toLocaleTimeString()}</strong></li>
                                        </ul>
                                    </div>
                                </div>
                            `;
                        } catch (error) {
                            console.error('Error loading detailed metrics:', error);
                        }
                    }
                    
                    // Create topic
                    function createTopic() {
                        const topicName = prompt('Enter topic name:');
                        if (topicName) {
                            // TODO: Implement topic creation
                            alert('Topic creation will be implemented soon!');
                        }
                    }
                    
                    // Message form submission
                    document.getElementById('message-form').addEventListener('submit', async function(e) {
                        e.preventDefault();
                        
                        const topicName = document.getElementById('topic-name').value;
                        const messageData = document.getElementById('message-data').value;
                        
                        try {
                            const response = await fetch(`/topics/${topicName}/messages`, {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
                                body: JSON.stringify({ data: messageData })
                            });
                            
                            if (response.ok) {
                                alert('Message published successfully!');
                                document.getElementById('message-form').reset();
                                loadMetrics(); // Refresh metrics
                            } else {
                                alert('Failed to publish message');
                            }
                        } catch (error) {
                            console.error('Error publishing message:', error);
                            alert('Error publishing message');
                        }
                    });
                    
                    // Utility functions
                    function formatUptime(uptime) {
                        if (!uptime) return '0s';
                        const seconds = Math.floor((Date.now() - uptime) / 1000);
                        if (seconds < 60) return seconds + 's';
                        const minutes = Math.floor(seconds / 60);
                        if (minutes < 60) return minutes + 'm ' + (seconds % 60) + 's';
                        const hours = Math.floor(minutes / 60);
                        return hours + 'h ' + (minutes % 60) + 'm';
                    }
                    
                    // Cleanup on page unload
                    window.addEventListener('beforeunload', function() {
                        if (metricsInterval) {
                            clearInterval(metricsInterval);
                        }
                    });
                </script>
            </body>
            </html>
            """;
    }
}
