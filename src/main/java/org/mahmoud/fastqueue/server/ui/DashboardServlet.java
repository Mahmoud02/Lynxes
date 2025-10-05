package org.mahmoud.fastqueue.server.ui;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Simple servlet to serve static dashboard files
 */
public class DashboardServlet extends HttpServlet {
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        String pathInfo = request.getPathInfo();
        if (pathInfo == null || pathInfo.equals("/") || pathInfo.equals("/dashboard")) {
            // Serve the main dashboard
            serveDashboard(response);
        } else {
            // Serve other static files if needed
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
    }
    
    private void serveDashboard(HttpServletResponse response) throws IOException {
        try {
            // Try to read the static HTML file
            Path dashboardPath = Paths.get("src/main/resources/static/dashboard.html");
            if (Files.exists(dashboardPath)) {
                String content = Files.readString(dashboardPath);
                response.setContentType("text/html; charset=UTF-8");
                try (PrintWriter out = response.getWriter()) {
                    out.print(content);
                }
            } else {
                // Fallback: serve a simple message
                response.setContentType("text/html; charset=UTF-8");
                try (PrintWriter out = response.getWriter()) {
                    out.println("<!DOCTYPE html>");
                    out.println("<html><head><title>FastQueue2 Dashboard</title></head>");
                    out.println("<body><h1>FastQueue2 Dashboard</h1>");
                    out.println("<p>Dashboard file not found. Please check the static resources.</p>");
                    out.println("</body></html>");
                }
            }
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.setContentType("text/plain");
            try (PrintWriter out = response.getWriter()) {
                out.println("Error serving dashboard: " + e.getMessage());
            }
        }
    }
}