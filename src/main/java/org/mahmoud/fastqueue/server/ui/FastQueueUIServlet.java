package org.mahmoud.fastqueue.server.ui;

import com.vaadin.flow.server.VaadinServlet;
import jakarta.servlet.annotation.WebServlet;

/**
 * Vaadin servlet for FastQueue2 Web UI
 * Handles all UI routing and serves the dashboard
 */
@WebServlet(urlPatterns = "/ui/*", name = "FastQueueUIServlet", asyncSupported = true)
public class FastQueueUIServlet extends VaadinServlet {
    
    @Override
    public String getServletName() {
        return "FastQueueUIServlet";
    }
}
