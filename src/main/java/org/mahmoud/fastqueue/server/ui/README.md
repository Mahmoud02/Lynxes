# FastQueue2 Web UI Structure

This directory contains the Vaadin Flow-based web user interface for FastQueue2.

## Directory Structure

```
ui/
├── FastQueueUIServlet.java          # Main Vaadin servlet for UI routing
├── layout/                          # Layout components
│   └── MainLayout.java              # Main application layout with navigation
├── views/                           # View components (pages)
│   ├── DashboardView.java           # Main dashboard overview
│   ├── TopicsView.java              # Topic management (CRUD operations)
│   ├── MessagesView.java            # Message publish/consume operations
│   ├── MetricsView.java             # Server metrics and performance
│   └── HealthView.java              # Health status and diagnostics
└── components/                      # Reusable UI components (future)
```

## Package Organization

- **`org.mahmoud.fastqueue.server.ui`** - Main UI package
- **`org.mahmoud.fastqueue.server.ui.layout`** - Layout components
- **`org.mahmoud.fastqueue.server.ui.views`** - View components (pages)
- **`org.mahmoud.fastqueue.server.ui.components`** - Reusable components (future)

## Features

### Dashboard View (`/ui/`)
- Server status overview
- Key metrics summary
- Recent activity feed
- Quick stats cards

### Topics View (`/ui/topics`)
- List all topics
- Create new topics
- Delete topics
- Topic statistics

### Messages View (`/ui/messages`)
- Publish messages to topics
- Consume messages from topics
- Message history display
- Offset-based consumption

### Metrics View (`/ui/metrics`)
- Performance metrics
- System resource usage
- Topic statistics
- Real-time monitoring

### Health View (`/ui/health`)
- Overall server health
- Component health status
- System health metrics
- Diagnostic information

## Technology Stack

- **Vaadin Flow 24.3.10** - Java web framework
- **Lumo Theme** - Modern, responsive design
- **Jetty Servlet Integration** - Embedded web server
- **Pure Java** - No HTML/CSS/JavaScript required

## Access

- **Web UI**: `http://localhost:8080/ui/`
- **API Endpoints**: `http://localhost:8080/health`, `/topics`, `/metrics`

## Future Enhancements

- Reusable components in `components/` package
- Real-time updates with WebSocket
- Advanced charts and visualizations
- User authentication and authorization
- Custom themes and branding
