# FastQueue2 - High-Performance Message Queue Server

## Overview

FastQueue2 is a high-performance, persistent message queue server built using a log-based architecture. It's designed to provide reliable message delivery, high throughput, and durability while maintaining simplicity and efficiency.

## Architecture

### Core Design Principles

FastQueue2 is built on the fundamental concept of **logs as a data structure**. This approach is used by many successful systems including:
- **Filesystems**: ext's journal for crash recovery
- **Databases**: PostgreSQL's Write-Ahead Log (WAL)
- **Distributed Systems**: Raft consensus algorithm
- **State Management**: Redux's action log

The core idea is to treat all data changes as an **ordered, append-only sequence of records**.

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Message Queue Server                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Topic A   │  │   Topic B   │  │   Topic C   │  ...   │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         Log Layer                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  Segment 1  │  │  Segment 2  │  │  Segment 3  │  ...   │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Segment Layer                          │
│  ┌─────────────┐              ┌─────────────┐              │
│  │ Store File  │              │ Index File  │              │
│  │ (Messages)  │              │ (Offsets)   │              │
│  └─────────────┘              └─────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

### Component Breakdown

#### 1. Store File
- **Purpose**: Append-only file containing actual message data
- **Characteristics**: 
  - Sequential writes for maximum performance
  - Immutable once written
  - Crash-safe with fsync operations
- **Format**: Binary format with message headers and payload

#### 2. Index File
- **Purpose**: Maps record offsets to physical byte positions in store file
- **Characteristics**:
  - Memory-mapped for high performance
  - Enables O(1) lookups by offset
  - Sparse index for memory efficiency
- **Format**: Fixed-size entries mapping offset → byte position

#### 3. Segment
- **Purpose**: Abstraction combining one store file + one index file
- **Responsibilities**:
  - Coordinate writes between store and index
  - Track current size and enforce limits
  - Handle segment rotation when full
- **Lifecycle**: Active → Full → Archived → Deleted

#### 4. Log
- **Purpose**: Highest-level abstraction managing collection of segments
- **Responsibilities**:
  - Direct writes to active segment
  - Create new segments when active segment is full
  - Route reads to appropriate segments
  - Manage segment lifecycle and cleanup
  - Recover state from disk on startup

#### 5. Message Queue Server
- **Purpose**: High-level API for external clients
- **Features**:
  - Multiple topic support
  - Producer/Consumer APIs
  - Offset-based message consumption
  - HTTP/gRPC interfaces

## Key Features

### Performance
- **High Throughput**: Sequential writes and memory-mapped reads
- **Low Latency**: Direct memory access to index files
- **Efficient Storage**: Sparse indexing and segment-based cleanup

### Reliability
- **Durability**: All writes are fsync'd to disk
- **Crash Recovery**: Automatic state recovery from disk files
- **Data Integrity**: Checksums and validation

### Scalability
- **Segment Rotation**: Automatic disk space management
- **Memory Efficiency**: Memory-mapped files with sparse indexing
- **Concurrent Access**: Thread-safe operations

## Implementation Plan

### Phase 1: Core Data Structures
1. **Record Class**: Message representation with offset, timestamp, data
2. **Store Class**: Append-only file operations with fsync
3. **Index Class**: Memory-mapped index for fast lookups
4. **Segment Class**: Store + Index coordination

### Phase 2: Log Management
1. **Log Class**: Segment collection management
2. **Segment Rotation**: Automatic creation and cleanup
3. **Recovery**: State reconstruction from disk

### Phase 3: Message Queue API
1. **Topic Class**: Message queue abstraction
2. **Producer Class**: Message publishing
3. **Consumer Class**: Message consumption
4. **Server Class**: External API endpoints

### Phase 4: Advanced Features
1. **Compression**: Message payload compression
2. **Replication**: Multi-node data replication
3. **Monitoring**: Metrics and health checks
4. **Configuration**: Flexible configuration system

## Technical Specifications

### File Formats

#### Store File Format
```
[Header][Message1][Header][Message2]...
Header: [Length:4][Timestamp:8][Checksum:4]
Message: [Payload:Length]
```

#### Index File Format
```
[Offset:8][Position:8][Length:4][Checksum:4]
```

### Configuration
- **Segment Size**: Maximum size before rotation (default: 1GB)
- **Index Interval**: How often to create index entries (default: every 1KB)
- **Retention**: How long to keep old segments (default: 7 days)
- **Compression**: Enable/disable message compression

### API Endpoints
- `POST /topics/{topic}/messages` - Publish message
- `GET /topics/{topic}/messages?offset={offset}&limit={limit}` - Consume messages
- `GET /topics/{topic}/offsets` - Get topic information
- `GET /health` - Health check

## Getting Started

### Prerequisites
- Java 11 or higher
- Maven 3.6 or higher
- 4GB+ RAM (for memory-mapped files)
- SSD storage (recommended for performance)

### Building
```bash
mvn clean compile
```

### Running
```bash
mvn exec:java -Dexec.mainClass="org.mahmoud.fastqueue.Fastqueue2"
```

## Design Decisions

### Why Log-Based Architecture?
1. **Simplicity**: Append-only operations are easier to reason about
2. **Performance**: Sequential writes are much faster than random writes
3. **Durability**: Logs provide natural crash recovery
4. **Replication**: Easy to replicate append-only logs
5. **Proven**: Used by many successful systems

### Why Memory-Mapped Files?
1. **Performance**: Direct memory access without system calls
2. **Efficiency**: OS handles caching and paging
3. **Simplicity**: No need to manage buffers manually

### Why Segments?
1. **Disk Management**: Automatic cleanup of old data
2. **Performance**: Smaller files are easier to manage
3. **Recovery**: Faster recovery from smaller segments
4. **Concurrency**: Different segments can be processed in parallel

## Future Enhancements

- **Clustering**: Multi-node deployment with leader election
- **Streaming**: Real-time message streaming
- **Schema Registry**: Message schema management
- **Dead Letter Queue**: Failed message handling
- **Metrics**: Prometheus/InfluxDB integration
- **Tracing**: Distributed tracing support

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
