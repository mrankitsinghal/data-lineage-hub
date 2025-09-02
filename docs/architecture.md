# Data Lineage Hub POC - Architecture Documentation

## Overview

The Data Lineage Hub POC demonstrates a production-ready architecture for data pipeline observability using industry-standard tools: **OpenLineage** for data lineage tracking, **Marquez** for lineage storage and visualization, and **OpenTelemetry** for distributed tracing and metrics collection.

## High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Pipeline │───▶│   API Gateway   │───▶│      Kafka      │───▶│   Consumers     │
│   (ETL Stages)  │    │   (FastAPI)     │    │  (Event Stream) │    │  (Processing)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         │                        │                        │                        │
         ▼                        ▼                        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ OpenLineage     │    │ OpenTelemetry   │    │   3 Topics:     │    │    Storage      │
│ Events          │    │ Traces/Metrics  │    │ • lineage-events│    │ • Marquez       │
│                 │    │                 │    │ • otel-spans    │    │ • ClickHouse    │
│                 │    │                 │    │ • otel-metrics  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                                                     │
                                                                                     ▼
                                                                     ┌─────────────────┐
                                                                     │ Visualization   │
                                                                     │ • Marquez UI    │
                                                                     │ • Grafana       │
                                                                     │ • Jaeger        │
                                                                     └─────────────────┘
```

## System Components

### 1. Data Pipeline Layer

#### Pipeline Stages
```python
# Each stage is decorated with OpenLineage tracking
@openlineage_job(
    job_name="extract_stage",
    description="Extract data from input source",
    inputs=[DatasetInfo(...)]
)
def extract(input_path: str) -> pd.DataFrame:
    # Stage implementation
```

**Three-Stage ETL Pipeline:**
- **Extract Stage**: Reads data from CSV/JSON sources with schema inference
- **Transform Stage**: Cleans, validates, and enriches data with derived fields
- **Load Stage**: Writes processed data to output destinations

**Key Features:**
- Automatic lineage tracking via decorators
- Schema evolution tracking
- Data quality validation
- Processing metrics collection

#### Pipeline Executor
- Orchestrates complete ETL workflow
- Manages OpenLineage run lifecycle (START → COMPLETE/FAIL)
- Correlates pipeline execution with distributed tracing
- Updates pipeline run status in real-time

### 2. API Gateway Layer

#### FastAPI Application
```python
# Main endpoints
POST /api/v1/pipeline/run      # Trigger pipeline execution
GET  /api/v1/pipeline/run/{id} # Get execution status
GET  /api/v1/metrics           # Pipeline performance metrics
POST /api/v1/webhook/lineage   # Receive lineage events
GET  /api/v1/health            # Health check with dependencies
```

**Features:**
- **Background Task Processing**: Non-blocking pipeline execution
- **OpenAPI Documentation**: Auto-generated API docs at `/docs`
- **Health Monitoring**: Dependency status checking
- **CORS Support**: Cross-origin requests enabled
- **Structured Logging**: All requests/responses logged

### 3. Event Streaming Layer

#### Kafka Topics Architecture
```yaml
openlineage-events:
  description: "OpenLineage JSON events (START/COMPLETE/FAIL)"
  partitions: 3
  key: run_id

otel-spans:
  description: "OpenTelemetry distributed tracing spans"
  partitions: 3
  key: trace_id

otel-metrics:
  description: "OpenTelemetry metrics and counters"
  partitions: 3
  key: service_name
```

**Event Publishing:**
- **Kafka Producer**: Reliable event publishing with acknowledgments
- **Serialization**: JSON for OpenLineage, Protocol Buffers for OTEL
- **Partitioning Strategy**: By run_id/trace_id for ordered processing
- **Error Handling**: Dead letter queues for failed messages

### 4. Event Processing Layer

#### OpenLineage Consumer
```python
class LineageConsumer:
    def start(self) -> None:
        # Consume from openlineage-events topic
        # Forward events to Marquez HTTP API
        # Handle schema validation and retries
```

**Responsibilities:**
- Consumes OpenLineage events from Kafka
- Validates event schema against OpenLineage specification
- Forwards events to Marquez `/api/v1/lineage` endpoint
- Implements retry logic and error handling

#### OpenTelemetry Consumer
```python
class OTelConsumer:
    def _process_spans_batch(self, spans) -> None:
        # Transform OTEL spans to ClickHouse schema
        # Batch insert for performance
        # Extract service/operation metadata
```

**Responsibilities:**
- Processes OTEL spans and metrics in batches
- Transforms data for ClickHouse time-series storage
- Correlates traces with lineage events via run IDs
- Handles schema evolution and data retention

### 5. Storage Layer

#### Marquez (OpenLineage Storage)
```yaml
Components:
  - PostgreSQL: Backend metadata storage
  - Web UI: Visual lineage exploration (port 3000)
  - REST API: Lineage queries and metadata access

Capabilities:
  - Dataset dependency graphs
  - Job execution history
  - Column-level lineage tracking
  - Facet-based metadata extension
```

#### ClickHouse (Time-Series Analytics)
```sql
-- Optimized tables for OTEL data
CREATE TABLE otel.traces (
    timestamp DateTime64(9),
    trace_id String,
    service_name String,
    duration_ns UInt64,
    -- Optimized for time-series queries
) ENGINE = MergeTree()
ORDER BY (service_name, operation_name, timestamp);
```

**Schema Design:**
- **Traces Table**: Distributed tracing spans with duration metrics
- **Metrics Table**: Counter/gauge measurements over time
- **Pipeline Runs Table**: Aggregated pipeline execution metrics
- **Partitioning**: By service and time for query performance

### 6. Visualization Layer

#### Marquez Web UI
- **Lineage Graphs**: Visual DAG representation of data dependencies
- **Dataset Explorer**: Schema evolution and metadata browsing
- **Job Timeline**: Execution history and run comparisons
- **Search & Discovery**: Find datasets and jobs across namespaces

#### Grafana Dashboards
```json
{
  "panels": [
    "Pipeline Success Rate Over Time",
    "Average Execution Duration",
    "Records Processed Per Hour",
    "Error Rate by Stage",
    "Resource Utilization"
  ]
}
```

#### Jaeger Tracing
- **Distributed Traces**: End-to-end request flow visualization
- **Performance Analysis**: Bottleneck identification
- **Service Dependencies**: Inter-service communication mapping

## Data Flow Architecture

### 1. Pipeline Execution Flow
```
1. API Request → POST /api/v1/pipeline/run
2. FastAPI → Background Task Creation
3. Pipeline Executor → Stage Execution (Extract → Transform → Load)
4. Each Stage → OpenLineage Event Generation
5. Events → Kafka Publishing (openlineage-events topic)
6. Consumer → Event Processing to Marquez
7. Marquez → Lineage Graph Updates
```

### 2. Observability Flow
```
1. Pipeline Stage → OpenTelemetry Span Creation
2. Span Context → Trace Correlation
3. Metrics Collection → Custom Pipeline Metrics
4. Events → Kafka Publishing (otel-spans, otel-metrics topics)
5. Consumer → ClickHouse Storage
6. Grafana → Dashboard Visualization
```

### 3. Event Correlation
```python
# Correlation via shared identifiers
run_id = "uuid-pipeline-run"
trace_id = "uuid-distributed-trace"

# OpenLineage Event
{
  "run": {"runId": run_id},
  "job": {"name": "pipeline-stage"},
  # ... lineage metadata
}

# OpenTelemetry Span
{
  "traceId": trace_id,
  "spanId": "uuid-span",
  "attributes": {"pipeline.run_id": run_id}
}
```

## Integration Patterns

### OpenLineage Integration
- **Event-Driven**: Automatic generation via method decorators
- **Standard Compliant**: OpenLineage v1.x specification adherence
- **Schema Tracking**: Dataset field-level lineage
- **Facet Extension**: Custom metadata via OpenLineage facets

### OpenTelemetry Integration
- **Auto-Instrumentation**: FastAPI and HTTP requests
- **Custom Metrics**: Pipeline-specific business metrics
- **Distributed Context**: Trace propagation across services
- **Resource Attribution**: Service identification and versioning

### Infrastructure Integration
- **Docker Compose**: Local development environment
- **Health Checks**: Service dependency monitoring
- **Configuration Management**: Environment-based settings
- **Graceful Shutdown**: Proper resource cleanup

## Scalability Considerations

### Horizontal Scaling
- **Stateless Services**: API and consumers can be replicated
- **Kafka Partitioning**: Parallel processing of events
- **Load Balancing**: Multiple API instances behind load balancer
- **Database Sharding**: ClickHouse cluster for high volume

### Performance Optimization
- **Batch Processing**: Consumer batch sizes configurable
- **Connection Pooling**: Database connection management
- **Async I/O**: Non-blocking operations throughout
- **Caching**: Redis for frequently accessed metadata

### Reliability Patterns
- **Circuit Breakers**: Fault tolerance for external dependencies
- **Retry Logic**: Exponential backoff for transient failures
- **Health Monitoring**: Automated failover capabilities
- **Data Durability**: Kafka persistence and replication

## Security Architecture

### API Security
- **Authentication**: Token-based auth (configurable)
- **Authorization**: Role-based access control
- **Input Validation**: Pydantic models for request validation
- **Rate Limiting**: Protection against abuse

### Data Security
- **Encryption**: TLS for all network communication
- **Secrets Management**: Environment variable configuration
- **Network Isolation**: Docker network segmentation
- **Audit Logging**: All operations logged with correlation IDs

## Deployment Architecture

### Local Development
```yaml
Services:
  - API Server: Python process
  - Infrastructure: Docker Compose
  - Dependencies: Poetry virtual environment
  - Monitoring: Local service endpoints
```

### Production Considerations
```yaml
Infrastructure:
  - Kubernetes: Container orchestration
  - Helm Charts: Deployment management
  - Ingress: API gateway and routing
  - Service Mesh: Inter-service communication

Monitoring:
  - Prometheus: Metrics collection
  - Grafana: Centralized dashboards
  - Jaeger: Distributed tracing
  - AlertManager: Incident response
```

This architecture provides a robust foundation for data pipeline observability that can scale from POC to production environments while maintaining observability, reliability, and extensibility.
