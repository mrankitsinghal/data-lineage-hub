# Data Lineage Hub - Project Overview

## Architecture Summary

Data Lineage Hub is a **centralized enterprise service** that provides data pipeline observability using OpenLineage and OpenTelemetry. The system has evolved from a standalone POC to a **unified service architecture** where teams use the SDK to send lineage events to the central hub.

## Core Architecture Principles

- **Centralized Service**: Single hub for all data lineage events across teams
- **Multi-tenant**: Namespace-based isolation for different teams/organizations
- **SDK-driven**: Teams integrate via decorators and client libraries
- **Real-time Processing**: Kafka-based event streaming with immediate visualization in Marquez

## Unified Source Structure (Current)

All code is now consolidated under `src/` with clean separation:

```text
src/
├── main.py                 # FastAPI application entry point
├── config.py              # Centralized settings (Pydantic)
├── api/                   # Central ingestion REST API endpoints
│   ├── routes.py          # Main API routes (/lineage/ingest, /telemetry/ingest, /namespaces)
│   ├── models.py          # API request/response models
│   └── middleware.py      # CORS, authentication, rate limiting
├── consumers/             # Kafka event consumers
│   ├── lineage_consumer.py # OpenLineage events → Marquez
│   └── otel_consumer.py   # OTEL metrics → ClickHouse
├── services/              # Core business logic
│   └── namespace.py       # Multi-tenant namespace management
├── utils/                 # Shared utilities
│   ├── kafka_client.py    # KafkaEventPublisher
│   ├── clickhouse_client.py # ClickHouse storage client
│   ├── openlineage_client.py # Marquez API client
│   ├── otel_config.py     # OpenTelemetry configuration
│   └── logging_config.py  # Structured logging via structlog
├── sdk/                   # ✅ Unified SDK (teams import from src.sdk)
│   ├── client.py          # LineageHubClient, TelemetryClient, BatchingLineageClient
│   ├── decorators.py      # @lineage_track, @telemetry_track
│   ├── config.py          # SDK configuration management
│   ├── models.py          # Pydantic models for lineage events
│   ├── types.py           # Type definitions (AdapterType, DataFormat, DatasetSpec)
│   └── cli.py             # Command-line interface
└── examples/              # ✅ SDK usage examples
    ├── basic_usage.py     # Simple lineage tracking example
    ├── ml_pipeline.py     # ML workflow example
    └── pipeline_stages_example.py # Multi-stage pipeline

tests/
└── sdk/                   # ✅ SDK tests mirror src structure
    ├── test_client.py     # Client testing
    ├── test_config.py     # Configuration testing
    └── test_decorators.py # Decorator testing
```

## Data Flow Architecture

### 1. Team SDK Integration Flow

```text
Team Code → @lineage_track decorator → LineageHubClient → Central API → Kafka → Consumers → Storage/Visualization
```

1. **Team Integration**: Teams `from src.sdk import lineage_track`
2. **Decorator Usage**: Add `@lineage_track` to pipeline functions
3. **Dataset Specification**: Dict-based input/output specs:

   ```python
   @lineage_track(
       inputs=[{"type": "mysql", "name": "users.table", "format": "table", "namespace": "prod-db"}],
       outputs=[{"type": "s3", "name": "s3://bucket/output.parquet", "format": "parquet", "namespace": "processed"}]
   )
   ```

4. **Central Ingestion**: SDK sends events to FastAPI endpoints
5. **Event Processing**: API → Kafka → Consumers → Storage

### 2. OpenLineage Flow: Real-time Lineage

```text
SDK → /api/v1/lineage/ingest → KafkaEventPublisher → openlineage-events topic → LineageConsumer → Marquez API → Lineage UI
```

### 3. Telemetry Flow: Performance Metrics

```text
SDK → /api/v1/telemetry/ingest → KafkaEventPublisher → otel-spans/otel-metrics topics → OTelConsumer → ClickHouse → Grafana
```

## Key Components Detail

### Central API Service (src/main.py)

- FastAPI application with lifespan management
- OpenTelemetry instrumentation for internal monitoring
- CORS middleware for cross-origin requests
- Routes: health check, lineage ingestion, telemetry ingestion, namespace management

### SDK Architecture (src/sdk/)

- **Clients**: `LineageHubClient` (HTTP), `TelemetryClient` (OTEL), `BatchingLineageClient` (batched events)
- **Decorators**: `@lineage_track`, `@telemetry_track` for automatic instrumentation
- **Configuration**: Environment-based config with `LineageHubConfig`
- **Models**: Pydantic models ensuring type safety

### Consumer Services

- **LineageConsumer**: Forwards OpenLineage events to Marquez immediately (real-time lineage)
- **OTelConsumer**: Processes OTEL spans/metrics to ClickHouse for analytics

### Multi-tenant Features

- **Namespace Isolation**: Each team gets isolated view in Marquez
- **Cross-namespace Discovery**: Optional data discovery across teams
- **Rate Limiting**: Per-namespace request limiting
- **Quota Management**: Events per day per namespace

## Technology Stack

### Core Technologies

- **FastAPI**: REST API framework
- **Kafka**: Event streaming (confluent-kafka-python)
- **Marquez**: Data lineage visualization (OpenLineage compatible)
- **ClickHouse**: Time-series metrics storage
- **Grafana**: Metrics visualization and dashboards

### Development & Quality

- **Poetry**: Dependency management
- **Pydantic**: Configuration management and data validation
- **Ruff**: Fast Python linter and formatter
- **MyPy**: Static type checking
- **pytest**: Testing framework
- **pre-commit**: Code quality hooks

### Observability

- **OpenTelemetry**: Internal service instrumentation
- **structlog**: Structured logging
- **Prometheus**: Metrics export (via OTEL)

## Infrastructure Services (Docker)

```yaml
services:
  - kafka: Event streaming backbone
  - zookeeper: Kafka coordination
  - marquez: Data lineage storage and UI
  - postgres: Marquez database
  - clickhouse: Metrics time-series storage
  - grafana: Metrics visualization
  - otel-collector: OpenTelemetry collection pipeline
```

## Enterprise Features

### Multi-tenancy

- Namespace-based isolation
- Team-specific dataset views
- Cross-team data discovery
- Namespace quota management

### Security & Compliance

- API key validation (configurable)
- Rate limiting per namespace
- Audit logging
- RBAC for namespace permissions

### Monitoring & Alerting

- Health check dependencies
- Slack webhook integration
- Metrics export (30s intervals)
- Service-level monitoring via OpenTelemetry

## Access Points

- **API Documentation**: <http://localhost:8000/docs>
- **Marquez Lineage UI**: <http://localhost:3000>
- **Grafana Dashboards**: <http://localhost:3001> (admin/admin)
