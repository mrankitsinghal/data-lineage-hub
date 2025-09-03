# Data Lineage Hub - Entrypoints and Services

## Main Application Entrypoint

### FastAPI Application (src/main.py)
```python
# Main FastAPI app with lifespan management
app = FastAPI(
    title="Data Lineage Hub POC",
    description="A POC for data pipeline observability with OpenLineage and OpenTelemetry",
    version="1.0.0",
    lifespan=lifespan,
)
```

**Key Features:**
- OpenTelemetry instrumentation for internal monitoring
- CORS middleware for cross-origin requests  
- Kafka publisher initialization and cleanup in lifespan
- Routes mounted at `/api/v1` prefix
- Auto-documentation at `/docs`

## API Routes (src/api/routes.py)

### Core Endpoints
- **`GET /api/v1/health`** - Health check endpoint
- **`POST /api/v1/lineage/ingest`** - Central lineage event ingestion
- **`POST /api/v1/telemetry/ingest`** - Telemetry data ingestion
- **`POST /api/v1/namespaces`** - Create new namespace
- **`GET /api/v1/namespaces`** - List all namespaces
- **`GET /api/v1/namespaces/{namespace_id}`** - Get namespace details
- **`PUT /api/v1/namespaces/{namespace_id}`** - Update namespace

### API Architecture
- FastAPI with Pydantic models for request/response validation
- Async endpoints for high throughput
- Structured logging with correlation IDs
- Error handling with proper HTTP status codes

## Consumer Services

### 1. LineageConsumer (src/consumers/lineage_consumer.py)
**Purpose**: Forwards OpenLineage events from Kafka to Marquez API

```python
class LineageConsumer:
    def __init__(self, kafka_servers: str, topic: str, marquez_url: str)
    async def start(self) -> None  # Main consumer loop
    async def process_message(self, message: str) -> None  # Process single event
```

**Data Flow**: 
```
Kafka openlineage-events topic → LineageConsumer → Marquez /api/v1/lineage → Real-time lineage visualization
```

**Key Features:**
- Immediate forwarding for real-time lineage
- Async HTTP client with retry logic
- Namespace-based event routing
- Error handling with dead letter patterns

**Run Command**: `make run-lineage-consumer` or `poetry run python -m src.consumers.lineage_consumer`

### 2. OTelConsumer (src/consumers/otel_consumer.py)
**Purpose**: Process OTEL spans and metrics from Kafka to ClickHouse

```python
class OTelConsumer:
    def __init__(self, kafka_servers: str, clickhouse_client: ClickHouseClient)
    async def start(self) -> None  # Main consumer loop
    async def process_spans_message(self, message: str) -> None
    async def process_metrics_message(self, message: str) -> None
```

**Data Flow**:
```
Kafka otel-spans/otel-metrics topics → OTelConsumer → ClickHouse → Grafana dashboards
```

**Key Features:**
- Time-series data storage for performance analytics  
- Batch processing for efficiency
- Schema validation and transformation
- Support for OTEL span and metric formats

**Run Command**: `make run-otel-consumer` or `poetry run python -m src.consumers.otel_consumer`

## SDK Entry Points

### Primary SDK Interface (src/sdk/__init__.py)
```python
# Main SDK exports for teams
from .client import LineageHubClient, TelemetryClient
from .config import LineageHubConfig, configure  
from .decorators import lineage_track, telemetry_track
from .models import LineageEvent, TelemetryData
from .types import AdapterType, DataFormat, DatasetSpec
```

### SDK Clients (src/sdk/client.py)

#### LineageHubClient
```python
class LineageHubClient:
    def __init__(self, config: LineageHubConfig)
    async def send_event(self, event: LineageEvent) -> bool
    def send_event_sync(self, event: LineageEvent) -> bool
```
- HTTP client for lineage event transmission
- Sync/async support
- Automatic retries and error handling

#### TelemetryClient  
```python
class TelemetryClient:
    def __init__(self, config: LineageHubConfig)
    async def send_telemetry(self, data: TelemetryData) -> bool
```
- OTEL-compatible telemetry transmission
- Metrics and span data support

#### BatchingLineageClient
```python 
class BatchingLineageClient:
    def __init__(self, config: LineageHubConfig, batch_size: int = 10)
    async def add_event(self, event: LineageEvent) -> None
    async def flush(self) -> None
```
- Batch processing for high-volume scenarios
- Configurable batch sizes
- Automatic flush on shutdown

### SDK Decorators (src/sdk/decorators.py)

#### @lineage_track
```python
@lineage_track(
    inputs=[{"type": "mysql", "name": "users.table", "format": "table", "namespace": "prod-db"}],
    outputs=[{"type": "s3", "name": "s3://bucket/output.parquet", "format": "parquet", "namespace": "processed"}]
)
def my_pipeline_function():
    # Team's pipeline logic
    pass
```

**Features:**
- Automatic START/COMPLETE/FAIL event generation
- Dict-based dataset specification 
- Support for sync/async functions
- Error tracking and duration metrics

#### @telemetry_track  
```python
@telemetry_track(operation_name="data_processing")
def my_function():
    # Team's function logic
    pass
```

**Features:**
- OTEL span creation
- Performance metrics collection
- Error tracking
- Custom operation naming

## Utilities and Shared Services

### Kafka Client (src/utils/kafka_client.py)
```python
class KafkaEventPublisher:
    def __init__(self, servers: str)
    async def publish_lineage_event(self, event: dict, namespace: str) -> bool
    async def publish_telemetry_data(self, data: dict) -> bool
    def close(self) -> None
```

### ClickHouse Client (src/utils/clickhouse_client.py)
- Time-series data storage
- Batch inserts for performance
- Schema management for OTEL data

### OpenLineage Client (src/utils/openlineage_client.py)
- HTTP client for Marquez API
- OpenLineage event formatting
- Async/sync support with retries

### Configuration (src/config.py)
```python
class Settings(BaseSettings):
    # API Configuration
    app_name: str = "Data Lineage Hub POC"
    port: int = 8000
    
    # Kafka Configuration  
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_openlineage_topic: str = "openlineage-events"
    
    # Multi-tenant Configuration
    namespace_isolation_enabled: bool = True
    auto_create_namespaces: bool = True
    
    # Enterprise Features
    rate_limiting_enabled: bool = True
    audit_logging_enabled: bool = True
```

## Service Management

### Running Services

#### Development Mode (All services)
```bash
make dev-setup  # Complete setup: dependencies + infrastructure + services
```

#### Individual Services  
```bash
make run-api                  # FastAPI server (port 8000)
make run-lineage-consumer     # OpenLineage event processing  
make run-otel-consumer        # OTEL data processing
```

#### Infrastructure
```bash
make start                    # Docker services (Kafka, Marquez, ClickHouse, Grafana)
make stop                     # Stop all Docker services
make status                   # Check service health
```

### Service Dependencies

**FastAPI App Dependencies:**
- Kafka (for event publishing)
- Redis (for rate limiting, optional)

**LineageConsumer Dependencies:**  
- Kafka (source of events)
- Marquez (target API)

**OTelConsumer Dependencies:**
- Kafka (source of events)  
- ClickHouse (target storage)

## Monitoring and Health Checks

### Health Check Endpoint (`/api/v1/health`)
```json
{
  "status": "healthy",
  "service": "Data Lineage Hub POC", 
  "version": "1.0.0",
  "dependencies": {
    "kafka": "healthy",
    "marquez": "healthy", 
    "clickhouse": "healthy"
  }
}
```

### Service Logs
```bash
make logs                     # All Docker service logs
docker logs -f data-lineage-hub-kafka-1  # Specific service
```

### Access Points
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/api/v1/health  
- **Marquez UI**: http://localhost:3000
- **Grafana**: http://localhost:3001 (admin/admin)