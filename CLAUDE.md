# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Lineage Hub POC demonstrating data pipeline observability using OpenLineage and Marquez for data lineage tracking, combined with OpenTelemetry and OTEL Collector for distributed tracing and metrics. The system processes data through a 3-stage ETL pipeline (Extract → Transform → Load) while automatically tracking lineage and performance metrics.

### Architecture Overview
- **OpenTelemetry**: Metrics & traces → OTEL Collector → ClickHouse (Grafana dashboards)
- **OpenLineage**: Lineage events → Kafka → Marquez
- **Standards-based**: Uses OTLP protocol and OTEL Collector for vendor-agnostic observability

## Development Environment

**Python 3.11+** with **Poetry** for dependency management. Local `.venv` directory configuration.

## Essential Commands

### Setup & Environment
```bash
make dev-setup              # Complete development setup (recommended first command)
make install-dev            # Install all dependencies including dev tools
make venv-recreate          # Recreate virtual environment from scratch
poetry shell                # Activate Poetry environment
```

### Running Services
```bash
make start                  # Start infrastructure (Docker services: Kafka, Marquez, ClickHouse, OTEL Collector)
make setup-grafana-plugins  # Install required Grafana plugins (ClickHouse datasource)
make run-api                # Run FastAPI server (port 8000) - sends OTEL data to collector
make run-lineage-consumer   # Run OpenLineage event consumer (Kafka → Marquez)
# Note: OTEL Collector runs in Docker, no separate OTEL consumer needed
```

### Code Quality (MUST RUN BEFORE COMMITTING)

```bash
make check                  # Run all quality checks (Ruff + MyPy) - run this first!
make format                 # Format code with Ruff
make fix                   # Auto-fix linting issues with Ruff
make test                  # Run pytest tests
poetry run pre-commit run --all-files  # Run pre-commit hooks manually
```

### Testing

```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_pipeline.py

# Run with coverage
poetry run pytest --cov=src --cov-report=html

# Run only unit tests
poetry run pytest -m unit

# Run only integration tests
poetry run pytest -m integration

# Test end-to-end pipeline
make test-pipeline
```

### Debugging & Monitoring
```bash
make status                 # Check all service health
make logs                   # View Docker service logs
docker logs -f data-lineage-hub-kafka-1  # Follow specific service logs
```

## Architecture & Key Components

### Observability Flow Architecture

1. **OpenTelemetry SDK** → **OTEL Collector** → **ClickHouse Backend**
   - FastAPI app sends OTLP data to OTEL Collector (port 4317/4318)
   - OTEL Collector processes and routes to: ClickHouse for metrics storage
   - Standards-compliant, vendor-agnostic approach

2. **Lineage Flow**: **OpenLineage** → **Kafka** → **Marquez**
   - KafkaEventPublisher (`src/utils/kafka_client.py`): Publishes lineage events
   - LineageConsumer forwards events to Marquez API
   - Separate from observability data (different concern)

3. **Configuration**:
   - OTEL configuration in `src/utils/otel_config.py`
   - Uses OTLP exporters instead of custom Kafka exporters
   - PipelineMetrics class generates custom business metrics

### Pipeline Execution Flow

1. **API Endpoint** (`/api/v1/pipeline/run`) → Creates background task
2. **PipelineExecutor** (`src/pipeline/executor.py`) → Orchestrates stages
3. **Pipeline Stages** (`src/pipeline/stages.py`):
   - `@openlineage_job` decorator auto-tracks lineage
   - Extract → Transform → Load stages
   - Each stage emits OpenLineage events (START/COMPLETE/FAIL)
4. **OpenLineageTracker** (`src/utils/openlineage_client.py`):
   - Manages run lifecycle
   - Creates datasets with schema information
   - Publishes events to Kafka

### Data Storage Layer

1. **OTEL Collector** (Docker container):
   - Receives OTLP data from FastAPI app
   - Exports to ClickHouse for metrics and traces (Grafana dashboards)
   - Configuration via `docker/otel-collector-config.yaml`

2. **Marquez Integration**:
   - LineageConsumer forwards OpenLineage events via HTTP to Marquez API
   - Endpoint: `/api/v1/lineage`
   - Async HTTP client with retry logic
   - Separate from observability data pipeline

### Configuration & Dependencies

- **Settings** (`src/config.py`): Pydantic Settings with `.env` support
- **Logging** (`src/utils/logging_config.py`): Structured logging via structlog
- **OTEL Config** (`src/utils/otel_config.py`): Tracing and metrics setup

## Key Integration Points

### OpenLineage Events
Events are structured with:
- `eventType`: START, COMPLETE, FAIL
- `job`: Contains namespace, name, facets
- `run`: Contains runId, facets
- `inputs`/`outputs`: Dataset information with schemas

### Kafka Message Flow
- **Producer**: Uses confluent-kafka Producer with delivery callbacks
- **Consumer**: Uses confluent-kafka Consumer with auto-commit
- **Serialization**: JSON encoding with UTF-8
- **Keys**: run_id (OpenLineage), trace_id (OTEL spans), service_name (metrics)

### Consumer Batch Processing
- OTelConsumer batches messages (default 100) before ClickHouse insertion
- LineageConsumer forwards immediately to maintain real-time lineage

## Pre-commit Hooks

Configured in `.pre-commit-config.yaml`:

- **Ruff**: Linting and formatting (auto-fixes most issues)
- **MyPy**: Type checking with strict mode
- **File checks**: YAML/JSON validation, trailing whitespace
- **Poetry**: Dependency validation

Hooks run automatically on commit. Manual run: `poetry run pre-commit run --all-files`

## Common Development Tasks

### Adding New Pipeline Stage

1. Create method with `@openlineage_job` decorator in `src/pipeline/stages.py`
2. Define `JobInfo` with inputs/outputs datasets
3. Add stage to executor in `src/pipeline/executor.py`

### Modifying Kafka Topics

1. Update topic names in `src/config.py`
2. Update consumer subscriptions in consumer classes
3. Restart Docker services: `make stop && make start`

### Debugging Consumer Issues

1. Check consumer logs: `docker logs data-lineage-hub-kafka-1`
2. Verify topic exists: `docker exec -it data-lineage-hub-kafka-1 kafka-topics --list --bootstrap-server localhost:9092`
3. Check consumer group: `docker exec -it data-lineage-hub-kafka-1 kafka-consumer-groups --describe --group lineage-consumer-group --bootstrap-server localhost:9092`

## Troubleshooting

### Container Startup Issues

**Kafka Container Fails to Start**

- **Symptom**: `ClassNotFoundException: io.confluent.metrics.reporter.ConfluentMetricsReporter`
- **Solution**: This is resolved in the current configuration. If you encounter this, remove Confluent metrics reporter settings from docker-compose.yml:

  ```yaml
  # Remove these environment variables if present:
  # KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
  # KAFKA_CONFLUENT_METRICS_REPORTER_*
  ```

**Grafana Container Fails to Start**

- **Symptom**: `failed to install plugin grafana-clickhouse-datasource: tls: failed to verify certificate`
- **Solution**: The ClickHouse plugin is now installed manually after container startup:

  ```bash
  # Wait for Grafana to start, then install plugin:
  docker exec grafana grafana cli plugins install grafana-clickhouse-datasource
  docker restart grafana
  ```

- **Alternative**: Use the automated setup command:

  ```bash
  make setup-grafana-plugins  # Automated plugin installation and Grafana restart
  ```

**General Container Issues**

- Check container status: `docker ps -a`
- Check specific logs: `docker logs <container-name>`
- Restart specific service: `docker-compose restart <service-name>`
- Full restart: `make stop && make start`

### Connectivity Issues

- **Kafka not accessible**: Verify port 9092 is open and Kafka is running
- **Marquez UI not loading**: Check PostgreSQL connection and port 3000
- **Grafana login issues**: Default credentials are admin/admin
- **ClickHouse connection**: Verify port 8123 and database initialization

### Plugin Installation Issues

- **ClickHouse Datasource Plugin**: Install manually as shown above
- **Plugin compatibility**: Verify plugin version matches Grafana version
- **Network issues**: Check Docker network connectivity for plugin downloads

## Access Points

- **API Documentation**: <http://localhost:8000/docs>
- **Marquez UI**: <http://localhost:3000> (lineage visualization)
- **Grafana**: <http://localhost:3001> (admin/admin)
- **OTEL Collector**: <http://localhost:4317> (OTLP gRPC), <http://localhost:4318> (OTLP HTTP)
