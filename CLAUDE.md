# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Lineage Hub POC demonstrating data pipeline observability using OpenLineage and Marquez for data lineage tracking. The system provides a centralized service architecture where teams can use the SDK to send lineage events to the central hub.

### Architecture Overview
- **OpenLineage**: Lineage events → Kafka → Marquez (data lineage visualization)
- **Centralized Service**: Teams use SDK decorators to send events to central hub
- **Multi-tenant**: Namespace-based isolation for different teams

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
make start                  # Start infrastructure (Docker services: Kafka, Marquez, ClickHouse, Grafana)
make setup-grafana-plugins  # Install required Grafana plugins (ClickHouse datasource)
make run-api                # Run FastAPI server (port 8000) - central ingestion API
make run-lineage-consumer   # Run OpenLineage event consumer (Kafka → Marquez)
# Note: Service provides central ingestion endpoints for team SDKs
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

### Unified Source Structure

After recent reorganization, all code is now unified under `src/` with clean separation:

```
src/
├── api/                    # FastAPI central ingestion endpoints
├── consumers/              # Kafka event consumers (lineage → Marquez)
├── services/               # Core business logic and namespace management
├── utils/                  # Shared utilities (Kafka, ClickHouse, logging, OTEL)
├── sdk/                    # ✅ Unified SDK (no nesting) - teams import from src.sdk
│   ├── client.py           # LineageHubClient, TelemetryClient
│   ├── decorators.py       # @lineage_track, @telemetry_track
│   ├── config.py           # SDK configuration management
│   └── models.py           # Pydantic models for lineage events
├── examples/               # ✅ SDK usage examples (moved from nested structure)
└── main.py                 # FastAPI application entry point

tests/
└── sdk/                    # ✅ SDK tests mirror src structure
    ├── test_client.py
    ├── test_config.py
    └── test_decorators.py
```

### Centralized Service Architecture

1. **Team SDK Integration**: Teams use `@lineage_track` decorators from `src.sdk`
   - Simple dict-based input/output specifications
   - Automatic namespace isolation per team
   - SDK sends events to central hub API

2. **Lineage Flow**: **OpenLineage** → **Kafka** → **Marquez**
   - Central ingestion API receives events from team SDKs
   - KafkaEventPublisher publishes lineage events to Kafka topics
   - LineageConsumer forwards events to Marquez API
   - Real-time data lineage visualization

3. **Configuration**:
   - Namespace-based multi-tenant isolation
   - Team-specific dataset views in Marquez
   - Cross-team data discovery capabilities

### SDK Integration Flow

1. **Team Integration**: Teams import `from src.sdk` directly
2. **Decorator Usage**: Add `@lineage_track` to pipeline functions
3. **Dataset Specification**: Use dict format for inputs/outputs:
   ```python
   from src.sdk import lineage_track

   @lineage_track(
       inputs=[{"type": "mysql", "name": "users.table", "format": "table", "namespace": "prod-db"}],
       outputs=[{"type": "s3", "name": "s3://bucket/output.parquet", "format": "parquet", "namespace": "processed"}]
   )
   ```
4. **Event Processing**: SDK sends events to central hub → Kafka → Marquez

### Data Storage Layer

1. **Marquez Integration**:
   - LineageConsumer forwards OpenLineage events via HTTP to Marquez API
   - Endpoint: `/api/v1/lineage`
   - Async HTTP client with retry logic
   - Multi-tenant namespace support

2. **ClickHouse Storage**:
   - Optional metrics storage for pipeline performance
   - Grafana dashboards for organizational visibility
   - Time-series data for trend analysis

### Configuration & Dependencies

- **Settings** (`src/config.py`): Pydantic Settings with `.env` support
- **Logging** (`src/utils/logging_config.py`): Structured logging via structlog
- **SDK Configuration**: Namespace and endpoint configuration for teams

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
- **Keys**: run_id for OpenLineage events, namespace-based partitioning

### Consumer Processing
- LineageConsumer forwards OpenLineage events immediately to maintain real-time lineage
- Namespace-based event routing for multi-tenant isolation

## Pre-commit Hooks

Configured in `.pre-commit-config.yaml`:

- **Ruff**: Linting and formatting (auto-fixes most issues)
- **MyPy**: Type checking with strict mode
- **File checks**: YAML/JSON validation, trailing whitespace
- **Poetry**: Dependency validation

Hooks run automatically on commit. Manual run: `poetry run pre-commit run --all-files`

## Common Development Tasks

### Using SDK in Team Repositories

1. Install SDK: `pip install data-lineage-hub-sdk`
2. Configure namespace: Set `LINEAGE_NAMESPACE="team-name"`
3. Add decorators to pipeline functions with dict-based dataset specifications
4. Events automatically flow to central hub → Kafka → Marquez

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
