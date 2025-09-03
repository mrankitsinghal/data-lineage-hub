# Data Lineage Hub - Suggested Commands

## Essential Setup Commands

### First Time Setup (Recommended)

```bash
make dev-setup              # Complete development setup (dependencies + infrastructure + services)
```

**What it does**: Creates virtual environment, installs dependencies, starts Docker services, configures Grafana plugins

### Manual Setup Steps

```bash
make install-dev            # Install all dependencies including dev tools
make start                  # Start infrastructure (Docker services: Kafka, Marquez, ClickHouse, Grafana)
make setup-grafana-plugins  # Install required Grafana plugins (ClickHouse datasource)
```

### Environment Management

```bash
make venv                   # Create Poetry virtual environment
make venv-info              # Show virtual environment information
make venv-recreate          # Recreate virtual environment from scratch
make venv-remove            # Remove Poetry virtual environment
poetry shell               # Activate Poetry environment
```

## Running Services

### Core Application Services

```bash
make run-api                # Run FastAPI server (port 8000) - central ingestion API
make run-lineage-consumer   # Run OpenLineage event consumer (Kafka → Marquez)
make run-otel-consumer      # Run OpenTelemetry consumer (Kafka → ClickHouse)
```

### Infrastructure Services

```bash
make start                  # Start all Docker services (Kafka, Marquez, ClickHouse, Grafana)
make stop                   # Stop all Docker services
make clean                  # Clean up containers and volumes
```

## Code Quality (MUST RUN BEFORE COMMITTING)

### Quality Checks

```bash
make check                  # Run all quality checks (Ruff + MyPy) - run this first!
make format                 # Format code with Ruff
make fix                   # Auto-fix linting issues with Ruff
make lint                  # Lint code with Ruff
```

### Testing

```bash
make test                  # Run tests with pytest
poetry run pytest tests/ -v                    # Run all tests with verbose output
poetry run pytest --cov=src --cov-report=html  # Run tests with coverage report
poetry run pytest -m unit                      # Run only unit tests
poetry run pytest -m integration               # Run only integration tests
```

### Pre-commit Hooks

```bash
poetry run pre-commit run --all-files  # Run pre-commit hooks manually
poetry run pre-commit install          # Install pre-commit hooks
```

## Debugging & Monitoring

### Service Health & Status

```bash
make status                 # Check all service health (API, Marquez, Docker services)
make logs                   # View all Docker service logs
docker logs -f data-lineage-hub-kafka-1  # Follow specific service logs
```

### Individual Service Logs

```bash
docker logs -f data-lineage-hub-kafka-1        # Kafka logs
docker logs -f data-lineage-hub-marquez-1      # Marquez logs
docker logs -f data-lineage-hub-clickhouse-1   # ClickHouse logs
docker logs -f data-lineage-hub-grafana-1      # Grafana logs
```

### Container Management

```bash
docker ps -a                           # Check all container statuses
docker-compose ps                       # Check compose service statuses
docker-compose restart <service-name>   # Restart specific service
docker exec -it <container-name> bash   # Enter container shell
```

## Testing & Development

### API Testing

```bash
make test-lineage          # Test the central lineage ingestion API
curl -X POST http://localhost:8000/api/v1/lineage/ingest \
  -H 'Content-Type: application/json' \
  -d '{"namespace": "demo-team", "events": [{"eventType": "START", "job": {"name": "test_job", "namespace": "demo-team"}}]}'
```

### Direct Poetry Commands

```bash
poetry install                         # Install dependencies
poetry install --with dev              # Install with dev dependencies
poetry run python -m src.main          # Run FastAPI app directly
poetry run python -m src.consumers.lineage_consumer  # Run lineage consumer directly
poetry run python -m src.consumers.otel_consumer     # Run OTEL consumer directly
```

## Kafka Management & Debugging

### Kafka Topic Operations

```bash
# List all topics
docker exec -it data-lineage-hub-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Create topic
docker exec -it data-lineage-hub-kafka-1 kafka-topics --create --topic test-topic --bootstrap-server localhost:9092

# Describe topic
docker exec -it data-lineage-hub-kafka-1 kafka-topics --describe --topic openlineage-events --bootstrap-server localhost:9092

# Delete topic
docker exec -it data-lineage-hub-kafka-1 kafka-topics --delete --topic test-topic --bootstrap-server localhost:9092
```

### Kafka Consumer Groups

```bash
# List consumer groups
docker exec -it data-lineage-hub-kafka-1 kafka-consumer-groups --list --bootstrap-server localhost:9092

# Describe consumer group
docker exec -it data-lineage-hub-kafka-1 kafka-consumer-groups --describe --group lineage-consumer-group --bootstrap-server localhost:9092

# Reset consumer group offset
docker exec -it data-lineage-hub-kafka-1 kafka-consumer-groups --reset-offsets --to-earliest --group lineage-consumer-group --topic openlineage-events --bootstrap-server localhost:9092 --execute
```

### Kafka Message Monitoring

```bash
# Consume messages from topic
docker exec -it data-lineage-hub-kafka-1 kafka-console-consumer --topic openlineage-events --from-beginning --bootstrap-server localhost:9092

# Produce test message
docker exec -it data-lineage-hub-kafka-1 kafka-console-producer --topic openlineage-events --bootstrap-server localhost:9092
```

## Database Operations

### ClickHouse Operations

```bash
# Connect to ClickHouse CLI
docker exec -it data-lineage-hub-clickhouse-1 clickhouse-client

# Run query from command line
docker exec -it data-lineage-hub-clickhouse-1 clickhouse-client --query "SHOW TABLES"
docker exec -it data-lineage-hub-clickhouse-1 clickhouse-client --query "SELECT * FROM otel.spans LIMIT 10"
```

### PostgreSQL (Marquez) Operations

```bash
# Connect to PostgreSQL CLI
docker exec -it data-lineage-hub-postgres-1 psql -U marquez -d marquez

# Check Marquez tables
docker exec -it data-lineage-hub-postgres-1 psql -U marquez -d marquez -c "\dt"
```

## Access Points & URLs

### Service Access

- **API Documentation**: <http://localhost:8000/docs>
- **API Health Check**: <http://localhost:8000/api/v1/health>
- **Marquez UI**: <http://localhost:3000> (data lineage visualization)
- **Grafana**: <http://localhost:3001> (admin/admin - metrics dashboards)

### API Endpoints

```bash
# Health check
curl http://localhost:8000/api/v1/health

# List namespaces
curl http://localhost:8000/api/v1/namespaces

# Marquez API
curl http://localhost:5000/api/v1/namespaces
```

## Performance & Optimization

### Resource Monitoring

```bash
# Container resource usage
docker stats

# Container logs with timestamps
docker logs -t data-lineage-hub-kafka-1

# System resource usage
htop                            # If installed
docker system df                # Docker space usage
docker system prune             # Clean up unused Docker resources
```

### Profiling & Debugging

```bash
# Run with debugging enabled
DEBUG=true make run-api

# Python memory profiling (if memory_profiler is installed)
poetry run python -m memory_profiler src/main.py

# Performance testing
poetry run python -m pytest tests/ --benchmark-only  # If pytest-benchmark is installed
```

## Troubleshooting Commands

### Common Issues

```bash
# Port already in use
lsof -i :8000                   # Check what's using port 8000
kill -9 <PID>                   # Kill process using port

# Docker issues
docker system prune -a          # Clean all Docker resources (careful!)
docker-compose down -v          # Stop and remove volumes
docker-compose up --force-recreate  # Force recreate containers

# Poetry issues
poetry cache clear --all        # Clear Poetry cache
poetry env remove --all         # Remove all virtual environments
rm -rf .venv                    # Remove local venv (if needed)
```

### Service Connectivity Testing

```bash
# Test Kafka connectivity
telnet localhost 9092

# Test API connectivity
curl -I http://localhost:8000

# Test Marquez connectivity
curl -I http://localhost:3000

# Test ClickHouse connectivity
curl http://localhost:8123/ping
```

## Helpful Aliases (Add to ~/.bashrc or ~/.zshrc)

```bash
alias dlh-start="make start"
alias dlh-stop="make stop"
alias dlh-api="make run-api"
alias dlh-consumer="make run-lineage-consumer"
alias dlh-check="make check"
alias dlh-test="make test"
alias dlh-status="make status"
alias dlh-logs="make logs"
```
