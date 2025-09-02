# Data Lineage Hub SDK

Python SDK for integrating with the Data Lineage Hub service. This SDK provides easy-to-use decorators and clients for automatic OpenLineage and OpenTelemetry data collection.

## Installation

```bash
pip install data-lineage-hub-sdk
```

## Quick Start

### Basic Usage with Decorators

```python
from data_lineage_hub_sdk import lineage_track, configure

# Configure the SDK
configure(
    hub_endpoint="http://localhost:8000",
    api_key="your-api-key",
    namespace="your-team-namespace"
)

# Track data lineage automatically
@lineage_track(
    job_name="user_data_processing",
    inputs=["/data/users.csv"],
    outputs=["/data/processed_users.parquet"]
)
def process_user_data(input_path: str, output_path: str):
    # Your data processing logic
    pass

# Function will automatically generate lineage events
process_user_data("/data/users.csv", "/data/processed_users.parquet")
```

### Manual Client Usage

```python
import asyncio
from data_lineage_hub_sdk import LineageHubClient

async def main():
    client = LineageHubClient(
        base_url="http://localhost:8000",
        api_key="your-api-key",
        namespace="your-team-namespace"
    )
    
    # Send custom lineage events
    events = [
        {
            "eventType": "START",
            "eventTime": "2024-01-01T00:00:00.000Z",
            "run": {"runId": "my-run-123"},
            "job": {"namespace": "your-namespace", "name": "my-job"},
            "inputs": [{"namespace": "file", "name": "/data/input.csv"}],
            "outputs": [{"namespace": "file", "name": "/data/output.csv"}]
        }
    ]
    
    response = await client.send_lineage_events(events)
    print(f"Sent {response.accepted} events successfully")

asyncio.run(main())
```

## Configuration

The SDK can be configured using environment variables or programmatically:

### Environment Variables

```bash
export LINEAGE_HUB_ENDPOINT="http://localhost:8000"
export LINEAGE_HUB_API_KEY="your-api-key"
export LINEAGE_HUB_NAMESPACE="your-team-namespace"
export LINEAGE_HUB_TIMEOUT=30
```

### Programmatic Configuration

```python
from data_lineage_hub_sdk import configure

configure(
    hub_endpoint="http://localhost:8000",
    api_key="your-api-key",
    namespace="your-team-namespace",
    timeout=30,
    retry_attempts=3,
    enable_telemetry=True
)
```

## Features

- **Automatic Lineage Tracking**: Use `@lineage_track` decorator for zero-code lineage collection
- **OpenTelemetry Integration**: Automatic distributed tracing and metrics collection
- **Async HTTP Client**: High-performance async HTTP client for API communication
- **Multi-tenant Support**: Namespace isolation for team-based data organization
- **Retry Logic**: Built-in retry mechanisms for reliable event delivery
- **Type Safety**: Full type hints and Pydantic models for better development experience

## API Reference

### Decorators

- `@lineage_track()`: Main decorator for automatic lineage tracking
- `@telemetry_track()`: Decorator for OpenTelemetry instrumentation only

### Clients

- `LineageHubClient`: Async HTTP client for direct API communication
- `TelemetryClient`: Client for sending OpenTelemetry data

### Configuration

- `configure()`: Global SDK configuration function
- `LineageHubConfig`: Configuration model class

## Examples

See the `/examples` directory for comprehensive usage examples including:

- ETL pipeline tracking
- ML model training lineage
- Database operation tracking
- Custom event publishing

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy data_lineage_hub_sdk/

# Linting
ruff check data_lineage_hub_sdk/
```

## License

Apache License 2.0