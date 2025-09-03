"""
Telemetry tracking examples using @telemetry_track decorator.
Tests both sync and async function calls to ensure proper OpenTelemetry integration.
"""

import asyncio
import contextlib
import time

from src.sdk import configure, telemetry_track


# Configure the SDK
configure(
    hub_endpoint="http://localhost:8000",
    api_key="demo-api-key",
    namespace="telemetry-test-example",
    enable_telemetry=True,
    debug=True,
)


# =============================================================================
# SYNC FUNCTION TELEMETRY EXAMPLES
# =============================================================================


@telemetry_track(
    span_name="data_processing",
    service_name="data-pipeline",
    tags={"component": "processor", "version": "1.0"},
)
def process_data_sync():
    """Sync function with telemetry tracking."""
    time.sleep(0.1)  # Simulate processing time
    return {"processed_records": 1000, "status": "success"}


@telemetry_track(
    span_name="user_validation",
    service_name="auth-service",
    tags={"component": "validator", "security": "high"},
)
def validate_user_sync(user_id: str):
    """Sync user validation with telemetry."""
    time.sleep(0.05)  # Simulate validation time
    if user_id == "invalid":
        raise ValueError("Invalid user ID")
    return {"user_id": user_id, "valid": True}


@telemetry_track(
    span_name="database_query",
    service_name="db-service",
    tags={"db": "postgres", "operation": "select"},
)
def query_database_sync(query: str):
    """Sync database query with telemetry."""
    time.sleep(0.2)  # Simulate DB query time
    return {"rows": 150, "execution_time": "200ms"}


# =============================================================================
# ASYNC FUNCTION TELEMETRY EXAMPLES
# =============================================================================


@telemetry_track(
    span_name="async_data_processing",
    service_name="async-pipeline",
    tags={"component": "async_processor", "version": "2.0"},
)
async def process_data_async():
    """Async function with telemetry tracking."""
    await asyncio.sleep(0.1)  # Simulate async processing time
    return {"processed_records": 2000, "status": "success", "mode": "async"}


@telemetry_track(
    span_name="async_api_call",
    service_name="integration-service",
    tags={"component": "api_client", "endpoint": "external"},
)
async def call_external_api_async(endpoint: str):
    """Async API call with telemetry."""
    await asyncio.sleep(0.3)  # Simulate API call time
    return {"endpoint": endpoint, "response_code": 200, "data": {"result": "success"}}


@telemetry_track(
    span_name="async_validation_with_error",
    service_name="validation-service",
    tags={"component": "async_validator", "error_handling": "enabled"},
)
async def validate_with_error_async(data: str):
    """Async function that can fail - test error tracking."""
    await asyncio.sleep(0.1)
    if data == "error":
        raise RuntimeError("Async validation failed")
    return {"data": data, "valid": True, "async": True}


# =============================================================================
# TEST FUNCTIONS
# =============================================================================


def test_sync_telemetry():
    """Test all sync telemetry functions."""

    # Test successful sync functions
    process_data_sync()

    validate_user_sync("user123")

    query_database_sync("SELECT * FROM users LIMIT 100")

    # Test sync function with error
    with contextlib.suppress(ValueError):
        validate_user_sync("invalid")


async def test_async_telemetry():
    """Test all async telemetry functions."""

    # Test successful async functions
    await process_data_async()

    await call_external_api_async("https://api.example.com/data")

    await validate_with_error_async("good_data")

    # Test async function with error
    with contextlib.suppress(RuntimeError):
        await validate_with_error_async("error")


def test_mixed_sync_async():
    """Test mixing sync and async telemetry calls."""

    # Mix sync and async calls
    sync_result = process_data_sync()

    async def mixed_async_test():
        async_result = await process_data_async()

        # Another sync call after async
        sync_result2 = query_database_sync("SELECT COUNT(*) FROM orders")

        return {"sync": sync_result, "async": async_result, "sync2": sync_result2}

    asyncio.run(mixed_async_test())


def main():
    """Run all telemetry tests."""

    # Test sync telemetry
    test_sync_telemetry()

    # Test async telemetry
    asyncio.run(test_async_telemetry())

    # Test mixed scenarios
    test_mixed_sync_async()


if __name__ == "__main__":
    main()
