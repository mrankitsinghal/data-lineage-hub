"""Tests for HTTP clients."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.sdk.client import (
    APIError,
    BatchingLineageClient,
    LineageHubClient,
    TelemetryClient,
)
from src.sdk.config import configure, reset_config


@pytest.fixture(autouse=True)
def reset_global_config():
    """Reset global config before each test."""
    reset_config()
    yield
    reset_config()


@pytest.fixture
def mock_httpx_client():
    """Mock httpx AsyncClient."""
    with patch("src.sdk.client.httpx.AsyncClient") as mock:
        yield mock


@pytest.fixture
def lineage_client():
    """Create a LineageHubClient for testing."""
    configure(
        hub_endpoint="https://test-hub.com",
        api_key="test-api-key",
        namespace="test-namespace",
    )
    return LineageHubClient()


class TestLineageHubClient:
    """Tests for LineageHubClient."""

    def test_client_initialization(self, mock_httpx_client):
        """Test client initialization."""
        client = LineageHubClient(
            base_url="https://custom-hub.com",
            api_key="custom-key",
            namespace="custom-ns",
        )

        assert client.base_url == "https://custom-hub.com"
        assert client.api_key == "custom-key"
        assert client.namespace == "custom-ns"

        # Verify httpx client was created with correct parameters
        mock_httpx_client.assert_called_once()
        call_args = mock_httpx_client.call_args
        assert call_args.kwargs["base_url"] == "https://custom-hub.com"
        assert "Authorization" in call_args.kwargs["headers"]
        assert call_args.kwargs["headers"]["Authorization"] == "Bearer custom-key"

    def test_client_initialization_from_config(self, mock_httpx_client):
        """Test client initialization using global config."""
        configure(
            hub_endpoint="https://config-hub.com",
            api_key="config-key",
            namespace="config-ns",
        )

        client = LineageHubClient()

        assert client.base_url == "https://config-hub.com"
        assert client.api_key == "config-key"
        assert client.namespace == "config-ns"

    @pytest.mark.asyncio
    async def test_health_check_success(self, lineage_client, mock_httpx_client):
        """Test successful health check."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "status": "healthy",
            "timestamp": "2024-01-01T00:00:00Z",
            "service": "data-lineage-hub",
            "version": "1.0.0",
            "dependencies": {"kafka": "healthy"},
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.get = AsyncMock(return_value=mock_response)

        health = await lineage_client.health_check()

        assert health.status == "healthy"
        assert health.service == "data-lineage-hub"
        mock_instance.get.assert_called_once_with("/health")

    @pytest.mark.asyncio
    async def test_health_check_error(self, lineage_client, mock_httpx_client):
        """Test health check with HTTP error."""
        mock_response = AsyncMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"detail": "Internal server error"}

        mock_instance = mock_httpx_client.return_value
        mock_instance.get = AsyncMock()
        mock_instance.get.return_value.raise_for_status.side_effect = (
            httpx.HTTPStatusError(
                "500 Server Error", request=None, response=mock_response
            )
        )

        with pytest.raises(APIError) as exc_info:
            await lineage_client.health_check()

        assert exc_info.value.status_code == 500
        assert "Health check failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_send_lineage_events_success(self, lineage_client, mock_httpx_client):
        """Test successful lineage event sending."""
        events = [
            {
                "eventType": "START",
                "eventTime": "2024-01-01T00:00:00Z",
                "run": {"runId": "test-run-123"},
                "job": {"namespace": "test", "name": "test-job"},
            }
        ]

        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "accepted": 1,
            "rejected": 0,
            "errors": [],
            "namespace": "test-namespace",
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock(return_value=mock_response)

        result = await lineage_client.send_lineage_events(events)

        assert result.accepted == 1
        assert result.rejected == 0
        assert result.namespace == "test-namespace"

        # Verify the request
        mock_instance.post.assert_called_once()
        call_args = mock_instance.post.call_args
        assert call_args.args[0] == "/api/v1/lineage/ingest"

        # Verify request body
        request_data = call_args.kwargs["json"]
        assert request_data["namespace"] == "test-namespace"
        assert request_data["events"] == events
        assert request_data["source"] == "data-lineage-hub-sdk"

    @pytest.mark.asyncio
    async def test_send_lineage_events_dry_run(self, mock_httpx_client):
        """Test lineage event sending in dry run mode."""
        configure(dry_run=True)
        client = LineageHubClient()

        events = [{"eventType": "START", "job": {"name": "test"}}]
        result = await client.send_lineage_events(events)

        assert result.accepted == 1
        assert result.rejected == 0

        # Verify no HTTP requests were made
        mock_httpx_client.return_value.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_telemetry_data_success(self, lineage_client, mock_httpx_client):
        """Test successful telemetry data sending."""
        traces = [{"traceId": "123", "spanId": "456"}]
        metrics = [{"name": "test_metric", "value": 1.0}]

        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "traces_accepted": 1,
            "traces_rejected": 0,
            "metrics_accepted": 1,
            "metrics_rejected": 0,
            "errors": [],
            "namespace": "test-namespace",
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock(return_value=mock_response)

        result = await lineage_client.send_telemetry_data(
            traces=traces, metrics=metrics
        )

        assert result.traces_accepted == 1
        assert result.metrics_accepted == 1
        assert result.namespace == "test-namespace"

        # Verify the request
        call_args = mock_instance.post.call_args
        assert call_args.args[0] == "/api/v1/telemetry/ingest"

        request_data = call_args.kwargs["json"]
        assert request_data["traces"] == traces
        assert request_data["metrics"] == metrics

    @pytest.mark.asyncio
    async def test_api_error_handling(self, lineage_client, mock_httpx_client):
        """Test API error handling."""
        mock_response = AsyncMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"detail": "Bad request"}

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock()
        mock_instance.post.return_value.raise_for_status.side_effect = (
            httpx.HTTPStatusError(
                "400 Bad Request", request=None, response=mock_response
            )
        )

        with pytest.raises(APIError) as exc_info:
            await lineage_client.send_lineage_events([{"test": "event"}])

        assert exc_info.value.status_code == 400
        assert "Failed to send lineage events" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_httpx_client):
        """Test client as async context manager."""
        mock_instance = mock_httpx_client.return_value
        mock_instance.aclose = AsyncMock()

        async with LineageHubClient() as client:
            assert isinstance(client, LineageHubClient)

        mock_instance.aclose.assert_called_once()


class TestTelemetryClient:
    """Tests for TelemetryClient."""

    @pytest.mark.asyncio
    async def test_send_traces(self, mock_httpx_client):
        """Test sending traces through telemetry client."""
        traces = [{"traceId": "123", "spanId": "456"}]

        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "traces_accepted": 1,
            "traces_rejected": 0,
            "metrics_accepted": 0,
            "metrics_rejected": 0,
            "errors": [],
            "namespace": "test",
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock(return_value=mock_response)

        client = TelemetryClient(namespace="test")
        result = await client.send_traces(traces)

        assert result.traces_accepted == 1

        # Verify only traces were sent
        call_args = mock_instance.post.call_args
        request_data = call_args.kwargs["json"]
        assert request_data["traces"] == traces
        assert request_data["metrics"] == []

    @pytest.mark.asyncio
    async def test_send_single_span(self, mock_httpx_client):
        """Test sending a single span."""
        span = {"traceId": "123", "spanId": "456"}

        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "traces_accepted": 1,
            "traces_rejected": 0,
            "metrics_accepted": 0,
            "metrics_rejected": 0,
            "errors": [],
            "namespace": "test",
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock(return_value=mock_response)

        client = TelemetryClient()
        result = await client.send_span(span)

        assert result.traces_accepted == 1

        # Verify span was wrapped in list
        call_args = mock_instance.post.call_args
        request_data = call_args.kwargs["json"]
        assert request_data["traces"] == [span]


class TestBatchingLineageClient:
    """Tests for BatchingLineageClient."""

    @pytest.mark.asyncio
    async def test_batching_by_size(self, mock_httpx_client):
        """Test automatic batching when size limit is reached."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "accepted": 3,
            "rejected": 0,
            "errors": [],
            "namespace": "test",
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock(return_value=mock_response)

        # Create client with small batch size
        client = BatchingLineageClient(batch_size=3, auto_start=False)

        # Add events one by one
        for i in range(3):
            await client.add_event({"eventType": "START", "id": i})

        # Should have triggered flush at 3 events
        mock_instance.post.assert_called_once()

        await client.stop()

    @pytest.mark.asyncio
    async def test_manual_flush(self, mock_httpx_client):
        """Test manual flushing."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "accepted": 2,
            "rejected": 0,
            "errors": [],
            "namespace": "test",
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock(return_value=mock_response)

        client = BatchingLineageClient(batch_size=10, auto_start=False)

        # Add events but don't reach batch size
        await client.add_event({"eventType": "START", "id": 1})
        await client.add_event({"eventType": "COMPLETE", "id": 2})

        # No flush yet
        mock_instance.post.assert_not_called()

        # Manual flush
        await client.flush()

        # Now should have flushed
        mock_instance.post.assert_called_once()

        await client.stop()

    @pytest.mark.asyncio
    async def test_stop_flushes_remaining(self, mock_httpx_client):
        """Test that stopping the client flushes remaining events."""
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "accepted": 1,
            "rejected": 0,
            "errors": [],
            "namespace": "test",
        }
        mock_response.raise_for_status.return_value = None

        mock_instance = mock_httpx_client.return_value
        mock_instance.post = AsyncMock(return_value=mock_response)

        client = BatchingLineageClient(batch_size=10, auto_start=False)

        # Add one event
        await client.add_event({"eventType": "START"})

        # Stop should flush the remaining event
        await client.stop()

        mock_instance.post.assert_called_once()
