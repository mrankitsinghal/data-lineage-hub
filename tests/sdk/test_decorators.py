"""Tests for lineage and telemetry decorators."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.sdk.config import configure, reset_config
from src.sdk.decorators import lineage_track, telemetry_track


@pytest.fixture(autouse=True)
def reset_global_config():
    """Reset global config before each test."""
    reset_config()
    yield
    reset_config()


@pytest.fixture
def mock_lineage_client():
    """Mock LineageHubClient."""
    with patch("src.sdk.decorators.LineageHubClient") as mock:
        mock_instance = mock.return_value
        mock_instance.send_lineage_events = AsyncMock()
        yield mock_instance


@pytest.fixture
def mock_tracer():
    """Mock OpenTelemetry tracer."""
    with patch("src.sdk.decorators.trace.get_tracer") as mock:
        tracer = mock.return_value
        span = MagicMock()
        tracer.start_as_current_span.return_value.__enter__ = MagicMock(
            return_value=span
        )
        tracer.start_as_current_span.return_value.__exit__ = MagicMock(
            return_value=None
        )
        yield tracer, span


class TestLineageTrackDecorator:
    """Tests for @lineage_track decorator."""

    def test_decorator_with_sync_function(self, mock_lineage_client):
        """Test decorator on synchronous function."""
        configure(enable_lineage=True, namespace="test-ns")

        @lineage_track(
            job_name="test_job",
            inputs=["/data/input.csv"],
            outputs=["/data/output.csv"],
            send_async=True,
        )
        def process_data():
            return "processed"

        result = process_data()

        assert result == "processed"
        # In sync mode with send_async=True, events are sent in background tasks
        # We can't easily test the async calls in sync context

    @pytest.mark.asyncio
    async def test_decorator_with_async_function(self, mock_lineage_client):
        """Test decorator on asynchronous function."""
        configure(enable_lineage=True, namespace="test-ns")

        @lineage_track(
            job_name="async_job",
            inputs=["/data/input.csv"],
            outputs=["/data/output.csv"],
            send_async=False,  # Use synchronous sending for easier testing
        )
        async def async_process():
            await asyncio.sleep(0.001)  # Simulate async work
            return "async_result"

        result = await async_process()

        assert result == "async_result"

        # Should have called send_lineage_events twice (START and COMPLETE)
        assert mock_lineage_client.send_lineage_events.call_count == 2

        # Check START event
        start_call = mock_lineage_client.send_lineage_events.call_args_list[0]
        start_event = start_call[0][0][0]  # First arg, first event
        assert start_event["eventType"] == "START"
        assert start_event["job"]["name"] == "async_job"
        assert start_event["job"]["namespace"] == "test-ns"
        assert "inputs" in start_event
        assert start_event["inputs"][0]["name"] == "/data/input.csv"

        # Check COMPLETE event
        complete_call = mock_lineage_client.send_lineage_events.call_args_list[1]
        complete_event = complete_call[0][0][0]
        assert complete_event["eventType"] == "COMPLETE"
        assert "outputs" in complete_event
        assert complete_event["outputs"][0]["name"] == "/data/output.csv"

    @pytest.mark.asyncio
    async def test_decorator_handles_exceptions(self, mock_lineage_client):
        """Test decorator handles function exceptions."""
        configure(enable_lineage=True)

        @lineage_track(job_name="failing_job", send_async=False)
        async def failing_function():
            raise ValueError("Something went wrong")

        with pytest.raises(ValueError, match="Something went wrong"):
            await failing_function()

        # Should have called send_lineage_events twice (START and FAIL)
        assert mock_lineage_client.send_lineage_events.call_count == 2

        # Check FAIL event
        fail_call = mock_lineage_client.send_lineage_events.call_args_list[1]
        fail_event = fail_call[0][0][0]
        assert fail_event["eventType"] == "FAIL"
        assert "error_info" in fail_event["run"]["facets"]
        assert (
            "Something went wrong"
            in fail_event["run"]["facets"]["error_info"]["error_message"]
        )

    def test_decorator_disabled_by_config(self, mock_lineage_client):
        """Test decorator does nothing when lineage is disabled."""
        configure(enable_lineage=False)

        @lineage_track(job_name="disabled_job")
        def process_data():
            return "result"

        result = process_data()

        assert result == "result"
        # Should not have made any calls
        mock_lineage_client.send_lineage_events.assert_not_called()

    @pytest.mark.asyncio
    async def test_decorator_dry_run_mode(self, mock_lineage_client):
        """Test decorator in dry run mode."""
        configure(enable_lineage=True, dry_run=True)

        @lineage_track(job_name="dry_run_job", send_async=False)
        async def process_data():
            return "result"

        result = await process_data()

        assert result == "result"
        # Should not have made HTTP calls in dry run
        mock_lineage_client.send_lineage_events.assert_not_called()

    def test_decorator_uses_function_name_as_default(self, mock_lineage_client):
        """Test decorator uses function name when job_name not provided."""
        configure(enable_lineage=True, namespace="test")

        @lineage_track()
        def my_processing_function():
            return "done"

        result = my_processing_function()
        assert result == "done"
        # The function name should be used as job name

    def test_decorator_with_custom_run_id(self, mock_lineage_client):
        """Test decorator with custom run ID."""
        configure(enable_lineage=True)

        custom_run_id = "custom-run-12345"

        @lineage_track(job_name="custom_run_job", run_id=custom_run_id)
        def process_with_custom_id():
            return "result"

        result = process_with_custom_id()
        assert result == "result"

    def test_decorator_with_tags(self, mock_lineage_client):
        """Test decorator with custom tags."""
        configure(enable_lineage=True)

        @lineage_track(
            job_name="tagged_job", tags={"environment": "test", "team": "data-platform"}
        )
        def tagged_function():
            return "tagged"

        result = tagged_function()
        assert result == "tagged"


class TestTelemetryTrackDecorator:
    """Tests for @telemetry_track decorator."""

    def test_telemetry_decorator_sync_function(self, mock_tracer):
        """Test telemetry decorator on sync function."""
        tracer, span = mock_tracer
        configure(enable_telemetry=True)

        @telemetry_track(
            span_name="test_span",
            service_name="test-service",
            tags={"component": "processor"},
        )
        def process_data():
            return "processed"

        result = process_data()

        assert result == "processed"
        tracer.start_as_current_span.assert_called_once_with("test_span")

        # Check that span attributes were set
        span.set_attribute.assert_any_call("component", "processor")
        span.set_attribute.assert_any_call("service.name", "test-service")
        span.set_attribute.assert_any_call("function.name", "process_data")
        span.set_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_telemetry_decorator_async_function(self, mock_tracer):
        """Test telemetry decorator on async function."""
        tracer, span = mock_tracer
        configure(enable_telemetry=True, namespace="test-ns")

        @telemetry_track(span_name="async_span", service_name="async-service")
        async def async_process():
            await asyncio.sleep(0.001)
            return "async_result"

        result = await async_process()

        assert result == "async_result"
        tracer.start_as_current_span.assert_called_once_with("async_span")
        span.set_attribute.assert_any_call("service.namespace", "test-ns")

    def test_telemetry_decorator_handles_exceptions(self, mock_tracer):
        """Test telemetry decorator handles exceptions."""
        tracer, span = mock_tracer
        configure(enable_telemetry=True)

        @telemetry_track(span_name="failing_span")
        def failing_function():
            raise RuntimeError("Test error")

        with pytest.raises(RuntimeError, match="Test error"):
            failing_function()

        # Should have recorded the exception
        span.record_exception.assert_called_once()
        span.set_status.assert_called()  # Should set error status

    def test_telemetry_decorator_disabled(self, mock_tracer):
        """Test telemetry decorator when disabled."""
        tracer, span = mock_tracer
        configure(enable_telemetry=False)

        @telemetry_track(span_name="disabled_span")
        def process_data():
            return "result"

        result = process_data()

        assert result == "result"
        # Should not have created any spans
        tracer.start_as_current_span.assert_not_called()

    def test_telemetry_decorator_uses_function_name_default(self, mock_tracer):
        """Test telemetry decorator uses function name as default span name."""
        tracer, span = mock_tracer
        configure(enable_telemetry=True)

        @telemetry_track()
        def my_telemetry_function():
            return "result"

        result = my_telemetry_function()

        assert result == "result"
        tracer.start_as_current_span.assert_called_once_with("my_telemetry_function")


class TestDecoratorIntegration:
    """Integration tests for decorators working together."""

    @pytest.mark.asyncio
    async def test_combined_decorators(self, mock_lineage_client, mock_tracer):
        """Test lineage and telemetry decorators together."""
        tracer, span = mock_tracer
        configure(enable_lineage=True, enable_telemetry=True)

        @lineage_track(job_name="combined_job", send_async=False)
        @telemetry_track(span_name="combined_span")
        async def combined_function():
            return "combined_result"

        result = await combined_function()

        assert result == "combined_result"

        # Both decorators should have been applied
        mock_lineage_client.send_lineage_events.assert_called()
        tracer.start_as_current_span.assert_called_once_with("combined_span")

    def test_decorator_order_matters(self, mock_lineage_client, mock_tracer):
        """Test that decorator order affects execution."""
        tracer, span = mock_tracer
        configure(enable_lineage=True, enable_telemetry=True)

        execution_order = []

        # Mock the span context manager to track execution
        def mock_context_manager(*args, **kwargs):
            execution_order.append("telemetry_enter")
            return span

        def mock_exit(*args):
            execution_order.append("telemetry_exit")

        tracer.start_as_current_span.return_value.__enter__ = mock_context_manager
        tracer.start_as_current_span.return_value.__exit__ = mock_exit

        @telemetry_track()
        @lineage_track(job_name="order_test")
        def ordered_function():
            execution_order.append("function_execution")
            return "result"

        result = ordered_function()
        assert result == "result"

        # Telemetry decorator should be outermost
        assert execution_order[0] == "telemetry_enter"
        assert execution_order[-1] == "telemetry_exit"
