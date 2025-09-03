"""HTTP clients for Data Lineage Hub SDK."""

import asyncio
import builtins
import contextlib
from typing import Any

import httpx
import structlog

from .config import LineageHubConfig, get_config
from .models import (
    HealthStatus,
    LineageIngestRequest,
    LineageIngestResponse,
    NamespaceInfo,
    TelemetryIngestRequest,
    TelemetryIngestResponse,
)


logger = structlog.get_logger(__name__)


class APIError(Exception):
    """Exception raised for API errors."""

    def __init__(
        self, message: str, status_code: int = 500, response_data: dict | None = None
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response_data = response_data or {}


class LineageHubClient:
    """Async HTTP client for Data Lineage Hub API."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        namespace: str | None = None,
        config: LineageHubConfig | None = None,
    ):
        """
        Initialize the client.

        Args:
            base_url: Base URL for the API (overrides config)
            api_key: API key for authentication (overrides config)
            namespace: Default namespace (overrides config)
            config: Configuration instance (uses global if None)
        """
        self._config = config or get_config()

        # Override config with provided parameters
        self.base_url = (base_url or self._config.hub_endpoint).rstrip("/")
        self.api_key = api_key or self._config.api_key
        self.namespace = namespace or self._config.namespace
        self.timeout = self._config.timeout

        # Create async HTTP client
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=httpx.Timeout(self.timeout),
        )

        logger.debug(
            "Initialized LineageHubClient",
            base_url=self.base_url,
            namespace=self.namespace,
            has_api_key=bool(self.api_key),
        )

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()

    async def health_check(self) -> HealthStatus:
        """Check the health of the Data Lineage Hub service."""
        try:
            response = await self._client.get("/health")
            response.raise_for_status()

            data = response.json()
            return HealthStatus(**data)

        except httpx.HTTPStatusError as e:
            logger.exception("Health check failed", status_code=e.response.status_code)
            raise APIError(
                f"Health check failed: {e.response.status_code}",
                status_code=e.response.status_code,
                response_data=e.response.json() if e.response.content else {},
            )
        except Exception as e:
            logger.exception("Health check error", error=str(e))
            raise APIError(f"Health check error: {e}")

    async def send_lineage_events(
        self,
        events: list[dict[str, Any]],
        namespace: str | None = None,
        source: str | None = None,
    ) -> LineageIngestResponse:
        """
        Send OpenLineage events to the hub.

        Args:
            events: List of OpenLineage event dictionaries
            namespace: Target namespace (defaults to client namespace)
            source: Source system identifier

        Returns:
            LineageIngestResponse with ingestion results
        """
        if self._config.dry_run:
            logger.info(
                "DRY RUN: Would send lineage events",
                event_count=len(events),
                namespace=namespace or self.namespace,
            )
            return LineageIngestResponse(
                accepted=len(events),
                rejected=0,
                errors=[],
                namespace=namespace or self.namespace,
            )

        request_data = LineageIngestRequest(
            namespace=namespace or self.namespace,
            events=events,
            source=source or "data-lineage-hub-sdk",
        )

        try:
            response = await self._client.post(
                "/api/v1/lineage/ingest",
                json={"lineage_data": request_data.model_dump()},
            )
            response.raise_for_status()

            data = response.json()
            result = LineageIngestResponse(**data)

            logger.info(
                "Successfully sent lineage events",
                accepted=result.accepted,
                rejected=result.rejected,
                namespace=result.namespace,
            )

            if result.errors:
                logger.warning("Some events were rejected", errors=result.errors[:5])

            return result

        except httpx.HTTPStatusError as e:
            error_data = {}
            with contextlib.suppress(builtins.BaseException):
                error_data = e.response.json()

            logger.exception(
                "Failed to send lineage events",
                status_code=e.response.status_code,
                error_data=error_data,
                event_count=len(events),
            )

            raise APIError(
                f"Failed to send lineage events: {e.response.status_code}",
                status_code=e.response.status_code,
                response_data=error_data,
            )
        except Exception as e:
            logger.exception(
                "Error sending lineage events",
                error=str(e),
                event_count=len(events),
            )
            raise APIError(f"Error sending lineage events: {e}")

    async def send_telemetry_data(
        self,
        traces: list[dict[str, Any]] | None = None,
        metrics: list[dict[str, Any]] | None = None,
        namespace: str | None = None,
        source: str | None = None,
    ) -> TelemetryIngestResponse:
        """
        Send OpenTelemetry data to the hub.

        Args:
            traces: List of trace span dictionaries
            metrics: List of metric dictionaries
            namespace: Target namespace (defaults to client namespace)
            source: Source system identifier

        Returns:
            TelemetryIngestResponse with ingestion results
        """
        traces = traces or []
        metrics = metrics or []

        if self._config.dry_run:
            logger.info(
                "DRY RUN: Would send telemetry data",
                trace_count=len(traces),
                metric_count=len(metrics),
                namespace=namespace or self.namespace,
            )
            return TelemetryIngestResponse(
                traces_accepted=len(traces),
                traces_rejected=0,
                metrics_accepted=len(metrics),
                metrics_rejected=0,
                errors=[],
                namespace=namespace or self.namespace,
            )

        request_data = TelemetryIngestRequest(
            namespace=namespace or self.namespace,
            traces=traces,
            metrics=metrics,
            source=source or "data-lineage-hub-sdk",
        )

        try:
            response = await self._client.post(
                "/api/v1/telemetry/ingest",
                json={"telemetry_request": request_data.model_dump()},
            )
            response.raise_for_status()

            data = response.json()
            result = TelemetryIngestResponse(**data)

            logger.info(
                "Successfully sent telemetry data",
                traces_accepted=result.traces_accepted,
                metrics_accepted=result.metrics_accepted,
                namespace=result.namespace,
            )

            if result.errors:
                logger.warning(
                    "Some telemetry data was rejected", errors=result.errors[:5]
                )

            return result

        except httpx.HTTPStatusError as e:
            error_data = {}
            with contextlib.suppress(builtins.BaseException):
                error_data = e.response.json()

            logger.exception(
                "Failed to send telemetry data",
                status_code=e.response.status_code,
                error_data=error_data,
                trace_count=len(traces),
                metric_count=len(metrics),
            )

            raise APIError(
                f"Failed to send telemetry data: {e.response.status_code}",
                status_code=e.response.status_code,
                response_data=error_data,
            )
        except Exception as e:
            logger.exception(
                "Error sending telemetry data",
                error=str(e),
                trace_count=len(traces),
                metric_count=len(metrics),
            )
            raise APIError(f"Error sending telemetry data: {e}")

    async def get_namespace(self, namespace_name: str) -> NamespaceInfo:
        """Get namespace information by name."""
        try:
            response = await self._client.get(f"/api/v1/namespaces/{namespace_name}")
            response.raise_for_status()

            data = response.json()
            return NamespaceInfo(**data)

        except httpx.HTTPStatusError as e:
            error_data = {}
            with contextlib.suppress(builtins.BaseException):
                error_data = e.response.json()

            logger.exception(
                "Failed to get namespace",
                namespace=namespace_name,
                status_code=e.response.status_code,
            )

            raise APIError(
                f"Failed to get namespace '{namespace_name}': {e.response.status_code}",
                status_code=e.response.status_code,
                response_data=error_data,
            )
        except Exception as e:
            logger.exception("Error getting namespace", error=str(e))
            raise APIError(f"Error getting namespace: {e}")

    async def list_namespaces(self) -> list[NamespaceInfo]:
        """List accessible namespaces."""
        try:
            response = await self._client.get("/api/v1/namespaces")
            response.raise_for_status()

            data = response.json()
            return [NamespaceInfo(**ns) for ns in data.get("namespaces", [])]

        except httpx.HTTPStatusError as e:
            error_data = {}
            with contextlib.suppress(builtins.BaseException):
                error_data = e.response.json()

            logger.exception(
                "Failed to list namespaces",
                status_code=e.response.status_code,
            )

            raise APIError(
                f"Failed to list namespaces: {e.response.status_code}",
                status_code=e.response.status_code,
                response_data=error_data,
            )
        except Exception as e:
            logger.exception("Error listing namespaces", error=str(e))
            raise APIError(f"Error listing namespaces: {e}")


class TelemetryClient:
    """Specialized client for OpenTelemetry data."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        namespace: str | None = None,
        config: LineageHubConfig | None = None,
    ):
        """Initialize the telemetry client."""
        self._lineage_client = LineageHubClient(base_url, api_key, namespace, config)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    async def close(self):
        """Close the client."""
        await self._lineage_client.close()

    async def send_traces(
        self,
        traces: list[dict[str, Any]],
        namespace: str | None = None,
    ) -> TelemetryIngestResponse:
        """Send OpenTelemetry traces."""
        return await self._lineage_client.send_telemetry_data(
            traces=traces, namespace=namespace
        )

    async def send_metrics(
        self,
        metrics: list[dict[str, Any]],
        namespace: str | None = None,
    ) -> TelemetryIngestResponse:
        """Send OpenTelemetry metrics."""
        return await self._lineage_client.send_telemetry_data(
            metrics=metrics, namespace=namespace
        )

    async def send_span(
        self,
        span_data: dict[str, Any],
        namespace: str | None = None,
    ) -> TelemetryIngestResponse:
        """Send a single OpenTelemetry span."""
        return await self.send_traces([span_data], namespace)

    async def send_metric(
        self,
        metric_data: dict[str, Any],
        namespace: str | None = None,
    ) -> TelemetryIngestResponse:
        """Send a single OpenTelemetry metric."""
        return await self.send_metrics([metric_data], namespace)


class BatchingLineageClient:
    """Lineage client with automatic batching and background sending."""

    def __init__(
        self,
        base_client: LineageHubClient | None = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        auto_start: bool = True,
    ):
        """
        Initialize batching client.

        Args:
            base_client: Underlying LineageHubClient (creates new if None)
            batch_size: Maximum events per batch
            flush_interval: Seconds between automatic flushes
            auto_start: Whether to start background flushing automatically
        """
        self._client = base_client or LineageHubClient()
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self._event_buffer: list[dict[str, Any]] = []
        self._flush_task: asyncio.Task | None = None
        self._closed = False

        if auto_start:
            self.start()

    def start(self):
        """Start background flushing task."""
        if not self._flush_task or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self):
        """Stop background flushing and flush remaining events."""
        self._closed = True
        if self._flush_task:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task

        # Flush any remaining events
        if self._event_buffer:
            await self.flush()

        await self._client.close()

    async def add_event(self, event: dict[str, Any]):
        """Add an event to the batch buffer."""
        if self._closed:
            logger.warning("Client is closed, ignoring event")
            return

        self._event_buffer.append(event)

        # Flush if batch is full
        if len(self._event_buffer) >= self.batch_size:
            await self.flush()

    async def flush(self):
        """Flush all buffered events."""
        if not self._event_buffer:
            return

        events_to_send = self._event_buffer.copy()
        self._event_buffer.clear()

        try:
            await self._client.send_lineage_events(events_to_send)
            logger.debug(f"Flushed {len(events_to_send)} events")
        except Exception as e:
            logger.exception(
                "Error flushing events", error=str(e), event_count=len(events_to_send)
            )
            # Re-add events to buffer for retry (simple strategy)
            self._event_buffer.extend(events_to_send)

    async def _flush_loop(self):
        """Background task that flushes events periodically."""
        try:
            while not self._closed:
                await asyncio.sleep(self.flush_interval)
                if self._event_buffer:
                    await self.flush()
        except asyncio.CancelledError:
            logger.debug("Flush loop cancelled")
        except Exception as e:
            logger.exception("Error in flush loop", error=str(e))
