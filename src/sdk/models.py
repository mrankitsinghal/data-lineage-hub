"""Data models for Data Lineage Hub SDK."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class LineageEvent(BaseModel):
    """OpenLineage event model."""

    eventType: str = Field(description="Type of event (START, COMPLETE, FAIL)")
    eventTime: str = Field(description="ISO 8601 timestamp of the event")
    run: dict[str, Any] = Field(description="Run information with runId")
    job: dict[str, Any] = Field(description="Job information with namespace and name")
    producer: str = Field(description="Producer identifier")
    inputs: list[dict[str, Any]] | None = Field(
        default=None, description="Input datasets"
    )
    outputs: list[dict[str, Any]] | None = Field(
        default=None, description="Output datasets"
    )


class TelemetryData(BaseModel):
    """OpenTelemetry data model."""

    traceId: str | None = Field(default=None, description="Trace identifier")
    spanId: str | None = Field(default=None, description="Span identifier")
    resource: dict[str, Any] = Field(description="Resource attributes")
    attributes: dict[str, Any] | None = Field(
        default=None, description="Span/metric attributes"
    )
    timestamp: str = Field(description="ISO 8601 timestamp")


class LineageIngestRequest(BaseModel):
    """Request model for lineage event ingestion."""

    namespace: str = Field(description="Target namespace for events")
    events: list[dict[str, Any]] = Field(description="List of OpenLineage events")
    source: str | None = Field(
        default="data-lineage-hub-sdk", description="Source system identifier"
    )


class LineageIngestResponse(BaseModel):
    """Response model for lineage event ingestion."""

    accepted: int = Field(description="Number of events accepted")
    rejected: int = Field(description="Number of events rejected")
    errors: list[str] = Field(description="List of error messages")
    namespace: str = Field(description="Target namespace")


class TelemetryIngestRequest(BaseModel):
    """Request model for telemetry data ingestion."""

    namespace: str = Field(description="Target namespace for telemetry")
    traces: list[dict[str, Any]] = Field(
        default_factory=list, description="List of trace spans"
    )
    metrics: list[dict[str, Any]] = Field(
        default_factory=list, description="List of metrics"
    )
    source: str | None = Field(
        default="data-lineage-hub-sdk", description="Source system identifier"
    )


class TelemetryIngestResponse(BaseModel):
    """Response model for telemetry data ingestion."""

    traces_accepted: int = Field(description="Number of traces accepted")
    traces_rejected: int = Field(description="Number of traces rejected")
    metrics_accepted: int = Field(description="Number of metrics accepted")
    metrics_rejected: int = Field(description="Number of metrics rejected")
    errors: list[str] = Field(description="List of error messages")
    namespace: str = Field(description="Target namespace")


class DatasetReference(BaseModel):
    """Reference to a dataset in lineage."""

    namespace: str = Field(description="Dataset namespace")
    name: str = Field(description="Dataset name")
    facets: dict[str, Any] | None = Field(default=None, description="Dataset facets")


class JobReference(BaseModel):
    """Reference to a job in lineage."""

    namespace: str = Field(description="Job namespace")
    name: str = Field(description="Job name")
    facets: dict[str, Any] | None = Field(default=None, description="Job facets")


class RunReference(BaseModel):
    """Reference to a run in lineage."""

    runId: str = Field(description="Unique run identifier")
    facets: dict[str, Any] | None = Field(default=None, description="Run facets")


class APIError(BaseModel):
    """API error response model."""

    detail: str = Field(description="Error message")
    status_code: int = Field(description="HTTP status code")
    error_type: str = Field(description="Type of error")


class HealthStatus(BaseModel):
    """Health check response model."""

    status: str = Field(description="Overall health status")
    timestamp: datetime = Field(description="Health check timestamp")
    service: str = Field(description="Service name")
    version: str = Field(description="Service version")
    dependencies: dict[str, str] = Field(description="Dependency health status")


class NamespaceInfo(BaseModel):
    """Namespace information model."""

    name: str = Field(description="Namespace name")
    display_name: str = Field(description="Human-readable name")
    description: str | None = Field(default=None, description="Namespace description")
    owners: list[str] = Field(description="List of owner email addresses")
    viewers: list[str] = Field(description="List of viewer email addresses")
    daily_event_quota: int = Field(description="Daily event quota limit")
    storage_retention_days: int = Field(description="Data retention period in days")
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")
    tags: dict[str, str] = Field(description="Namespace tags")


class BatchOptions(BaseModel):
    """Options for batch processing."""

    batch_size: int = Field(default=100, description="Maximum batch size")
    flush_interval: float = Field(default=5.0, description="Flush interval in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay: float = Field(
        default=1.0, description="Initial retry delay in seconds"
    )


class MetricPoint(BaseModel):
    """Single metric data point."""

    metric_name: str = Field(description="Name of the metric")
    value: float = Field(description="Metric value")
    timestamp: str = Field(description="ISO 8601 timestamp")
    attributes: dict[str, str] | None = Field(
        default=None, description="Metric attributes"
    )
    unit: str | None = Field(default=None, description="Unit of measurement")


class SpanData(BaseModel):
    """OpenTelemetry span data."""

    traceId: str = Field(description="Trace identifier")
    spanId: str = Field(description="Span identifier")
    parentSpanId: str | None = Field(default=None, description="Parent span identifier")
    name: str = Field(description="Span name")
    startTime: str = Field(description="Start time in ISO 8601 format")
    endTime: str = Field(description="End time in ISO 8601 format")
    status: dict[str, Any] = Field(description="Span status")
    attributes: dict[str, Any] | None = Field(
        default=None, description="Span attributes"
    )
    events: list[dict[str, Any]] | None = Field(default=None, description="Span events")
    links: list[dict[str, Any]] | None = Field(default=None, description="Span links")
    resource: dict[str, Any] = Field(description="Resource attributes")
