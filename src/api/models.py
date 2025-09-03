"""API models and schemas."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class PipelineRunRequest(BaseModel):
    """Request model for pipeline execution."""

    pipeline_name: str = Field(..., description="Name of the pipeline to run")
    input_path: str = Field(..., description="Path to input data")
    output_path: str = Field(..., description="Path for output data")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Pipeline parameters"
    )


class PipelineRunResponse(BaseModel):
    """Response model for pipeline execution."""

    run_id: str = Field(..., description="Unique run identifier")
    pipeline_name: str = Field(..., description="Pipeline name")
    status: PipelineStatus = Field(..., description="Current status")
    started_at: datetime = Field(..., description="Start timestamp")
    completed_at: datetime | None = Field(None, description="Completion timestamp")
    duration_ms: int | None = Field(
        None, description="Execution duration in milliseconds"
    )
    stages_completed: int = Field(0, description="Number of completed stages")
    total_stages: int = Field(0, description="Total number of stages")
    records_processed: int = Field(0, description="Number of records processed")
    error_message: str | None = Field(None, description="Error message if failed")


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="Health status")
    timestamp: datetime = Field(..., description="Check timestamp")
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")
    dependencies: dict[str, str] = Field(..., description="Dependency health status")


class LineageEventRequest(BaseModel):
    """OpenLineage event webhook request."""

    event: dict[str, Any] = Field(..., description="OpenLineage event JSON")
    source: str | None = Field(None, description="Event source")


class MetricsResponse(BaseModel):
    """Metrics endpoint response."""

    pipeline_runs_total: int = Field(..., description="Total pipeline runs")
    pipeline_runs_success: int = Field(..., description="Successful pipeline runs")
    pipeline_runs_failed: int = Field(..., description="Failed pipeline runs")
    avg_duration_ms: float = Field(..., description="Average execution duration")
    last_run_timestamp: datetime | None = Field(None, description="Last run timestamp")


# =============================================================================
# External Team Ingestion Models (Centralized Service)
# =============================================================================


class LineageIngestRequest(BaseModel):
    """Request model for external teams to send OpenLineage events."""

    namespace: str = Field(
        ...,
        description="Team namespace (e.g., 'team-data-platform')",
        min_length=3,
        max_length=50,
    )
    events: list[dict[str, Any]] = Field(
        ...,
        description="Array of OpenLineage event objects (min: 1, max: 100 items)",
    )
    source: str | None = Field(None, description="Event source identifier (optional)")


class LineageIngestResponse(BaseModel):
    """Response model for lineage ingestion."""

    accepted: int = Field(..., description="Number of events accepted")
    rejected: int = Field(..., description="Number of events rejected")
    errors: list[str] = Field(default_factory=list, description="Validation errors")
    namespace: str = Field(..., description="Target namespace")


class TelemetryIngestRequest(BaseModel):
    """Request model for external teams to send OpenTelemetry data."""

    namespace: str = Field(
        ...,
        description="Team namespace (e.g., 'team-ml-platform')",
        min_length=3,
        max_length=50,
    )
    traces: list[dict[str, Any]] = Field(
        default_factory=list, description="Array of OTLP trace spans"
    )
    metrics: list[dict[str, Any]] = Field(
        default_factory=list, description="Array of OTLP metric points"
    )
    source: str | None = Field(
        None, description="Telemetry source identifier (optional)"
    )


class TelemetryIngestResponse(BaseModel):
    """Response model for telemetry ingestion."""

    traces_accepted: int = Field(..., description="Number of trace spans accepted")
    metrics_accepted: int = Field(..., description="Number of metric points accepted")
    traces_rejected: int = Field(..., description="Number of trace spans rejected")
    metrics_rejected: int = Field(..., description="Number of metric points rejected")
    errors: list[str] = Field(default_factory=list, description="Processing errors")
    namespace: str = Field(..., description="Target namespace")


class NamespaceConfig(BaseModel):
    """Namespace configuration model."""

    name: str = Field(
        ..., description="Namespace identifier", min_length=3, max_length=50
    )
    display_name: str = Field(..., description="Human-readable namespace name")
    description: str | None = Field(None, description="Namespace description")

    # Access Control
    owners: list[str] = Field(default_factory=list, description="Owner email addresses")
    viewers: list[str] = Field(
        default_factory=list, description="Viewer email addresses"
    )

    # Resource Limits
    daily_event_quota: int = Field(
        default=100000, description="Maximum events per day", ge=1000, le=10000000
    )
    storage_retention_days: int = Field(
        default=30, description="Data retention period in days", ge=1, le=365
    )

    # Metadata
    created_at: datetime | None = Field(None, description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")

    # Custom Properties
    tags: dict[str, str] = Field(
        default_factory=dict, description="Custom namespace tags"
    )


class NamespaceCreateRequest(BaseModel):
    """Request model for creating a new namespace."""

    name: str = Field(
        ...,
        description="Namespace identifier (lowercase, alphanumeric + dashes)",
        min_length=3,
        max_length=50,
    )
    display_name: str = Field(..., description="Human-readable namespace name")
    description: str | None = Field(None, description="Namespace description")
    owners: list[str] = Field(
        ..., description="Owner email addresses (minimum 1 required)"
    )
    daily_event_quota: int = Field(
        default=100000, description="Maximum events per day", ge=1000, le=10000000
    )
    tags: dict[str, str] = Field(
        default_factory=dict, description="Custom namespace tags"
    )


class NamespaceListResponse(BaseModel):
    """Response model for listing namespaces."""

    namespaces: list[NamespaceConfig] = Field(..., description="Available namespaces")
    total: int = Field(..., description="Total number of namespaces")


class ErrorResponse(BaseModel):
    """Standard error response model."""

    error: str = Field(..., description="Error message")
    details: str | None = Field(None, description="Detailed error description")
    namespace: str | None = Field(None, description="Associated namespace")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Error timestamp"
    )
