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
