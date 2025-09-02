"""API routes for the data lineage service."""

import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, BackgroundTasks, HTTPException

from src.config import settings
from src.pipeline.executor import PipelineExecutor
from src.utils.kafka_client import get_kafka_publisher

from .models import (
    HealthResponse,
    LineageEventRequest,
    MetricsResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineStatus,
)


logger = structlog.get_logger(__name__)

# Create router
router = APIRouter()

# In-memory storage for demo purposes
pipeline_runs: dict[str, dict[str, Any]] = {}


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint with dependency status."""
    dependencies = {}

    # Check Kafka connectivity
    try:
        publisher = get_kafka_publisher()
        dependencies["kafka"] = "healthy" if publisher.producer else "unhealthy"
    except (ImportError, RuntimeError, ConnectionError):
        dependencies["kafka"] = "unhealthy"

    # Check other services (simplified)
    dependencies["marquez"] = "unknown"  # Would check HTTP endpoint
    dependencies["clickhouse"] = "unknown"  # Would check connection

    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(UTC),
        service=settings.app_name,
        version=settings.app_version,
        dependencies=dependencies,
    )


@router.post("/pipeline/run", response_model=PipelineRunResponse)
async def run_pipeline(request: PipelineRunRequest, background_tasks: BackgroundTasks):
    """Execute a data pipeline with lineage tracking."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    logger.info(
        "Starting pipeline execution",
        run_id=run_id,
        pipeline_name=request.pipeline_name,
        input_path=request.input_path,
        output_path=request.output_path,
    )

    # Create run record
    run_data = {
        "run_id": run_id,
        "pipeline_name": request.pipeline_name,
        "status": PipelineStatus.RUNNING,
        "started_at": started_at,
        "completed_at": None,
        "duration_ms": None,
        "stages_completed": 0,
        "total_stages": 3,  # Our sample pipeline has 3 stages
        "records_processed": 0,
        "error_message": None,
        "input_path": request.input_path,
        "output_path": request.output_path,
        "parameters": request.parameters,
    }

    pipeline_runs[run_id] = run_data

    # Start pipeline execution in background
    executor = PipelineExecutor(run_id, request)
    background_tasks.add_task(executor.execute)

    return PipelineRunResponse(
        run_id=run_data["run_id"],
        pipeline_name=run_data["pipeline_name"],
        status=run_data["status"],
        started_at=run_data["started_at"],
        completed_at=run_data["completed_at"],
        duration_ms=run_data["duration_ms"],
        stages_completed=run_data["stages_completed"],
        total_stages=run_data["total_stages"],
        records_processed=run_data["records_processed"],
        error_message=run_data["error_message"],
        parameters=run_data["parameters"],
    )


@router.get("/pipeline/run/{run_id}", response_model=PipelineRunResponse)
async def get_pipeline_run(run_id: str):
    """Get pipeline run status and details."""
    if run_id not in pipeline_runs:
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    run_data = pipeline_runs[run_id]
    return PipelineRunResponse(
        run_id=run_data["run_id"],
        pipeline_name=run_data["pipeline_name"],
        status=run_data["status"],
        started_at=run_data["started_at"],
        completed_at=run_data["completed_at"],
        duration_ms=run_data["duration_ms"],
        stages_completed=run_data["stages_completed"],
        total_stages=run_data["total_stages"],
        records_processed=run_data["records_processed"],
        error_message=run_data["error_message"],
        parameters=run_data["parameters"],
    )


@router.get("/pipeline/runs", response_model=list[PipelineRunResponse])
async def list_pipeline_runs(limit: int = 10, offset: int = 0):
    """List recent pipeline runs."""
    all_runs = list(pipeline_runs.values())
    # Sort by started_at descending
    all_runs.sort(key=lambda x: x["started_at"], reverse=True)

    paginated_runs = all_runs[offset : offset + limit]
    return [PipelineRunResponse(**run_data) for run_data in paginated_runs]


@router.post("/webhook/lineage")
async def receive_lineage_event(request: LineageEventRequest):
    """Webhook endpoint for receiving OpenLineage events."""
    logger.info(
        "Received lineage event",
        event_type=request.event.get("eventType"),
        job_name=request.event.get("job", {}).get("name"),
        source=request.source,
    )

    # Publish event to Kafka
    publisher = get_kafka_publisher()
    run_id = request.event.get("run", {}).get("runId")

    success = publisher.publish_openlineage_event(request.event, run_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to publish lineage event")

    return {"status": "accepted", "message": "Lineage event received and queued"}


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics():
    """Get pipeline execution metrics."""
    if not pipeline_runs:
        return MetricsResponse(
            pipeline_runs_total=0,
            pipeline_runs_success=0,
            pipeline_runs_failed=0,
            avg_duration_ms=0.0,
            last_run_timestamp=None,
        )

    runs = list(pipeline_runs.values())
    total_runs = len(runs)
    success_runs = len([r for r in runs if r["status"] == PipelineStatus.COMPLETED])
    failed_runs = len([r for r in runs if r["status"] == PipelineStatus.FAILED])

    # Calculate average duration for completed runs
    completed_runs = [r for r in runs if r["duration_ms"] is not None]
    avg_duration = (
        sum(r["duration_ms"] for r in completed_runs) / len(completed_runs)
        if completed_runs
        else 0.0
    )

    # Get last run timestamp
    last_run = max(runs, key=lambda x: x["started_at"]) if runs else None
    last_timestamp = last_run["started_at"] if last_run else None

    return MetricsResponse(
        pipeline_runs_total=total_runs,
        pipeline_runs_success=success_runs,
        pipeline_runs_failed=failed_runs,
        avg_duration_ms=avg_duration,
        last_run_timestamp=last_timestamp,
    )


def update_pipeline_run(run_id: str, **updates) -> None:
    """Update pipeline run data (used by executor)."""
    if run_id in pipeline_runs:
        pipeline_runs[run_id].update(updates)
        logger.debug("Updated pipeline run", run_id=run_id, updates=updates)
