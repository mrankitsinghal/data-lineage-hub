"""API routes for the data lineage service."""

from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException

from src.config import settings

# Pipeline executor removed - using core ingestion APIs only
from src.services.namespace import namespace_service
from src.utils.kafka_client import get_kafka_publisher

from .middleware import get_current_user, validate_namespace_access
from .models import (
    HealthResponse,
    LineageIngestRequest,
    LineageIngestResponse,
    NamespaceConfig,
    NamespaceCreateRequest,
    NamespaceListResponse,
    TelemetryIngestRequest,
    TelemetryIngestResponse,
)


logger = structlog.get_logger(__name__)

# Create router
router = APIRouter()

# Core ingestion APIs only - pipeline execution removed


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


# Pipeline routes removed - using core ingestion APIs only


# =============================================================================
# Centralized Service Ingestion APIs for External Teams
# =============================================================================


@router.post("/lineage/ingest", response_model=LineageIngestResponse)
async def ingest_lineage_events(
    lineage_data: LineageIngestRequest,
    current_user: str | None = Depends(get_current_user),
):
    """Centralized endpoint for external teams to send OpenLineage events."""
    logger.info(
        "Received lineage ingestion request",
        namespace=lineage_data.namespace,
        event_count=len(lineage_data.events),
        source=lineage_data.source,
        user=current_user,
    )

    # TODO: Implement namespace access validation and event quota checking

    # Process events
    publisher = get_kafka_publisher()
    accepted = 0
    rejected = 0
    errors: list[str] = []

    for i, event in enumerate(lineage_data.events):
        try:
            # Add namespace to event metadata
            if "job" not in event:
                event["job"] = {}
            if "namespace" not in event["job"]:
                event["job"]["namespace"] = lineage_data.namespace

            # Publish to Kafka with namespace
            run_id = event.get("run", {}).get("runId")
            success = publisher.publish_openlineage_event(
                event, run_id, lineage_data.namespace
            )

            if success:
                accepted += 1
            else:
                rejected += 1
                errors.append(f"Event {i}: Failed to publish to Kafka")

        except (ValueError, KeyError, TypeError) as e:
            rejected += 1
            errors.append(f"Event {i}: {e!s}")

    logger.info(
        "Completed lineage ingestion",
        namespace=lineage_data.namespace,
        accepted=accepted,
        rejected=rejected,
        errors=len(errors),
    )

    return LineageIngestResponse(
        accepted=accepted,
        rejected=rejected,
        errors=errors,
        namespace=lineage_data.namespace,
    )


@router.post("/telemetry/ingest", response_model=TelemetryIngestResponse)
async def ingest_telemetry_data(
    telemetry_request: TelemetryIngestRequest,
    current_user: str | None = Depends(get_current_user),
):
    """Centralized endpoint for external teams to send OpenTelemetry data."""
    logger.info(
        "Received telemetry ingestion request",
        namespace=telemetry_request.namespace,
        traces_count=len(telemetry_request.traces),
        metrics_count=len(telemetry_request.metrics),
        source=telemetry_request.source,
        user=current_user,
    )

    # TODO: Implement namespace access validation

    publisher = get_kafka_publisher()

    # Process traces
    traces_accepted = 0
    traces_rejected = 0
    errors: list[str] = []

    for i, trace in enumerate(telemetry_request.traces):
        try:
            # Add namespace to trace attributes
            if "resource" not in trace:
                trace["resource"] = {}
            if "attributes" not in trace["resource"]:
                trace["resource"]["attributes"] = {}
            trace["resource"]["attributes"]["service.namespace"] = (
                telemetry_request.namespace
            )

            # Publish to Kafka OTEL spans topic with namespace
            trace_id = trace.get("traceId")
            success = publisher.publish_otel_span(
                trace, trace_id, telemetry_request.namespace
            )

            if success:
                traces_accepted += 1
            else:
                traces_rejected += 1
                errors.append(f"Trace {i}: Failed to publish to Kafka")

        except (ValueError, KeyError, TypeError) as e:
            traces_rejected += 1
            errors.append(f"Trace {i}: {e!s}")

    # Process metrics
    metrics_accepted = 0
    metrics_rejected = 0

    for i, metric in enumerate(telemetry_request.metrics):
        try:
            # Add namespace to metric attributes
            if "resource" not in metric:
                metric["resource"] = {}
            if "attributes" not in metric["resource"]:
                metric["resource"]["attributes"] = {}
            metric["resource"]["attributes"]["service.namespace"] = (
                telemetry_request.namespace
            )

            # Publish to Kafka OTEL metrics topic with namespace
            service_name = (
                metric.get("resource", {}).get("attributes", {}).get("service.name")
            )
            success = publisher.publish_otel_metric(
                metric, service_name, telemetry_request.namespace
            )

            if success:
                metrics_accepted += 1
            else:
                metrics_rejected += 1
                errors.append(f"Metric {i}: Failed to publish to Kafka")

        except (ValueError, KeyError, TypeError) as e:
            metrics_rejected += 1
            errors.append(f"Metric {i}: {e!s}")

    logger.info(
        "Completed telemetry ingestion",
        namespace=telemetry_request.namespace,
        traces_accepted=traces_accepted,
        traces_rejected=traces_rejected,
        metrics_accepted=metrics_accepted,
        metrics_rejected=metrics_rejected,
        errors=len(errors),
    )

    return TelemetryIngestResponse(
        traces_accepted=traces_accepted,
        metrics_accepted=metrics_accepted,
        traces_rejected=traces_rejected,
        metrics_rejected=metrics_rejected,
        errors=errors,
        namespace=telemetry_request.namespace,
    )


# =============================================================================
# Namespace Management APIs
# =============================================================================


@router.post("/namespaces", response_model=NamespaceConfig)
async def create_namespace(
    request: NamespaceCreateRequest,
    current_user: str | None = Depends(get_current_user),
):
    """Create a new namespace for team isolation."""
    logger.info(
        "Creating new namespace",
        namespace=request.name,
        display_name=request.display_name,
        owners=request.owners,
        user=current_user,
    )

    try:
        config = namespace_service.create_namespace(request)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception(
            "Failed to create namespace", namespace=request.name, error=str(e)
        )
        raise HTTPException(status_code=500, detail="Failed to create namespace") from e
    else:
        logger.info("Successfully created namespace", namespace=request.name)
        return config


@router.get("/namespaces", response_model=NamespaceListResponse)
async def list_namespaces(current_user: str | None = Depends(get_current_user)):
    """List accessible namespaces for the user."""
    logger.debug("Listing namespaces", user_email=current_user)

    namespaces = namespace_service.list_namespaces(current_user)

    return NamespaceListResponse(namespaces=namespaces, total=len(namespaces))


@router.get("/namespaces/{namespace_name}", response_model=NamespaceConfig)
async def get_namespace(
    namespace_name: str, current_user: str | None = Depends(get_current_user)
):
    """Get namespace configuration by name."""
    # Validate namespace access
    await validate_namespace_access(namespace_name, current_user)

    config = namespace_service.get_namespace(namespace_name)

    if not config:
        raise HTTPException(
            status_code=404, detail=f"Namespace '{namespace_name}' not found"
        )

    return config


@router.patch("/namespaces/{namespace_name}", response_model=NamespaceConfig)
async def update_namespace(
    namespace_name: str,
    updates: dict[str, Any],
    current_user: str | None = Depends(get_current_user),
):
    """Update namespace configuration."""
    # Validate namespace access (require owner permissions for updates)
    await validate_namespace_access(namespace_name, current_user, require_owner=True)

    logger.info(
        "Updating namespace", namespace=namespace_name, updates=list(updates.keys())
    )

    config = namespace_service.update_namespace(namespace_name, updates)

    if not config:
        raise HTTPException(
            status_code=404, detail=f"Namespace '{namespace_name}' not found"
        )

    return config
