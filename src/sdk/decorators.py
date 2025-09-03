"""Decorators for automatic lineage and telemetry tracking."""

import asyncio
import functools
import inspect
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar

import httpx
import structlog
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

from .client import LineageHubClient, TelemetryClient
from .config import get_config
from .types import create_dataset_specs


logger = structlog.get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

_otel_initialized = False


def _initialize_otel_if_needed() -> None:
    """Initialize OpenTelemetry if not already done."""
    global _otel_initialized  # noqa: PLW0603

    if _otel_initialized:
        return

    config = get_config()

    # Create resource with service information
    resource = Resource.create(
        {
            "service.name": "data-lineage-hub-sdk",
            "service.version": "1.0.0",
            "service.namespace": config.namespace,
        }
    )

    # Create and set tracer provider
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)

    _otel_initialized = True
    logger.debug("OpenTelemetry initialized for SDK")


def _send_span_to_api(span, namespace: str) -> None:
    """Extract span data and send to API."""
    try:
        # Extract span data in a format compatible with our API
        span_context = span.get_span_context()

        # Handle start/end times and duration safely
        start_time = getattr(span, "start_time", None)
        end_time = getattr(span, "end_time", None)
        current_time = time.time_ns()

        if start_time is None:
            start_time = current_time
        if end_time is None:
            end_time = current_time

        duration_ns = max(0, end_time - start_time)

        span_data = {
            "traceId": format(span_context.trace_id, "032x"),
            "spanId": format(span_context.span_id, "016x"),
            "operationName": getattr(span, "name", "unknown_operation"),
            "startTime": int(start_time / 1_000_000),  # Convert to milliseconds
            "duration": int(duration_ns / 1000),  # Convert to microseconds
            "tags": {},
            "status": {
                "code": "OK"  # Default to OK, we'll update based on span status if available
            },
        }

        # Handle status safely
        if hasattr(span, "status") and hasattr(span.status, "status_code"):
            span_data["status"]["code"] = (
                "OK" if span.status.status_code == trace.StatusCode.OK else "ERROR"
            )

        # Add attributes as tags
        if hasattr(span, "attributes") and span.attributes:
            span_data["tags"] = {str(k): str(v) for k, v in span.attributes.items()}

        # Send span data using single async context pattern (same fix as lineage)
        async def _send_telemetry_span():
            """Send span data in a single async context."""
            try:
                async with TelemetryClient(namespace=namespace) as client:
                    await client.send_span(span_data, namespace=namespace)
                logger.debug(
                    "Successfully sent span to API", trace_id=span_data["traceId"]
                )
            except Exception as e:
                logger.exception(
                    "Failed to send span to API",
                    error=str(e),
                    trace_id=span_data["traceId"],
                )

        # Check if we're already in an event loop
        try:
            # Try to get current event loop
            loop = asyncio.get_running_loop()
            # If we get here, we're in a running loop - schedule as task
            _ = loop.create_task(_send_telemetry_span())  # noqa: RUF006
        except RuntimeError:
            # No running loop, safe to use asyncio.run
            asyncio.run(_send_telemetry_span())

    except Exception as e:
        logger.exception("Error processing span for API", error=str(e))


def lineage_track(
    job_name: str | None = None,
    namespace: str | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
    description: str | None = None,
    tags: dict[str, str] | None = None,
    run_id: str | None = None,
    send_async: bool = True,
) -> Callable[[F], F]:
    """
    Decorator for explicit OpenLineage tracking with dict-based dataset specifications.

    Args:
        job_name: Name of the job (defaults to function name)
        namespace: Namespace for the job (defaults to config namespace)
        inputs: List of input dataset specifications
        outputs: List of output dataset specifications
        description: Job description
        tags: Additional tags for the job
        run_id: Specific run ID (defaults to UUID)
        send_async: Whether to send events asynchronously

    Dataset specification format:
        {
            "type": "mysql",           # AdapterType enum value
            "name": "schema.table",    # Resource identifier
            "format": "table",         # DataFormat enum value (optional)
            "namespace": "prod-db"     # Optional namespace
        }

    Example:
        @lineage_track(
            inputs=[
                {"type": "mysql", "name": "users.profiles", "format": "table", "namespace": "prod"},
                {"type": "s3", "name": "s3://data/events.parquet", "format": "parquet"}
            ],
            outputs=[
                {"type": "clickhouse", "name": "analytics.user_metrics", "format": "table"}
            ],
            description="Process user data for analytics"
        )
        def process_user_data():
            # Processing logic here
            pass
    """

    def decorator(func: F) -> F:
        config = get_config()

        if not config.enable_lineage:
            logger.debug("Lineage tracking disabled, skipping decoration")
            return func

        actual_job_name = job_name or func.__name__
        actual_namespace = namespace or config.namespace
        actual_run_id = run_id or str(uuid.uuid4())

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await _execute_with_lineage_async(
                    func,
                    args,
                    kwargs,
                    actual_job_name,
                    actual_namespace,
                    inputs,
                    outputs,
                    description,
                    tags,
                    actual_run_id,
                    send_async,
                )

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            if send_async:
                return _execute_with_lineage_sync_async(
                    func,
                    args,
                    kwargs,
                    actual_job_name,
                    actual_namespace,
                    inputs,
                    outputs,
                    description,
                    tags,
                    actual_run_id,
                )
            return _execute_with_lineage_sync(
                func,
                args,
                kwargs,
                actual_job_name,
                actual_namespace,
                inputs,
                outputs,
                description,
                tags,
                actual_run_id,
            )

        return sync_wrapper

    return decorator


def telemetry_track(
    span_name: str | None = None,
    service_name: str | None = None,
    namespace: str | None = None,
    tags: dict[str, str] | None = None,
) -> Callable[[F], F]:
    """
    Decorator for OpenTelemetry instrumentation only.

    Args:
        span_name: Name of the span (defaults to function name)
        service_name: Service name for telemetry
        namespace: Namespace for the telemetry data
        tags: Additional tags for the span

    Example:
        @telemetry_track(
            span_name="user_validation",
            service_name="user-service",
            tags={"component": "validator"}
        )
        def validate_user_data(data):
            # Validation logic here
            pass
    """

    def decorator(func: F) -> F:
        config = get_config()

        if not config.enable_telemetry:
            logger.debug("Telemetry tracking disabled, skipping decoration")
            return func

        actual_span_name = span_name or func.__name__
        actual_service_name = service_name or "unknown-service"
        actual_namespace = namespace or config.namespace

        # Initialize OpenTelemetry if needed
        _initialize_otel_if_needed()

        tracer = trace.get_tracer(__name__)

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                with tracer.start_as_current_span(actual_span_name) as span:
                    # Add tags as span attributes
                    if tags:
                        for key, value in tags.items():
                            span.set_attribute(key, value)

                    span.set_attribute("service.name", actual_service_name)
                    span.set_attribute("service.namespace", actual_namespace)
                    span.set_attribute("function.name", func.__name__)

                    try:
                        result = await func(*args, **kwargs)
                    except Exception as e:
                        span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                        span.record_exception(e)

                        # Send span data to API even on error
                        _send_span_to_api(span, actual_namespace)

                        raise
                    else:
                        span.set_status(trace.Status(trace.StatusCode.OK))

                        # Send span data to API after successful completion
                        _send_span_to_api(span, actual_namespace)

                        return result

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            with tracer.start_as_current_span(actual_span_name) as span:
                # Add tags as span attributes
                if tags:
                    for key, value in tags.items():
                        span.set_attribute(key, value)

                span.set_attribute("service.name", actual_service_name)
                span.set_attribute("service.namespace", actual_namespace)
                span.set_attribute("function.name", func.__name__)

                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    span.record_exception(e)

                    # Send span data to API even on error
                    _send_span_to_api(span, actual_namespace)

                    raise
                else:
                    span.set_status(trace.Status(trace.StatusCode.OK))

                    # Send span data to API after successful completion
                    _send_span_to_api(span, actual_namespace)

                    return result

        return sync_wrapper

    return decorator


async def _execute_with_lineage_async(
    func: Callable,
    args: tuple,
    kwargs: dict,
    job_name: str,
    namespace: str,
    inputs: list[dict[str, Any]] | None,
    outputs: list[dict[str, Any]] | None,
    description: str | None,
    tags: dict[str, str] | None,
    run_id: str,
    send_async: bool,
) -> Any:
    """Execute async function with lineage tracking."""
    config = get_config()

    if config.dry_run:
        logger.info(
            "DRY RUN: Would track lineage",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id,
        )
        return await func(*args, **kwargs)

    client = LineageHubClient()

    # Create START event
    start_event = _create_lineage_event(
        event_type="START",
        job_name=job_name,
        namespace=namespace,
        run_id=run_id,
        inputs=inputs,
        outputs=None,  # Don't include outputs in START
        description=description,
        tags=tags,
    )

    # Send START event
    try:
        if send_async:
            asyncio.run(client.send_lineage_events([start_event]))
        else:
            await client.send_lineage_events([start_event])
    except httpx.HTTPError as e:
        logger.warning("Failed to send START event", error=str(e))

    # Send pipeline metrics for START
    try:
        await _send_pipeline_metrics(
            event_type="START",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id,
            _inputs=inputs,
            _outputs=outputs,
        )
    except httpx.HTTPError as e:
        logger.warning("Failed to send START metrics", error=str(e))

    start_time = time.time()

    try:
        # Execute the function
        result = await func(*args, **kwargs)
    except Exception as e:
        # Create FAIL event
        fail_event = _create_lineage_event(
            event_type="FAIL",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            description=description,
            tags=tags,
            duration=time.time() - start_time,
            error_message=str(e),
        )

        # Send FAIL event
        try:
            if send_async:
                asyncio.run(client.send_lineage_events([fail_event]))
            else:
                await client.send_lineage_events([fail_event])
        except httpx.HTTPError as send_error:
            logger.warning("Failed to send FAIL event", error=str(send_error))

        # Send pipeline metrics for FAIL
        try:
            duration_ms = (time.time() - start_time) * 1000  # Convert to milliseconds
            await _send_pipeline_metrics(
                event_type="FAIL",
                job_name=job_name,
                namespace=namespace,
                run_id=run_id,
                duration_ms=duration_ms,
                _inputs=inputs,
                _outputs=outputs,
            )
        except httpx.HTTPError as metrics_error:
            logger.warning("Failed to send FAIL metrics", error=str(metrics_error))

        raise
    else:
        # Create COMPLETE event
        complete_event = _create_lineage_event(
            event_type="COMPLETE",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            description=description,
            tags=tags,
            duration=time.time() - start_time,
        )

        # Send COMPLETE event
        try:
            if send_async:
                asyncio.run(client.send_lineage_events([complete_event]))
            else:
                await client.send_lineage_events([complete_event])
        except httpx.HTTPError as e:
            logger.warning("Failed to send COMPLETE event", error=str(e))

        # Send pipeline metrics for COMPLETE
        try:
            duration_ms = (time.time() - start_time) * 1000  # Convert to milliseconds
            await _send_pipeline_metrics(
                event_type="COMPLETE",
                job_name=job_name,
                namespace=namespace,
                run_id=run_id,
                duration_ms=duration_ms,
                _inputs=inputs,
                _outputs=outputs,
            )
        except httpx.HTTPError as e:
            logger.warning("Failed to send COMPLETE metrics", error=str(e))

        return result


def _execute_with_lineage_sync_async(
    func: Callable,
    args: tuple,
    kwargs: dict,
    job_name: str,
    namespace: str,
    inputs: list[dict[str, Any]] | None,
    outputs: list[dict[str, Any]] | None,
    description: str | None,
    tags: dict[str, str] | None,
    run_id: str,
) -> Any:
    """Execute sync function with async lineage tracking."""
    config = get_config()

    if config.dry_run:
        logger.info(
            "DRY RUN: Would track lineage",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id,
        )
        return func(*args, **kwargs)

    async def _send_lineage_events():
        """Send all lineage events in a single async context."""
        async with LineageHubClient() as client:
            # Send START event
            start_event = _create_lineage_event(
                event_type="START",
                job_name=job_name,
                namespace=namespace,
                run_id=run_id,
                inputs=inputs,
                outputs=None,
                description=description,
                tags=tags,
            )

            try:
                await client.send_lineage_events([start_event])
            except httpx.HTTPError as e:
                logger.warning("Failed to send START event", error=str(e))

            # Send pipeline metrics for START
            try:
                await _send_pipeline_metrics(
                    event_type="START",
                    job_name=job_name,
                    namespace=namespace,
                    run_id=run_id,
                    _inputs=inputs,
                    _outputs=outputs,
                )
            except httpx.HTTPError as e:
                logger.warning("Failed to send START metrics", error=str(e))

            # Execute the actual function
            start_time = time.time()

            try:
                result = func(*args, **kwargs)

                # Send COMPLETE event
                complete_event = _create_lineage_event(
                    event_type="COMPLETE",
                    job_name=job_name,
                    namespace=namespace,
                    run_id=run_id,
                    inputs=inputs,
                    outputs=outputs,
                    description=description,
                    tags=tags,
                    duration=time.time() - start_time,
                )

                try:
                    await client.send_lineage_events([complete_event])
                except httpx.HTTPError as e:
                    logger.warning("Failed to send COMPLETE event", error=str(e))

                # Send pipeline metrics for COMPLETE
                try:
                    duration_ms = (
                        time.time() - start_time
                    ) * 1000  # Convert to milliseconds
                    await _send_pipeline_metrics(
                        event_type="COMPLETE",
                        job_name=job_name,
                        namespace=namespace,
                        run_id=run_id,
                        duration_ms=duration_ms,
                        _inputs=inputs,
                        _outputs=outputs,
                    )
                except httpx.HTTPError as e:
                    logger.warning("Failed to send COMPLETE metrics", error=str(e))

                return result  # noqa: TRY300

            except Exception as e:
                # Send FAIL event
                fail_event = _create_lineage_event(
                    event_type="FAIL",
                    job_name=job_name,
                    namespace=namespace,
                    run_id=run_id,
                    inputs=inputs,
                    outputs=outputs,
                    description=description,
                    tags=tags,
                    duration=time.time() - start_time,
                    error_message=str(e),
                )

                try:
                    await client.send_lineage_events([fail_event])
                except httpx.HTTPError as send_error:
                    logger.warning("Failed to send FAIL event", error=str(send_error))

                # Send pipeline metrics for FAIL
                try:
                    duration_ms = (
                        time.time() - start_time
                    ) * 1000  # Convert to milliseconds
                    await _send_pipeline_metrics(
                        event_type="FAIL",
                        job_name=job_name,
                        namespace=namespace,
                        run_id=run_id,
                        duration_ms=duration_ms,
                        _inputs=inputs,
                        _outputs=outputs,
                    )
                except httpx.HTTPError as metrics_error:
                    logger.warning(
                        "Failed to send FAIL metrics", error=str(metrics_error)
                    )

                raise

    return asyncio.run(_send_lineage_events())


def _execute_with_lineage_sync(
    func: Callable,
    args: tuple,
    kwargs: dict,
    job_name: str,
    namespace: str,
    _inputs: list[dict[str, Any]] | None,
    _outputs: list[dict[str, Any]] | None,
    _description: str | None,
    _tags: dict[str, str] | None,
    run_id: str,
) -> Any:
    """Execute sync function with sync lineage tracking."""
    config = get_config()

    if config.dry_run:
        logger.info(
            "DRY RUN: Would track lineage",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id,
        )
        return func(*args, **kwargs)

    # For synchronous execution, we need to use sync methods
    # This is a simplified version - in practice, you might want a sync HTTP client
    logger.warning(
        "Synchronous lineage tracking not fully implemented. "
        "Consider using send_async=True or async functions."
    )

    return func(*args, **kwargs)


async def _send_pipeline_metrics(
    event_type: str,
    job_name: str,
    namespace: str,
    run_id: str,
    duration_ms: float | None = None,
    record_count: int | None = None,
    _inputs: list[dict[str, Any]] | None = None,
    _outputs: list[dict[str, Any]] | None = None,
) -> None:
    """Send pipeline metrics to the telemetry API."""
    try:
        # Use TelemetryClient which internally uses LineageHubClient
        telemetry_client = TelemetryClient(namespace=namespace)

        timestamp = datetime.now(UTC).isoformat()

        metrics = []

        # Pipeline run metrics
        if event_type == "START":
            metrics.append(
                {
                    "name": "pipeline_runs_total",
                    "value": 1,
                    "timestamp": timestamp,
                    "attributes": {
                        "job_name": job_name,
                        "run_id": run_id,
                        "namespace": namespace,
                    },
                }
            )
        elif event_type == "COMPLETE":
            metrics.extend(
                [
                    {
                        "name": "pipeline_runs_success_total",
                        "value": 1,
                        "timestamp": timestamp,
                        "attributes": {
                            "job_name": job_name,
                            "run_id": run_id,
                            "namespace": namespace,
                        },
                    },
                ]
            )

            # Duration metric
            if duration_ms is not None:
                metrics.append(
                    {
                        "name": "pipeline_duration_milliseconds",
                        "value": duration_ms,
                        "timestamp": timestamp,
                        "attributes": {
                            "job_name": job_name,
                            "run_id": run_id,
                            "namespace": namespace,
                        },
                    }
                )

            # Records processed metric
            if record_count is not None:
                metrics.append(
                    {
                        "name": "records_processed_total",
                        "value": record_count,
                        "timestamp": timestamp,
                        "attributes": {
                            "job_name": job_name,
                            "run_id": run_id,
                            "namespace": namespace,
                            "record_source": "estimated",
                        },
                    }
                )

        elif event_type == "FAIL":
            metrics.append(
                {
                    "name": "pipeline_runs_failed_total",
                    "value": 1,
                    "timestamp": timestamp,
                    "attributes": {
                        "job_name": job_name,
                        "run_id": run_id,
                        "namespace": namespace,
                    },
                }
            )

        if metrics:
            await telemetry_client.send_metrics(metrics, namespace=namespace)
            logger.debug(
                "Successfully sent pipeline metrics",
                event_type=event_type,
                metrics_count=len(metrics),
                job_name=job_name,
            )

        await telemetry_client.close()

    except httpx.HTTPError as e:
        logger.warning(
            "Failed to send pipeline metrics",
            error=str(e),
            event_type=event_type,
            job_name=job_name,
        )


def _estimate_record_count(
    inputs: list[dict[str, Any]] | None, outputs: list[dict[str, Any]] | None
) -> int:
    """Estimate record count based on dataset types (simple heuristic)."""
    # Simple heuristic: assume 1000 records per dataset for demo purposes
    # In real implementation, this could be based on dataset metadata
    total_datasets = 0
    if inputs:
        total_datasets += len(inputs)
    if outputs:
        total_datasets += len(outputs)

    return total_datasets * 1000  # Rough estimate for demo


def _create_lineage_event(
    event_type: str,
    job_name: str,
    namespace: str,
    run_id: str,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
    description: str | None = None,
    tags: dict[str, str] | None = None,
    duration: float | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Create OpenLineage event dictionary with dict-based dataset specifications."""

    event = {
        "eventType": event_type,
        "eventTime": datetime.now(UTC).isoformat(),
        "run": {"runId": run_id},
        "job": {
            "namespace": namespace,
            "name": job_name,
        },
        "producer": "data-lineage-hub-sdk/1.0.0",
    }

    # Add job description if provided
    if description:
        event["job"]["description"] = description

    # Add inputs if provided
    if inputs:
        try:
            input_specs = create_dataset_specs(inputs)
            event["inputs"] = [spec.to_openlineage_dataset() for spec in input_specs]
        except (ValueError, TypeError) as e:
            logger.warning("Failed to process input specifications: %s", e)
            # Fallback to simple format
            event["inputs"] = [
                {"namespace": "unknown", "name": str(inp)} for inp in inputs
            ]

    # Add outputs if provided
    if outputs:
        try:
            output_specs = create_dataset_specs(outputs)
            event["outputs"] = [spec.to_openlineage_dataset() for spec in output_specs]
        except (ValueError, TypeError) as e:
            logger.warning("Failed to process output specifications: %s", e)
            # Fallback to simple format
            event["outputs"] = [
                {"namespace": "unknown", "name": str(out)} for out in outputs
            ]

    # Add custom facets for additional metadata
    run_facets = {}

    if duration is not None:
        # Add processing time facet
        run_facets["processing_time"] = {
            "_producer": "data-lineage-hub-sdk",
            "_schemaURL": "custom://processing_time",
            "duration_seconds": duration,
        }

    if tags:
        # Add tags facet
        run_facets["tags"] = {
            "_producer": "data-lineage-hub-sdk",
            "_schemaURL": "custom://tags",
            "tags": tags,
        }

    if error_message:
        # Add error info facet
        run_facets["error_info"] = {
            "_producer": "data-lineage-hub-sdk",
            "_schemaURL": "custom://error_info",
            "error_message": error_message,
            "error_type": "execution_error",
        }

    if run_facets:
        event["run"]["facets"] = run_facets

    return event
