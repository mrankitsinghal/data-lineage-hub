"""Decorators for automatic lineage and telemetry tracking."""

import asyncio
import functools
import inspect
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import structlog
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap_generator

from .client import LineageHubClient, TelemetryClient
from .config import get_config
from .models import LineageEvent, TelemetryData

logger = structlog.get_logger(__name__)

F = TypeVar('F', bound=Callable[..., Any])


def lineage_track(
    job_name: Optional[str] = None,
    namespace: Optional[str] = None,
    inputs: Optional[List[str]] = None,
    outputs: Optional[List[str]] = None,
    description: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    run_id: Optional[str] = None,
    send_async: bool = True,
) -> Callable[[F], F]:
    """
    Decorator for automatic OpenLineage tracking of job functions.
    
    Args:
        job_name: Name of the job (defaults to function name)
        namespace: Namespace for the job (defaults to config namespace)
        inputs: List of input dataset identifiers
        outputs: List of output dataset identifiers  
        description: Job description
        tags: Additional tags for the job
        run_id: Specific run ID (defaults to UUID)
        send_async: Whether to send events asynchronously
        
    Example:
        @lineage_track(
            job_name="user_data_processing",
            inputs=["/data/users.csv"],
            outputs=["/data/processed_users.parquet"],
            description="Process user data with validation"
        )
        def process_users(input_path, output_path):
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
                    func, args, kwargs, actual_job_name, actual_namespace, 
                    inputs, outputs, description, tags, actual_run_id, send_async
                )
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                if send_async:
                    return _execute_with_lineage_sync_async(
                        func, args, kwargs, actual_job_name, actual_namespace,
                        inputs, outputs, description, tags, actual_run_id
                    )
                else:
                    return _execute_with_lineage_sync(
                        func, args, kwargs, actual_job_name, actual_namespace,
                        inputs, outputs, description, tags, actual_run_id
                    )
            return sync_wrapper
    
    return decorator


def telemetry_track(
    span_name: Optional[str] = None,
    service_name: Optional[str] = None,
    namespace: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
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
                        span.set_status(trace.Status(trace.StatusCode.OK))
                        return result
                    except Exception as e:
                        span.set_status(
                            trace.Status(trace.StatusCode.ERROR, str(e))
                        )
                        span.record_exception(e)
                        raise
            return async_wrapper
        else:
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
                        span.set_status(trace.Status(trace.StatusCode.OK))
                        return result
                    except Exception as e:
                        span.set_status(
                            trace.Status(trace.StatusCode.ERROR, str(e))
                        )
                        span.record_exception(e)
                        raise
            return sync_wrapper
    
    return decorator


async def _execute_with_lineage_async(
    func: Callable,
    args: tuple,
    kwargs: dict,
    job_name: str,
    namespace: str,
    inputs: Optional[List[str]],
    outputs: Optional[List[str]],
    description: Optional[str],
    tags: Optional[Dict[str, str]],
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
            run_id=run_id
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
            asyncio.create_task(client.send_lineage_events([start_event]))
        else:
            await client.send_lineage_events([start_event])
    except Exception as e:
        logger.warning("Failed to send START event", error=str(e))
    
    start_time = time.time()
    
    try:
        # Execute the function
        result = await func(*args, **kwargs)
        
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
                asyncio.create_task(client.send_lineage_events([complete_event]))
            else:
                await client.send_lineage_events([complete_event])
        except Exception as e:
            logger.warning("Failed to send COMPLETE event", error=str(e))
        
        return result
        
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
                asyncio.create_task(client.send_lineage_events([fail_event]))
            else:
                await client.send_lineage_events([fail_event])
        except Exception as send_error:
            logger.warning("Failed to send FAIL event", error=str(send_error))
        
        raise


def _execute_with_lineage_sync_async(
    func: Callable,
    args: tuple,
    kwargs: dict,
    job_name: str,
    namespace: str,
    inputs: Optional[List[str]],
    outputs: Optional[List[str]],
    description: Optional[str],
    tags: Optional[Dict[str, str]],
    run_id: str,
) -> Any:
    """Execute sync function with async lineage tracking."""
    # Create new event loop for async operations
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    async def async_execution():
        return await _execute_with_lineage_async(
            func, args, kwargs, job_name, namespace, inputs, outputs,
            description, tags, run_id, send_async=True
        )
    
    # For sync functions, we need to handle the async execution differently
    config = get_config()
    
    if config.dry_run:
        logger.info(
            "DRY RUN: Would track lineage",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id
        )
        return func(*args, **kwargs)
    
    # Send START event in background
    client = LineageHubClient()
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
    
    # Schedule async sending
    asyncio.create_task(client.send_lineage_events([start_event]))
    
    start_time = time.time()
    
    try:
        result = func(*args, **kwargs)
        
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
        
        asyncio.create_task(client.send_lineage_events([complete_event]))
        return result
        
    except Exception as e:
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
        
        asyncio.create_task(client.send_lineage_events([fail_event]))
        raise


def _execute_with_lineage_sync(
    func: Callable,
    args: tuple,
    kwargs: dict,
    job_name: str,
    namespace: str,
    inputs: Optional[List[str]],
    outputs: Optional[List[str]],
    description: Optional[str],
    tags: Optional[Dict[str, str]],
    run_id: str,
) -> Any:
    """Execute sync function with sync lineage tracking."""
    config = get_config()
    
    if config.dry_run:
        logger.info(
            "DRY RUN: Would track lineage",
            job_name=job_name,
            namespace=namespace,
            run_id=run_id
        )
        return func(*args, **kwargs)
    
    # For synchronous execution, we need to use sync methods
    # This is a simplified version - in practice, you might want a sync HTTP client
    logger.warning(
        "Synchronous lineage tracking not fully implemented. "
        "Consider using send_async=True or async functions."
    )
    
    return func(*args, **kwargs)


def _create_lineage_event(
    event_type: str,
    job_name: str,
    namespace: str,
    run_id: str,
    inputs: Optional[List[str]] = None,
    outputs: Optional[List[str]] = None,
    description: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    duration: Optional[float] = None,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    """Create OpenLineage event dictionary."""
    from datetime import datetime, timezone
    
    event = {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
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
        event["inputs"] = [
            {"namespace": "file", "name": input_path}
            for input_path in inputs
        ]
    
    # Add outputs if provided  
    if outputs:
        event["outputs"] = [
            {"namespace": "file", "name": output_path}
            for output_path in outputs
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