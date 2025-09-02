"""OpenTelemetry configuration and instrumentation."""

import structlog
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from src.config import settings


logger = structlog.get_logger(__name__)


def configure_opentelemetry() -> None:
    """Configure OpenTelemetry tracing and metrics."""

    # Create resource with service information
    resource = Resource.create(
        {
            SERVICE_NAME: settings.otel_service_name,
            SERVICE_VERSION: settings.otel_service_version,
            "service.namespace": settings.openlineage_namespace,
        }
    )

    # Configure tracing
    trace_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(trace_provider)

    # OTLP span exporter for OTEL Collector
    otlp_span_exporter = OTLPSpanExporter(
        endpoint=settings.otel_collector_endpoint,
        insecure=True,  # Use insecure connection for local development
    )
    span_processor = BatchSpanProcessor(otlp_span_exporter)
    trace_provider.add_span_processor(span_processor)
    logger.info(
        "OTLP span exporter configured", endpoint=settings.otel_collector_endpoint
    )

    # Configure metrics with OTLP exporter
    otlp_metrics_exporter = OTLPMetricExporter(
        endpoint=settings.otel_collector_endpoint,
        insecure=True,  # Use insecure connection for local development
    )
    metric_reader = PeriodicExportingMetricReader(
        exporter=otlp_metrics_exporter,
        export_interval_millis=10000,  # Export every 10 seconds for faster feedback
    )
    metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(metric_provider)
    logger.info(
        "OTLP metrics exporter configured", endpoint=settings.otel_collector_endpoint
    )

    logger.info("OpenTelemetry configured", service=settings.otel_service_name)


def instrument_app(app) -> None:
    """Instrument FastAPI application with OpenTelemetry."""

    # Auto-instrument FastAPI
    FastAPIInstrumentor.instrument_app(app)

    # Auto-instrument HTTP requests
    RequestsInstrumentor().instrument()

    logger.info("FastAPI application instrumented with OpenTelemetry")


def get_tracer(name: str | None = None) -> trace.Tracer:
    """Get OpenTelemetry tracer."""
    return trace.get_tracer(name or __name__)


def get_meter(name: str | None = None) -> metrics.Meter:
    """Get OpenTelemetry meter."""
    return metrics.get_meter(name or __name__)


class PipelineMetrics:
    """Custom metrics for pipeline monitoring."""

    def __init__(self):
        self.meter = get_meter("pipeline_metrics")

        # Counters
        self.pipeline_runs_total = self.meter.create_counter(
            "pipeline_runs_total", description="Total number of pipeline runs"
        )

        self.pipeline_runs_success = self.meter.create_counter(
            "pipeline_runs_success_total",
            description="Total number of successful pipeline runs",
        )

        self.pipeline_runs_failed = self.meter.create_counter(
            "pipeline_runs_failed_total",
            description="Total number of failed pipeline runs",
        )

        self.records_processed_total = self.meter.create_counter(
            "records_processed_total", description="Total number of records processed"
        )

        # Histograms
        self.pipeline_duration = self.meter.create_histogram(
            "pipeline_duration_milliseconds",
            description="Pipeline execution duration in milliseconds",
        )

        self.stage_duration = self.meter.create_histogram(
            "stage_duration_milliseconds",
            description="Stage execution duration in milliseconds",
        )

    def record_pipeline_start(self, pipeline_name: str, run_id: str) -> None:
        """Record pipeline start."""
        self.pipeline_runs_total.add(
            1, {"pipeline_name": pipeline_name, "run_id": run_id}
        )

    def record_pipeline_success(
        self, pipeline_name: str, run_id: str, duration_ms: int, records: int
    ) -> None:
        """Record successful pipeline completion."""
        attributes = {"pipeline_name": pipeline_name, "run_id": run_id}

        self.pipeline_runs_success.add(1, attributes)
        self.pipeline_duration.record(duration_ms, attributes)
        self.records_processed_total.add(records, attributes)

    def record_pipeline_failure(
        self, pipeline_name: str, run_id: str, duration_ms: int, error: str
    ) -> None:
        """Record pipeline failure."""
        attributes = {"pipeline_name": pipeline_name, "run_id": run_id, "error": error}

        self.pipeline_runs_failed.add(1, attributes)
        self.pipeline_duration.record(duration_ms, attributes)

    def record_stage_duration(
        self, stage_name: str, duration_ms: int, run_id: str
    ) -> None:
        """Record stage execution duration."""
        attributes = {"stage_name": stage_name, "run_id": run_id}
        self.stage_duration.record(duration_ms, attributes)


# Global metrics instance
_pipeline_metrics: PipelineMetrics | None = None


def get_pipeline_metrics() -> PipelineMetrics:
    """Get or create pipeline metrics instance."""
    global _pipeline_metrics
    if _pipeline_metrics is None:
        _pipeline_metrics = PipelineMetrics()
    return _pipeline_metrics


def pipeline_trace(operation_name: str):
    """Decorator for tracing pipeline operations."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            tracer = get_tracer()

            with tracer.start_as_current_span(operation_name) as span:
                # Add attributes
                span.set_attribute("operation.name", operation_name)
                span.set_attribute("service.name", settings.otel_service_name)

                try:
                    result = func(*args, **kwargs)
                    span.set_attribute("operation.status", "success")
                    return result
                except Exception as e:
                    span.set_attribute("operation.status", "error")
                    span.set_attribute("error.message", str(e))
                    span.record_exception(e)
                    raise

        return wrapper

    return decorator
