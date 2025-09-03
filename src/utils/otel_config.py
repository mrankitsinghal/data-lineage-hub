"""OpenTelemetry configuration with Kafka publishing."""

from collections.abc import Sequence

import structlog
from opentelemetry import metrics, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from src.config import settings
from src.utils.kafka_client import get_kafka_publisher


logger = structlog.get_logger(__name__)


class KafkaSpanExporter(SpanExporter):
    """Custom span exporter that sends spans to Kafka."""

    def __init__(self, namespace: str = "internal"):
        self.kafka_publisher = get_kafka_publisher()
        self.namespace = namespace

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Export spans to Kafka."""
        try:
            for span in spans:
                span_data = {
                    "traceId": format(span.context.trace_id, "032x"),
                    "spanId": format(span.context.span_id, "016x"),
                    "parentSpanId": format(span.parent.span_id, "016x")
                    if span.parent
                    else "",
                    "operationName": span.name,
                    "serviceName": span.resource.attributes.get(
                        "service.name", "unknown"
                    ),
                    "duration": span.end_time - span.start_time if span.end_time else 0,
                    "startTime": span.start_time,
                    "endTime": span.end_time or 0,
                    "status": {"code": span.status.status_code.name},
                    "kind": span.kind.name,
                    "tags": dict(span.attributes) if span.attributes else {},
                    "process": {
                        "serviceName": span.resource.attributes.get(
                            "service.name", "unknown"
                        ),
                        "tags": dict(span.resource.attributes)
                        if span.resource.attributes
                        else {},
                    },
                }

                trace_id = format(span.context.trace_id, "032x")
                success = self.kafka_publisher.publish_otel_span(
                    span_data, trace_id, self.namespace
                )

                if not success:
                    logger.warning("Failed to publish span to Kafka", trace_id=trace_id)

            return SpanExportResult.SUCCESS

        except Exception as e:
            logger.exception("Error exporting spans to Kafka", error=str(e))
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        """Shutdown the exporter."""


def configure_opentelemetry() -> None:
    """Configure OpenTelemetry with Kafka publishing."""

    # Create resource with service information
    resource = Resource.create(
        {
            SERVICE_NAME: settings.otel_service_name,
            SERVICE_VERSION: settings.otel_service_version,
            "service.namespace": settings.default_namespace,
        }
    )

    # Configure tracing with Kafka exporter
    trace_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(trace_provider)

    # Use Kafka span exporter instead of OTLP
    kafka_span_exporter = KafkaSpanExporter(namespace="internal")
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    span_processor = BatchSpanProcessor(kafka_span_exporter)
    trace_provider.add_span_processor(span_processor)
    logger.info("Kafka span exporter configured for internal service")

    # Configure basic meter provider (metrics will be handled by PipelineMetrics class)
    metric_provider = MeterProvider(resource=resource)
    metrics.set_meter_provider(metric_provider)
    logger.info("Basic metric provider configured")

    logger.info(
        "OpenTelemetry configured with Kafka publishing",
        service=settings.otel_service_name,
    )


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
