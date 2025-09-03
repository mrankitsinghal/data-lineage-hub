"""OpenTelemetry consumer for processing telemetry data from Kafka to ClickHouse."""

import json
import threading
import time
from datetime import UTC, datetime
from typing import Any

import structlog
from confluent_kafka import Message

from src.config import settings
from src.utils.clickhouse_client import ClickHouseClient
from src.utils.kafka_client import KafkaEventConsumer


logger = structlog.get_logger(__name__)


class OTelConsumer:
    """Consumer for processing OpenTelemetry data from Kafka topics."""

    def __init__(self) -> None:
        self.clickhouse = ClickHouseClient()
        self.span_batch: list[dict[str, Any]] = []
        self.metric_batch: list[dict[str, Any]] = []
        self.batch_size = 100
        self.batch_timeout = 30  # seconds

        # Time-based flushing
        self.last_span_flush = time.time()
        self.last_metric_flush = time.time()
        self.flush_lock = threading.Lock()
        self.shutdown_event = threading.Event()

    def process_otel_message(self, message: Message) -> None:
        """Process a single OpenTelemetry message from Kafka."""
        try:
            # Deserialize message
            data = json.loads(message.value().decode("utf-8"))
            topic = message.topic()

            # Extract namespace from headers or key
            namespace = self._extract_namespace(message)

            if topic == settings.kafka_otel_spans_topic:
                self._process_span(data, namespace)
            elif topic == settings.kafka_otel_metrics_topic:
                self._process_metric(data, namespace)
            else:
                logger.warning("Unknown OTEL topic", topic=topic)

            # Check if we should flush batches (size-based or time-based)
            current_time = time.time()

            if len(self.span_batch) >= self.batch_size or (
                self.span_batch
                and current_time - self.last_span_flush >= self.batch_timeout
            ):
                self._flush_spans()

            if len(self.metric_batch) >= self.batch_size or (
                self.metric_batch
                and current_time - self.last_metric_flush >= self.batch_timeout
            ):
                self._flush_metrics()

        except Exception as e:
            logger.exception(
                "Error processing OTEL message",
                error=str(e),
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
            )

    def _extract_namespace(self, message: Message) -> str:
        """Extract namespace from message headers or key."""
        # Try headers first
        if message.headers():
            for key, value in message.headers():
                if key == "namespace":
                    return value.decode("utf-8")

        # Try key format (namespace:identifier)
        if message.key():
            key_str = message.key().decode("utf-8")
            if ":" in key_str:
                return key_str.split(":")[0]

        # Default namespace
        return "internal"

    def _process_span(self, span_data: dict[str, Any], namespace: str) -> None:
        """Process a single span and add to batch."""
        try:
            # Convert attributes to string values (ClickHouse Map requires string values)
            def convert_to_string_map(data: dict[str, Any]) -> dict[str, str]:
                return {k: str(v) for k, v in data.items()}

            # Extract span fields for ClickHouse
            span_record = {
                "timestamp": datetime.now(UTC),
                "trace_id": span_data.get("traceId", ""),
                "span_id": span_data.get("spanId", ""),
                "parent_span_id": span_data.get("parentSpanId", ""),
                "operation_name": span_data.get("operationName", ""),
                "service_name": span_data.get("serviceName", "unknown"),
                "duration_ns": span_data.get("duration", 0),
                "status_code": span_data.get("status", {}).get("code", "OK"),
                "span_kind": span_data.get("kind", "INTERNAL"),
                "namespace": namespace,
                "attributes": convert_to_string_map(span_data.get("tags", {})),
                "resource_attributes": convert_to_string_map(
                    span_data.get("process", {}).get("tags", {})
                ),
                "events": [],  # Extract events if present
            }

            # Extract events if present
            if "logs" in span_data:
                span_record["events"] = [
                    (
                        datetime.fromtimestamp(
                            log.get("timestamp", 0) / 1000000, tz=UTC
                        ),  # microseconds to seconds
                        log.get("fields", {}).get("event", "log"),
                        log.get("fields", {}),
                    )
                    for log in span_data["logs"]
                ]

            self.span_batch.append(span_record)
            logger.debug(
                "Added span to batch",
                trace_id=span_record["trace_id"],
                batch_size=len(self.span_batch),
            )

        except Exception as e:
            logger.exception("Error processing span", error=str(e), span_data=span_data)

    def _process_metric(self, metric_data: dict[str, Any], namespace: str) -> None:
        """Process a single metric and add to batch."""
        try:
            # Convert attributes to string values (ClickHouse Map requires string values)
            def convert_to_string_map(data: dict[str, Any]) -> dict[str, str]:
                return {k: str(v) for k, v in data.items()}

            # Extract metric fields for ClickHouse
            metric_record = {
                "timestamp": datetime.now(UTC),
                "metric_name": metric_data.get("name", "unknown"),
                "metric_type": metric_data.get("type", "gauge"),
                "value": float(metric_data.get("value", 0)),
                "unit": metric_data.get("unit", ""),
                "service_name": metric_data.get("serviceName", "unknown"),
                "namespace": namespace,
                "attributes": convert_to_string_map(metric_data.get("tags", {})),
                "resource_attributes": convert_to_string_map(
                    metric_data.get("resource", {}).get("attributes", {})
                ),
            }

            self.metric_batch.append(metric_record)
            logger.debug(
                "Added metric to batch",
                metric_name=metric_record["metric_name"],
                batch_size=len(self.metric_batch),
            )

        except Exception as e:
            logger.exception(
                "Error processing metric", error=str(e), metric_data=metric_data
            )

    def _flush_spans(self) -> None:
        """Flush span batch to ClickHouse."""
        with self.flush_lock:
            if not self.span_batch:
                return

            try:
                batch_to_flush = self.span_batch.copy()
                success = self.clickhouse.insert_otel_spans(batch_to_flush)
                if success:
                    logger.info(
                        "Flushed spans to ClickHouse", count=len(batch_to_flush)
                    )
                    self.span_batch.clear()
                    self.last_span_flush = time.time()
                else:
                    logger.error(
                        "Failed to flush spans to ClickHouse", count=len(batch_to_flush)
                    )

            except Exception as e:
                logger.exception(
                    "Error flushing spans", error=str(e), count=len(self.span_batch)
                )

    def _flush_metrics(self) -> None:
        """Flush metric batch to ClickHouse."""
        with self.flush_lock:
            if not self.metric_batch:
                return

            try:
                batch_to_flush = self.metric_batch.copy()
                success = self.clickhouse.insert_otel_metrics(batch_to_flush)
                if success:
                    logger.info(
                        "Flushed metrics to ClickHouse", count=len(batch_to_flush)
                    )
                    self.metric_batch.clear()
                    self.last_metric_flush = time.time()
                else:
                    logger.error(
                        "Failed to flush metrics to ClickHouse",
                        count=len(batch_to_flush),
                    )

            except Exception as e:
                logger.exception(
                    "Error flushing metrics", error=str(e), count=len(self.metric_batch)
                )

    def _periodic_flush(self) -> None:
        """Background thread for periodic time-based flushing."""
        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()

                # Check if spans need time-based flushing
                if (
                    self.span_batch
                    and current_time - self.last_span_flush >= self.batch_timeout
                ):
                    logger.debug("Time-based flush triggered for spans")
                    self._flush_spans()

                # Check if metrics need time-based flushing
                if (
                    self.metric_batch
                    and current_time - self.last_metric_flush >= self.batch_timeout
                ):
                    logger.debug("Time-based flush triggered for metrics")
                    self._flush_metrics()

                # Sleep for a reasonable interval (check every 5 seconds)
                self.shutdown_event.wait(5)

            except Exception as e:
                logger.exception("Error in periodic flush", error=str(e))

    def start(self) -> None:
        """Start the OTEL consumer."""
        logger.info("Starting OTEL consumer")

        # Create consumer for both OTEL topics
        topics = [settings.kafka_otel_spans_topic, settings.kafka_otel_metrics_topic]

        consumer = KafkaEventConsumer(
            topics=topics,
            group_id="otel-consumer-group",
            message_handler=self.process_otel_message,
            auto_offset_reset="latest",
        )

        try:
            # Start background thread for periodic time-based flushing
            flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
            flush_thread.start()
            logger.info("Started periodic flush thread")

            # Start the consumer (this blocks)
            consumer.start()

        except KeyboardInterrupt:
            logger.info("Shutting down OTEL consumer")

            # Signal shutdown to background thread
            self.shutdown_event.set()

            # Flush any remaining batches
            self._flush_spans()
            self._flush_metrics()

            # Close ClickHouse connection
            self.clickhouse.close()

        except Exception as e:
            logger.exception("OTEL consumer error", error=str(e))
            raise


def main() -> None:
    """Main entry point for the OTEL consumer."""
    consumer = OTelConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
