"""Kafka client utilities for publishing events."""

import json
from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from src.config import settings


logger = structlog.get_logger(__name__)


class KafkaEventPublisher:
    """Kafka client for publishing events to various topics."""

    def __init__(self) -> None:
        self.producer = None
        self._connect()

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        """Callback for message delivery reports."""
        if err:
            logger.error(
                "Message delivery failed",
                error=str(err),
                topic=msg.topic() if msg else None,
            )
        else:
            logger.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def _connect(self) -> None:
        """Connect to Kafka cluster."""
        try:
            config = {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 300,
                "request.timeout.ms": 30000,
                "delivery.timeout.ms": 60000,
            }
            self.producer = Producer(config)
            logger.info("Connected to Kafka", servers=settings.kafka_bootstrap_servers)
        except Exception as e:
            logger.exception("Failed to connect to Kafka", error=str(e))
            raise

    def publish_openlineage_event(
        self, event: dict[str, Any], run_id: str | None = None, namespace: str | None = None
    ) -> bool:
        """Publish OpenLineage event to Kafka with namespace support."""
        try:
            # Add namespace metadata to event for backward compatibility
            if namespace and "job" in event:
                if "namespace" not in event["job"]:
                    event["job"]["namespace"] = namespace

            # Serialize the event data
            value = json.dumps(event).encode("utf-8")
            
            # Create namespace-aware message key: namespace:run_id or namespace:random
            if namespace:
                key_parts = [namespace]
                if run_id:
                    key_parts.append(run_id)
                key = ":".join(key_parts).encode("utf-8")
            else:
                key = run_id.encode("utf-8") if run_id else None

            # Add namespace as header for consumer routing
            headers = {}
            if namespace:
                headers["namespace"] = namespace.encode("utf-8")
                headers["event_type"] = "openlineage".encode("utf-8")
            
            # Produce message asynchronously
            self.producer.produce(
                topic=settings.kafka_openlineage_topic,
                value=value,
                key=key,
                headers=headers if headers else None,
                callback=self._delivery_callback,
            )

            # Flush to ensure message is sent
            self.producer.flush(timeout=10)

            logger.info(
                "Published OpenLineage event",
                topic=settings.kafka_openlineage_topic,
                run_id=run_id,
                namespace=namespace,
            )
            return True
        except (KafkaError, KafkaException) as e:
            logger.exception(
                "Failed to publish OpenLineage event", 
                error=str(e), 
                run_id=run_id, 
                namespace=namespace
            )
            return False

    def publish_otel_span(
        self, span_data: dict[str, Any], trace_id: str | None = None, namespace: str | None = None
    ) -> bool:
        """Publish OpenTelemetry span to Kafka with namespace support."""
        try:
            # Add namespace to span attributes if not already present
            if namespace:
                if "resource" not in span_data:
                    span_data["resource"] = {}
                if "attributes" not in span_data["resource"]:
                    span_data["resource"]["attributes"] = {}
                if "service.namespace" not in span_data["resource"]["attributes"]:
                    span_data["resource"]["attributes"]["service.namespace"] = namespace

            # Serialize the span data
            value = json.dumps(span_data).encode("utf-8")
            
            # Create namespace-aware message key
            if namespace:
                key_parts = [namespace]
                if trace_id:
                    key_parts.append(trace_id)
                key = ":".join(key_parts).encode("utf-8")
            else:
                key = trace_id.encode("utf-8") if trace_id else None

            # Add namespace as header for consumer routing
            headers = {}
            if namespace:
                headers["namespace"] = namespace.encode("utf-8")
                headers["event_type"] = "otel_span".encode("utf-8")

            # Produce message asynchronously
            self.producer.produce(
                topic=settings.kafka_otel_spans_topic,
                value=value,
                key=key,
                headers=headers if headers else None,
                callback=self._delivery_callback,
            )

            # Flush to ensure message is sent
            self.producer.flush(timeout=10)

            logger.debug(
                "Published OTEL span",
                topic=settings.kafka_otel_spans_topic,
                trace_id=trace_id,
                namespace=namespace,
            )
            return True
        except (KafkaError, KafkaException) as e:
            logger.exception(
                "Failed to publish OTEL span", 
                error=str(e), 
                trace_id=trace_id, 
                namespace=namespace
            )
            return False

    def publish_otel_metric(
        self, metric_data: dict[str, Any], service_name: str | None = None, namespace: str | None = None
    ) -> bool:
        """Publish OpenTelemetry metric to Kafka with namespace support."""
        try:
            # Add namespace to metric attributes if not already present
            if namespace:
                if "resource" not in metric_data:
                    metric_data["resource"] = {}
                if "attributes" not in metric_data["resource"]:
                    metric_data["resource"]["attributes"] = {}
                if "service.namespace" not in metric_data["resource"]["attributes"]:
                    metric_data["resource"]["attributes"]["service.namespace"] = namespace

            # Serialize the metric data
            value = json.dumps(metric_data).encode("utf-8")
            
            # Create namespace-aware message key
            if namespace:
                key_parts = [namespace]
                if service_name:
                    key_parts.append(service_name)
                key = ":".join(key_parts).encode("utf-8")
            else:
                key = service_name.encode("utf-8") if service_name else None

            # Add namespace as header for consumer routing
            headers = {}
            if namespace:
                headers["namespace"] = namespace.encode("utf-8")
                headers["event_type"] = "otel_metric".encode("utf-8")

            # Produce message asynchronously
            self.producer.produce(
                topic=settings.kafka_otel_metrics_topic,
                value=value,
                key=key,
                headers=headers if headers else None,
                callback=self._delivery_callback,
            )

            # Flush to ensure message is sent
            self.producer.flush(timeout=10)

            logger.debug(
                "Published OTEL metric",
                topic=settings.kafka_otel_metrics_topic,
                service=service_name,
                namespace=namespace,
            )
            return True
        except (KafkaError, KafkaException) as e:
            logger.exception(
                "Failed to publish OTEL metric", 
                error=str(e), 
                service=service_name, 
                namespace=namespace
            )
            return False

    def close(self) -> None:
        """Close the Kafka producer."""
        if self.producer:
            # Flush any remaining messages
            self.producer.flush(timeout=30)
            logger.info("Kafka producer closed")


# Global publisher instance
_publisher: KafkaEventPublisher | None = None


def get_kafka_publisher() -> KafkaEventPublisher:
    """Get or create global Kafka publisher instance."""
    global _publisher
    if _publisher is None:
        _publisher = KafkaEventPublisher()
    return _publisher


class KafkaEventConsumer:
    """Generic Kafka consumer for processing messages from topics."""

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        message_handler: Callable[[Any], None],
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
    ) -> None:
        """
        Initialize Kafka consumer.

        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            message_handler: Function to handle each message
            auto_offset_reset: Where to start reading when no offset exists
            enable_auto_commit: Whether to auto-commit offsets
            auto_commit_interval_ms: Auto-commit interval in milliseconds
        """
        self.topics = topics
        self.group_id = group_id
        self.message_handler = message_handler
        self.consumer = None
        self.running = False

        # Consumer configuration
        self.config = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
            "auto.commit.interval.ms": auto_commit_interval_ms,
        }

    def start(self) -> None:
        """Start consuming messages from Kafka topics."""
        logger.info(
            "Starting Kafka consumer",
            topics=self.topics,
            group_id=self.group_id,
        )

        try:
            self.consumer = Consumer(self.config)
            self.consumer.subscribe(self.topics)
            self.running = True

            logger.info("Kafka consumer initialized", topics=self.topics)

            # Start consuming messages
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.debug("Reached end of partition")
                        continue
                    logger.error("Consumer error", error=str(msg.error()))
                    break

                # Process the message
                try:
                    self.message_handler(msg)
                except Exception as e:
                    logger.exception(
                        "Error in message handler",
                        error=str(e),
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except (KafkaError, KafkaException) as e:
            logger.exception("Consumer error", error=str(e))
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the consumer and clean up resources."""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

    def deserialize_message(self, msg: Any) -> tuple[dict[str, Any], str | None]:
        """
        Deserialize a Kafka message.

        Args:
            msg: Kafka message

        Returns:
            Tuple of (message_data, key)
        """
        message_data = json.loads(msg.value().decode("utf-8"))
        key = msg.key().decode("utf-8") if msg.key() else None
        return message_data, key
