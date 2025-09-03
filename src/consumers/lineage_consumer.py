"""OpenLineage consumer that forwards events to Marquez."""

import asyncio
from typing import Any

import httpx
import structlog
from confluent_kafka import Message

from src.config import settings
from src.utils.kafka_client import KafkaEventConsumer
from src.utils.logging_config import configure_logging


logger = structlog.get_logger(__name__)


class LineageConsumer:
    """Kafka consumer for OpenLineage events that forwards them to Marquez."""

    def __init__(self) -> None:
        self.http_client = httpx.AsyncClient(
            base_url=settings.marquez_url, timeout=httpx.Timeout(30.0)
        )
        self.marquez_endpoint = "/api/v1/lineage"

        # Initialize Kafka consumer with message handler
        self.kafka_consumer = KafkaEventConsumer(
            topics=[settings.kafka_openlineage_topic],
            group_id="lineage-consumer-group",
            message_handler=self._handle_kafka_message,
        )

    def start(self) -> None:
        """Start consuming OpenLineage events."""
        logger.info(
            "Starting OpenLineage consumer",
            topic=settings.kafka_openlineage_topic,
            marquez_url=settings.marquez_url,
        )

        try:
            # Start the Kafka consumer
            self.kafka_consumer.start()
        finally:
            self._cleanup()

    def _handle_kafka_message(self, msg: Any) -> None:
        """Handle a Kafka message (called by KafkaEventConsumer)."""
        # Process the message asynchronously with smart event loop detection
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._process_message(msg))
        except RuntimeError:
            # No event loop running, create one
            asyncio.run(self._process_message(msg))

    async def _process_message(self, message: Message) -> None:
        """Process a single OpenLineage event message with namespace support."""
        try:
            # Deserialize the message using the centralized method
            event_data, run_id = self.kafka_consumer.deserialize_message(message)

            # Extract namespace from message headers or fallback to event data
            namespace = self._extract_namespace(message, event_data)

            logger.debug(
                "Processing OpenLineage event",
                run_id=run_id,
                event_type=event_data.get("eventType"),
                job_name=event_data.get("job", {}).get("name"),
                namespace=namespace,
                partition=message.partition(),
                offset=message.offset(),
            )

            # Forward to Marquez with namespace context
            success = await self._forward_to_marquez(event_data, namespace)

            if success:
                logger.info(
                    "Successfully forwarded event to Marquez",
                    run_id=run_id,
                    event_type=event_data.get("eventType"),
                    namespace=namespace,
                )
            else:
                logger.error(
                    "Failed to forward event to Marquez",
                    run_id=run_id,
                    event_type=event_data.get("eventType"),
                    namespace=namespace,
                )

        except Exception as e:
            logger.exception(
                "Error processing OpenLineage message",
                error=str(e),
                message_offset=message.offset(),
                message_partition=message.partition(),
            )

    def _extract_namespace(
        self, message: Message, event_data: dict[str, Any]
    ) -> str | None:
        """Extract namespace from Kafka message headers or event data."""
        # Try to get namespace from message headers first (preferred)
        if hasattr(message, "headers") and message.headers():
            for key, value in message.headers():
                if key == "namespace" and value:
                    return value.decode("utf-8")

        # Fallback to event data job namespace
        if "job" in event_data and "namespace" in event_data["job"]:
            return event_data["job"]["namespace"]

        # Default namespace for backward compatibility
        return settings.default_namespace

    async def _forward_to_marquez(
        self, event_data: dict[str, Any], namespace: str | None = None
    ) -> bool:
        """Forward OpenLineage event to Marquez with namespace context."""
        try:
            # Add namespace context to headers for better tracking
            headers = {"Content-Type": "application/json"}
            if namespace:
                headers["X-Namespace"] = namespace

            # Ensure event has namespace in job metadata for Marquez
            if namespace and "job" in event_data:
                if "namespace" not in event_data["job"]:
                    event_data["job"]["namespace"] = namespace

            response = await self.http_client.post(
                self.marquez_endpoint,
                json=event_data,
                headers=headers,
            )

            if response.status_code == 201:
                logger.debug(
                    "Event successfully sent to Marquez",
                    namespace=namespace,
                    job_name=event_data.get("job", {}).get("name"),
                )
                return True
            logger.warning(
                "Marquez returned non-201 status",
                status_code=response.status_code,
                response_text=response.text,
                namespace=namespace,
            )
            return False

        except httpx.RequestError as e:
            logger.exception("HTTP request to Marquez failed", error=str(e))
            return False
        except Exception as e:
            logger.exception("Unexpected error forwarding to Marquez", error=str(e))
            return False

    def _cleanup(self) -> None:
        """Clean up resources."""
        # Stop the Kafka consumer
        self.kafka_consumer.stop()

        # Close HTTP client with smart event loop detection
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.http_client.aclose())
        except RuntimeError:
            # No event loop running, create one
            asyncio.run(self.http_client.aclose())
        logger.info("HTTP client closed")


def main() -> None:
    """Main entry point for the lineage consumer."""
    configure_logging("lineage-consumer")

    consumer = LineageConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
