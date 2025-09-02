"""OpenLineage consumer that forwards events to Marquez."""

import asyncio
from typing import Any

import httpx
import structlog

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
        # Process the message asynchronously
        asyncio.run(self._process_message(msg))

    async def _process_message(self, message) -> None:
        """Process a single OpenLineage event message."""
        try:
            # Deserialize the message using the centralized method
            event_data, run_id = self.kafka_consumer.deserialize_message(message)

            logger.debug(
                "Processing OpenLineage event",
                run_id=run_id,
                event_type=event_data.get("eventType"),
                job_name=event_data.get("job", {}).get("name"),
                partition=message.partition(),
                offset=message.offset(),
            )

            # Forward to Marquez
            success = await self._forward_to_marquez(event_data)

            if success:
                logger.info(
                    "Successfully forwarded event to Marquez",
                    run_id=run_id,
                    event_type=event_data.get("eventType"),
                )
            else:
                logger.error(
                    "Failed to forward event to Marquez",
                    run_id=run_id,
                    event_type=event_data.get("eventType"),
                )

        except Exception as e:
            logger.exception(
                "Error processing OpenLineage message",
                error=str(e),
                message_offset=message.offset(),
                message_partition=message.partition(),
            )

    async def _forward_to_marquez(self, event_data: dict[str, Any]) -> bool:
        """Forward OpenLineage event to Marquez."""
        try:
            response = await self.http_client.post(
                self.marquez_endpoint,
                json=event_data,
                headers={"Content-Type": "application/json"},
            )

            if response.status_code == 201:
                logger.debug("Event successfully sent to Marquez")
                return True
            logger.warning(
                "Marquez returned non-201 status",
                status_code=response.status_code,
                response_text=response.text,
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

        # Close HTTP client
        asyncio.run(self.http_client.aclose())
        logger.info("HTTP client closed")


def main() -> None:
    """Main entry point for the lineage consumer."""
    configure_logging("lineage-consumer")

    consumer = LineageConsumer()
    consumer.start()


if __name__ == "__main__":
    main()
