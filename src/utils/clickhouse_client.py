"""ClickHouse client utilities for database operations."""

from typing import Any

import structlog
from clickhouse_driver import Client

from src.config import settings


logger = structlog.get_logger(__name__)


class ClickHouseClient:
    """Centralized ClickHouse client for database operations."""

    def __init__(self) -> None:
        """Initialize ClickHouse client."""
        self.client = None
        self._connect()

    def _connect(self) -> None:
        """Connect to ClickHouse database."""
        try:
            self.client = Client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_database,
                user=settings.clickhouse_user,
                password=settings.clickhouse_password,
                connect_timeout=30,
                send_receive_timeout=30,
            )

            # Test connection
            self.client.execute("SELECT 1")
            logger.info(
                "Connected to ClickHouse",
                host=settings.clickhouse_host,
                database=settings.clickhouse_database,
            )

        except Exception as e:
            logger.exception("Failed to connect to ClickHouse", error=str(e))
            raise

    def execute(self, query: str, params: Any = None) -> Any:
        """
        Execute a ClickHouse query.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query result
        """
        try:
            if params:
                return self.client.execute(query, params)
            return self.client.execute(query)
        except Exception as e:
            logger.exception(
                "Failed to execute ClickHouse query",
                error=str(e),
                query=query[:100] + "..." if len(query) > 100 else query,
            )
            raise

    def insert_otel_spans(self, spans: list[dict]) -> bool:
        """
        Insert a batch of OpenTelemetry spans into ClickHouse.

        Args:
            spans: List of span dictionaries to insert

        Returns:
            True if successful, False otherwise
        """
        if not spans:
            return True

        try:
            # Convert dict format to tuple format expected by ClickHouse
            span_tuples = [
                (
                    span["timestamp"],
                    span["trace_id"],
                    span["span_id"],
                    span["parent_span_id"],
                    span["operation_name"],
                    span["service_name"],
                    span["duration_ns"],
                    span["status_code"],
                    span["span_kind"],
                    span["namespace"],
                    span["attributes"],
                    span["resource_attributes"],
                    span["events"],
                )
                for span in spans
            ]

            self.client.execute(
                """
                INSERT INTO otel.traces
                (timestamp, trace_id, span_id, parent_span_id, operation_name,
                 service_name, duration_ns, status_code, span_kind, namespace,
                 attributes, resource_attributes, events)
                VALUES
                """,
                span_tuples,
            )
            logger.info("Inserted span batch to ClickHouse", count=len(spans))
            return True

        except Exception as e:
            logger.exception("Failed to insert spans to ClickHouse", error=str(e))
            return False

    def insert_otel_metrics(self, metrics: list[dict]) -> bool:
        """
        Insert a batch of OpenTelemetry metrics into ClickHouse.

        Args:
            metrics: List of metric dictionaries to insert

        Returns:
            True if successful, False otherwise
        """
        if not metrics:
            return True

        try:
            # Convert dict format to tuple format expected by ClickHouse
            metric_tuples = [
                (
                    metric["timestamp"],
                    metric["metric_name"],
                    metric["metric_type"],
                    metric["value"],
                    metric["unit"],
                    metric["service_name"],
                    metric["namespace"],
                    metric["attributes"],
                    metric["resource_attributes"],
                )
                for metric in metrics
            ]

            self.client.execute(
                """
                INSERT INTO otel.metrics
                (timestamp, metric_name, metric_type, value, unit, service_name,
                 namespace, attributes, resource_attributes)
                VALUES
                """,
                metric_tuples,
            )
            logger.info("Inserted metrics batch to ClickHouse", count=len(metrics))
            return True

        except Exception as e:
            logger.exception("Failed to insert metrics to ClickHouse", error=str(e))
            return False

    def close(self) -> None:
        """Close the ClickHouse connection."""
        self.disconnect()

    def disconnect(self) -> None:
        """Disconnect from ClickHouse."""
        if self.client:
            self.client.disconnect()
            logger.info("ClickHouse client disconnected")

    def is_connected(self) -> bool:
        """
        Check if the client is connected to ClickHouse.

        Returns:
            True if connected, False otherwise
        """
        try:
            if self.client:
                self.client.execute("SELECT 1")
                return True
        except Exception:
            pass
        return False


# Global client instance
_clickhouse_client: ClickHouseClient | None = None


def get_clickhouse_client() -> ClickHouseClient:
    """Get or create global ClickHouse client instance."""
    global _clickhouse_client
    if _clickhouse_client is None:
        _clickhouse_client = ClickHouseClient()
    return _clickhouse_client
