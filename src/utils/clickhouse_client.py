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

    def insert_spans_batch(self, spans: list[tuple]) -> None:
        """
        Insert a batch of OpenTelemetry spans into ClickHouse.

        Args:
            spans: List of span tuples to insert
        """
        if not spans:
            return

        try:
            self.client.execute(
                """
                INSERT INTO otel.traces
                (timestamp, trace_id, span_id, parent_span_id, operation_name,
                 service_name, duration_ns, status_code, span_kind, attributes,
                 resource_attributes, events)
                VALUES
                """,
                spans,
            )
            logger.info("Inserted span batch to ClickHouse", count=len(spans))

        except Exception as e:
            logger.exception("Failed to insert spans to ClickHouse", error=str(e))
            raise

    def insert_metrics_batch(self, metrics: list[tuple]) -> None:
        """
        Insert a batch of OpenTelemetry metrics into ClickHouse.

        Args:
            metrics: List of metric tuples to insert
        """
        if not metrics:
            return

        try:
            self.client.execute(
                """
                INSERT INTO otel.metrics
                (timestamp, metric_name, metric_type, value, unit, service_name,
                 attributes, resource_attributes)
                VALUES
                """,
                metrics,
            )
            logger.info("Inserted metrics batch to ClickHouse", count=len(metrics))

        except Exception as e:
            logger.exception("Failed to insert metrics to ClickHouse", error=str(e))
            raise

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
