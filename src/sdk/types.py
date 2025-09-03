"""Data source types and format definitions for lineage tracking."""

from enum import Enum
from typing import Any

from pydantic import BaseModel


class AdapterType(str, Enum):
    """Predefined data source adapter types."""

    MYSQL = "mysql"
    POSTGRES = "postgres"
    CLICKHOUSE = "clickhouse"
    ICEBERG = "iceberg"
    S3 = "s3"
    FILE = "file"
    EVENT = "event"
    API = "api"
    DATAFRAME = "dataframe"


class DataFormat(str, Enum):
    """Common data formats."""

    TABLE = "table"
    JSON = "json"
    AVRO = "avro"
    PARQUET = "parquet"
    CSV = "csv"
    XML = "xml"
    YAML = "yaml"
    TEXT = "text"
    BINARY = "binary"


class DatasetSpec(BaseModel):
    """Specification for a dataset in lineage tracking."""

    type: AdapterType
    name: str
    format: DataFormat | None = None
    namespace: str | None = None

    class Config:
        use_enum_values = True

    def to_openlineage_dataset(self) -> dict[str, Any]:
        """Convert to OpenLineage dataset format."""
        dataset: dict[str, Any] = {
            "namespace": self.namespace or f"{self.type}://default",
            "name": self.name,
        }

        # Add facets for additional metadata
        facets: dict[str, Any] = {}

        # Data source facet
        facets["dataSource"] = {
            "_producer": "data-lineage-hub-sdk",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
            "name": self.namespace or f"{self.type}://default",
            "uri": f"{self.type}://{self.namespace or 'default'}",
        }

        # Add format information if provided
        if self.format:
            facets["format"] = {
                "_producer": "data-lineage-hub-sdk",
                "_schemaURL": "custom://format",
                "format": self.format,
                "adapter_type": self.type,
            }

        if facets:
            dataset["facets"] = facets

        return dataset


# Example specifications for each adapter type
ADAPTER_EXAMPLES: dict[AdapterType, list[dict[str, Any]]] = {
    AdapterType.MYSQL: [
        {
            "type": "mysql",
            "name": "users.user_profiles",
            "format": "table",
            "namespace": "prod-db",
        },
        {
            "type": "mysql",
            "name": "analytics.daily_metrics",
            "format": "table",
            "namespace": "analytics-db",
        },
    ],
    AdapterType.POSTGRES: [
        {
            "type": "postgres",
            "name": "public.orders",
            "format": "table",
            "namespace": "warehouse",
        },
        {
            "type": "postgres",
            "name": "staging.raw_events",
            "format": "table",
            "namespace": "staging-db",
        },
    ],
    AdapterType.CLICKHOUSE: [
        {
            "type": "clickhouse",
            "name": "events.user_clicks",
            "format": "table",
            "namespace": "analytics",
        },
        {
            "type": "clickhouse",
            "name": "metrics.daily_aggregates",
            "format": "table",
            "namespace": "reporting",
        },
    ],
    AdapterType.ICEBERG: [
        {
            "type": "iceberg",
            "name": "warehouse.dim_users",
            "format": "table",
            "namespace": "data-lake",
        },
        {
            "type": "iceberg",
            "name": "raw.transaction_log",
            "format": "table",
            "namespace": "transactional",
        },
    ],
    AdapterType.S3: [
        {
            "type": "s3",
            "name": "s3://data-lake/events/2024/01/",
            "format": "parquet",
            "namespace": "production",
        },
        {
            "type": "s3",
            "name": "s3://ml-datasets/features.avro",
            "format": "avro",
            "namespace": "ml-platform",
        },
        {
            "type": "s3",
            "name": "s3://reports/daily_summary.csv",
            "format": "csv",
            "namespace": "reporting",
        },
    ],
    AdapterType.FILE: [
        {
            "type": "file",
            "name": "/data/input/users.csv",
            "format": "csv",
            "namespace": "local",
        },
        {
            "type": "file",
            "name": "/tmp/processed_data.parquet",
            "format": "parquet",
            "namespace": "temp",
        },
        {
            "type": "file",
            "name": "/logs/application.log",
            "format": "text",
            "namespace": "logs",
        },
    ],
    AdapterType.EVENT: [
        {
            "type": "event",
            "name": "user-events",
            "format": "avro",
            "namespace": "kafka",
        },
        {
            "type": "event",
            "name": "order-notifications",
            "format": "json",
            "namespace": "sqs",
        },
        {
            "type": "event",
            "name": "system-metrics",
            "format": "json",
            "namespace": "kinesis",
        },
    ],
    AdapterType.API: [
        {
            "type": "api",
            "name": "https://api.company.com/v1/users",
            "format": "json",
            "namespace": "external",
        },
        {
            "type": "api",
            "name": "https://internal-api.company.com/features",
            "format": "json",
            "namespace": "internal",
        },
        {
            "type": "api",
            "name": "https://data-api.partner.com/feed",
            "format": "xml",
            "namespace": "partner",
        },
    ],
    AdapterType.DATAFRAME: [
        {
            "type": "dataframe",
            "name": "user_segments_df",
            "format": "table",
            "namespace": "memory",
        },
        {
            "type": "dataframe",
            "name": "ml_features_df",
            "format": "table",
            "namespace": "processing",
        },
        {
            "type": "dataframe",
            "name": "analysis_results_df",
            "format": "table",
            "namespace": "analytics",
        },
    ],
}


def get_adapter_examples(adapter_type: AdapterType) -> list[dict[str, Any]]:
    """Get example specifications for a given adapter type."""
    return ADAPTER_EXAMPLES.get(adapter_type, [])


def validate_dataset_spec(spec: dict[str, Any]) -> DatasetSpec:
    """Validate and convert dict to DatasetSpec."""
    return DatasetSpec(**spec)


def create_dataset_specs(specs: list[dict[str, Any]]) -> list[DatasetSpec]:
    """Convert list of dicts to validated DatasetSpec objects."""
    return [validate_dataset_spec(spec) for spec in specs]
