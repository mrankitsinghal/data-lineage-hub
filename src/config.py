"""Configuration settings for the data lineage POC."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""

    # API Configuration
    app_name: str = "Data Lineage Hub POC"
    app_version: str = "1.0.0"
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True

    # OpenLineage Configuration
    openlineage_namespace: str = "poc-pipeline"
    openlineage_producer: str = "data-lineage-hub"

    # Kafka Configuration
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_openlineage_topic: str = "openlineage-events"
    kafka_otel_spans_topic: str = "otel-spans"
    kafka_otel_metrics_topic: str = "otel-metrics"

    # Marquez Configuration
    marquez_url: str = "http://localhost:5000"

    # ClickHouse Configuration
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    clickhouse_database: str = "otel"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # OpenTelemetry Configuration
    otel_service_name: str = "data-pipeline-service"
    otel_service_version: str = "1.0.0"
    otel_collector_endpoint: str = "http://localhost:4317"

    # Data Configuration
    sample_data_path: str = "data"

    # Redis Configuration
    redis_url: str = "redis://localhost:6379"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
