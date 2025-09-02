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
    otel_service_name: str = "data-lineage-hub-service"
    otel_service_version: str = "1.0.0"
    otel_collector_endpoint: str = "http://localhost:4317"

    # Multi-Tenant Namespace Configuration
    default_namespace: str = "demo-pipeline"
    namespace_isolation_enabled: bool = True
    auto_create_namespaces: bool = True
    namespace_quota_events_per_day: int = 100000
    namespace_retention_days: int = 30

    # Organization-wide Service Configuration  
    hub_service_name: str = "Data Lineage Hub"
    enable_external_ingestion: bool = True
    api_key_validation: bool = False  # Set to true in production

    # SDK Client Configuration (for teams using this service)
    lineage_hub_endpoint: str = "http://localhost:8000"
    lineage_hub_api_key: str = "demo-api-key"

    # Enterprise Features
    rate_limiting_enabled: bool = True
    rate_limit_per_namespace: int = 1000  # requests per minute per namespace
    audit_logging_enabled: bool = True

    # Cross-Namespace Features
    enable_cross_namespace_discovery: bool = True
    require_namespace_permissions: bool = False  # Set to true in production

    # Monitoring & Alerting
    health_check_dependencies: bool = True
    slack_webhook_url: str = ""  # Optional: for service alerts
    metrics_export_interval: int = 30  # seconds

    # Data Configuration
    sample_data_path: str = "data"

    # Redis Configuration (for namespace caching and rate limiting)
    redis_url: str = "redis://localhost:6379"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
