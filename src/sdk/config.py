"""Configuration management for Data Lineage Hub SDK."""

from pydantic import Field
from pydantic_settings import BaseSettings


class LineageHubConfig(BaseSettings):
    """Configuration settings for Data Lineage Hub SDK."""

    # Connection settings
    hub_endpoint: str = Field(
        default="http://localhost:8000",
        description="Data Lineage Hub service endpoint URL",
    )
    api_key: str | None = Field(
        default=None, description="API key for authentication (required in production)"
    )
    namespace: str = Field(
        default="default", description="Namespace for multi-tenant isolation"
    )

    # HTTP client settings
    timeout: int = Field(default=30, description="HTTP request timeout in seconds")
    retry_attempts: int = Field(
        default=3, description="Number of retry attempts for failed requests"
    )
    retry_delay: float = Field(
        default=1.0, description="Initial delay between retries in seconds"
    )

    # Feature flags
    enable_telemetry: bool = Field(
        default=True, description="Enable OpenTelemetry instrumentation"
    )
    enable_lineage: bool = Field(
        default=True, description="Enable OpenLineage event tracking"
    )
    auto_instrument: bool = Field(
        default=True, description="Automatically instrument common libraries"
    )

    # Batching settings
    batch_size: int = Field(
        default=100, description="Maximum number of events to batch together"
    )
    flush_interval: float = Field(
        default=5.0, description="Interval to flush batched events in seconds"
    )

    # Development settings
    debug: bool = Field(default=False, description="Enable debug logging and features")
    dry_run: bool = Field(
        default=False, description="Enable dry-run mode (log events instead of sending)"
    )

    class Config:
        """Pydantic configuration."""

        env_prefix = "LINEAGE_HUB_"
        case_sensitive = False


# Global configuration instance
_config: LineageHubConfig | None = None


def get_config() -> LineageHubConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = LineageHubConfig()
    return _config


def configure(
    hub_endpoint: str | None = None,
    api_key: str | None = None,
    namespace: str | None = None,
    timeout: int | None = None,
    retry_attempts: int | None = None,
    enable_telemetry: bool | None = None,
    enable_lineage: bool | None = None,
    debug: bool | None = None,
    **kwargs,
) -> LineageHubConfig:
    """
    Configure the Data Lineage Hub SDK globally.

    Args:
        hub_endpoint: Data Lineage Hub service endpoint URL
        api_key: API key for authentication
        namespace: Namespace for multi-tenant isolation
        timeout: HTTP request timeout in seconds
        retry_attempts: Number of retry attempts for failed requests
        enable_telemetry: Enable OpenTelemetry instrumentation
        enable_lineage: Enable OpenLineage event tracking
        debug: Enable debug logging and features
        **kwargs: Additional configuration options

    Returns:
        Configured LineageHubConfig instance
    """
    global _config

    # Build configuration dict from parameters
    config_dict = {}
    if hub_endpoint is not None:
        config_dict["hub_endpoint"] = hub_endpoint
    if api_key is not None:
        config_dict["api_key"] = api_key
    if namespace is not None:
        config_dict["namespace"] = namespace
    if timeout is not None:
        config_dict["timeout"] = timeout
    if retry_attempts is not None:
        config_dict["retry_attempts"] = retry_attempts
    if enable_telemetry is not None:
        config_dict["enable_telemetry"] = enable_telemetry
    if enable_lineage is not None:
        config_dict["enable_lineage"] = enable_lineage
    if debug is not None:
        config_dict["debug"] = debug

    # Add any additional kwargs
    config_dict.update(kwargs)

    # Create new config with merged settings
    if _config is None:
        _config = LineageHubConfig(**config_dict)
    else:
        # Update existing config
        for key, value in config_dict.items():
            if hasattr(_config, key):
                setattr(_config, key, value)

    return _config


def reset_config() -> None:
    """Reset configuration to default values (mainly for testing)."""
    global _config
    _config = None
