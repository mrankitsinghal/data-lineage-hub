"""Data Lineage Hub SDK - Python client for Data Lineage Hub service."""

from .client import LineageHubClient, TelemetryClient
from .config import LineageHubConfig, configure
from .decorators import lineage_track, telemetry_track
from .models import LineageEvent, TelemetryData
from .types import AdapterType, DataFormat, DatasetSpec


__version__ = "1.0.0"
__author__ = "Data Platform Team"
__email__ = "data-platform@company.com"

__all__ = [
    # Types for dataset specifications
    "AdapterType",
    "DataFormat",
    "DatasetSpec",
    # Data models
    "LineageEvent",
    # Core clients
    "LineageHubClient",
    # Configuration
    "LineageHubConfig",
    "TelemetryClient",
    "TelemetryData",
    "configure",
    # Decorators
    "lineage_track",
    "telemetry_track",
]
