"""Data Lineage Hub SDK - Python client for Data Lineage Hub service."""

from .client import LineageHubClient, TelemetryClient
from .config import LineageHubConfig, configure
from .decorators import lineage_track, telemetry_track
from .models import LineageEvent, TelemetryData

__version__ = "1.0.0"
__author__ = "Data Platform Team"
__email__ = "data-platform@company.com"

__all__ = [
    "LineageHubClient",
    "TelemetryClient", 
    "LineageHubConfig",
    "configure",
    "lineage_track",
    "telemetry_track",
    "LineageEvent",
    "TelemetryData",
]