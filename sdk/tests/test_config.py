"""Tests for configuration management."""

import os
from unittest.mock import patch

import pytest

from data_lineage_hub_sdk.config import LineageHubConfig, configure, get_config, reset_config


@pytest.fixture(autouse=True)
def reset_global_config():
    """Reset global config before each test."""
    reset_config()
    yield
    reset_config()


def test_default_config():
    """Test default configuration values."""
    config = LineageHubConfig()
    
    assert config.hub_endpoint == "http://localhost:8000"
    assert config.api_key is None
    assert config.namespace == "default"
    assert config.timeout == 30
    assert config.retry_attempts == 3
    assert config.enable_telemetry is True
    assert config.enable_lineage is True
    assert config.debug is False


def test_config_from_env():
    """Test configuration from environment variables."""
    env_vars = {
        "LINEAGE_HUB_HUB_ENDPOINT": "https://lineage-hub.company.com",
        "LINEAGE_HUB_API_KEY": "test-api-key-123",
        "LINEAGE_HUB_NAMESPACE": "test-namespace",
        "LINEAGE_HUB_TIMEOUT": "60",
        "LINEAGE_HUB_RETRY_ATTEMPTS": "5",
        "LINEAGE_HUB_ENABLE_TELEMETRY": "false",
        "LINEAGE_HUB_DEBUG": "true",
    }
    
    with patch.dict(os.environ, env_vars):
        config = LineageHubConfig()
        
        assert config.hub_endpoint == "https://lineage-hub.company.com"
        assert config.api_key == "test-api-key-123"
        assert config.namespace == "test-namespace"
        assert config.timeout == 60
        assert config.retry_attempts == 5
        assert config.enable_telemetry is False
        assert config.debug is True


def test_get_config_singleton():
    """Test that get_config returns the same instance."""
    config1 = get_config()
    config2 = get_config()
    
    assert config1 is config2


def test_configure_function():
    """Test the configure function."""
    config = configure(
        hub_endpoint="https://test-hub.com",
        api_key="test-key",
        namespace="test-ns",
        timeout=45,
        debug=True,
    )
    
    assert config.hub_endpoint == "https://test-hub.com"
    assert config.api_key == "test-key"
    assert config.namespace == "test-ns"
    assert config.timeout == 45
    assert config.debug is True
    
    # Verify it's also the global config
    global_config = get_config()
    assert global_config is config


def test_configure_partial_update():
    """Test that configure updates only specified fields."""
    # Set initial config
    initial_config = configure(
        hub_endpoint="https://initial.com",
        api_key="initial-key",
        namespace="initial-ns",
    )
    
    # Update only some fields
    updated_config = configure(
        api_key="updated-key",
        timeout=120,
    )
    
    # Check that new values are set and old values preserved
    assert updated_config.hub_endpoint == "https://initial.com"  # Preserved
    assert updated_config.api_key == "updated-key"  # Updated
    assert updated_config.namespace == "initial-ns"  # Preserved
    assert updated_config.timeout == 120  # Updated


def test_reset_config():
    """Test configuration reset."""
    # Set some config
    configure(hub_endpoint="https://test.com")
    config1 = get_config()
    assert config1.hub_endpoint == "https://test.com"
    
    # Reset and get new config
    reset_config()
    config2 = get_config()
    
    # Should be different instance with default values
    assert config2 is not config1
    assert config2.hub_endpoint == "http://localhost:8000"


@pytest.mark.parametrize("field,value,expected", [
    ("batch_size", 50, 50),
    ("flush_interval", 10.5, 10.5),
    ("dry_run", True, True),
    ("auto_instrument", False, False),
])
def test_config_field_types(field, value, expected):
    """Test various config field types."""
    kwargs = {field: value}
    config = configure(**kwargs)
    
    assert getattr(config, field) == expected


def test_invalid_config():
    """Test configuration with invalid values."""
    with pytest.raises(ValueError):
        # Negative timeout should fail validation
        LineageHubConfig(timeout=-1)