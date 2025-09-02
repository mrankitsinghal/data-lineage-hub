"""Namespace management service for multi-tenant support."""

import re
from datetime import datetime
from typing import Dict, List, Optional

import structlog

from src.api.models import NamespaceConfig, NamespaceCreateRequest
from src.config import settings

logger = structlog.get_logger(__name__)


class NamespaceService:
    """Service for managing multi-tenant namespaces."""
    
    def __init__(self):
        """Initialize namespace service with in-memory storage."""
        self._namespaces: Dict[str, NamespaceConfig] = {}
        self._initialize_default_namespace()
    
    def _initialize_default_namespace(self) -> None:
        """Create default namespace for demo purposes."""
        default_config = NamespaceConfig(
            name=settings.default_namespace,
            display_name="Demo Pipeline",
            description="Default namespace for demonstration purposes",
            owners=["demo@data-lineage-hub.com"],
            viewers=["public@data-lineage-hub.com"],
            daily_event_quota=settings.namespace_quota_events_per_day,
            storage_retention_days=settings.namespace_retention_days,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            tags={"type": "demo", "environment": "development"}
        )
        self._namespaces[settings.default_namespace] = default_config
        logger.info("Initialized default namespace", namespace=settings.default_namespace)
    
    def create_namespace(self, request: NamespaceCreateRequest) -> NamespaceConfig:
        """Create a new namespace."""
        # Validate namespace name format
        if not self._is_valid_namespace_name(request.name):
            raise ValueError(
                f"Invalid namespace name: {request.name}. "
                "Must be 3-50 characters, lowercase alphanumeric with dashes"
            )
        
        # Check if namespace already exists
        if request.name in self._namespaces:
            raise ValueError(f"Namespace '{request.name}' already exists")
        
        # Create namespace configuration
        config = NamespaceConfig(
            name=request.name,
            display_name=request.display_name,
            description=request.description,
            owners=request.owners,
            viewers=[],  # Empty initially
            daily_event_quota=request.daily_event_quota,
            storage_retention_days=settings.namespace_retention_days,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            tags=request.tags
        )
        
        self._namespaces[request.name] = config
        
        logger.info(
            "Created namespace",
            namespace=request.name,
            display_name=request.display_name,
            owners=request.owners
        )
        
        return config
    
    def get_namespace(self, name: str) -> Optional[NamespaceConfig]:
        """Get namespace configuration by name."""
        return self._namespaces.get(name)
    
    def list_namespaces(self, user_email: Optional[str] = None) -> List[NamespaceConfig]:
        """List all namespaces (with optional user filtering)."""
        if not settings.require_namespace_permissions or user_email is None:
            # Return all namespaces if permissions not required
            return list(self._namespaces.values())
        
        # Filter namespaces based on user permissions
        accessible_namespaces = []
        for config in self._namespaces.values():
            if (user_email in config.owners or 
                user_email in config.viewers or
                settings.enable_cross_namespace_discovery):
                accessible_namespaces.append(config)
        
        return accessible_namespaces
    
    def update_namespace(self, name: str, updates: dict) -> Optional[NamespaceConfig]:
        """Update namespace configuration."""
        if name not in self._namespaces:
            return None
        
        config = self._namespaces[name]
        
        # Update allowed fields
        allowed_updates = {
            'display_name', 'description', 'owners', 'viewers',
            'daily_event_quota', 'storage_retention_days', 'tags'
        }
        
        for key, value in updates.items():
            if key in allowed_updates and hasattr(config, key):
                setattr(config, key, value)
        
        config.updated_at = datetime.utcnow()
        
        logger.info("Updated namespace", namespace=name, updates=list(updates.keys()))
        
        return config
    
    def validate_namespace_access(
        self, 
        namespace: str, 
        user_email: Optional[str] = None,
        require_owner: bool = False
    ) -> bool:
        """Validate if user has access to namespace."""
        if not settings.namespace_isolation_enabled:
            return True
        
        config = self.get_namespace(namespace)
        if not config:
            return settings.auto_create_namespaces
        
        if not settings.api_key_validation or user_email is None:
            return True  # Allow access if auth disabled
        
        if require_owner:
            return user_email in config.owners
        
        return (user_email in config.owners or 
                user_email in config.viewers)
    
    def auto_create_namespace_if_needed(self, namespace: str) -> bool:
        """Auto-create namespace if it doesn't exist and auto-creation is enabled."""
        if namespace in self._namespaces:
            return True
        
        if not settings.auto_create_namespaces:
            return False
        
        # Auto-create with default settings
        try:
            auto_request = NamespaceCreateRequest(
                name=namespace,
                display_name=f"Auto-created: {namespace}",
                description=f"Automatically created namespace for {namespace}",
                owners=[f"admin@{namespace}.com"],
                daily_event_quota=settings.namespace_quota_events_per_day,
                tags={"auto_created": "true", "environment": "development"}
            )
            
            self.create_namespace(auto_request)
            return True
            
        except Exception as e:
            logger.error(
                "Failed to auto-create namespace",
                namespace=namespace,
                error=str(e)
            )
            return False
    
    def check_event_quota(self, namespace: str, event_count: int) -> bool:
        """Check if namespace has remaining event quota for today."""
        # TODO: Implement quota tracking with Redis
        # For now, always allow (demo mode)
        config = self.get_namespace(namespace)
        if not config:
            return False
        
        # Simple check - in production would track daily usage
        return event_count <= 1000  # Batch limit
    
    def _is_valid_namespace_name(self, name: str) -> bool:
        """Validate namespace name format."""
        # Must be 3-50 characters, lowercase alphanumeric with dashes
        pattern = r'^[a-z0-9][a-z0-9-]{1,48}[a-z0-9]$'
        return bool(re.match(pattern, name))


# Global namespace service instance
namespace_service = NamespaceService()