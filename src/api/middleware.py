"""API middleware for authentication and authorization."""


import structlog
from fastapi import HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.config import settings
from src.services.namespace import namespace_service


logger = structlog.get_logger(__name__)

# Security scheme for API key authentication
security = HTTPBearer(auto_error=False)


class APIKeyValidator:
    """Validates API keys and extracts user information."""

    def __init__(self):
        """Initialize API key validator."""
        # In production, this would integrate with an identity provider
        # For demo, we use a simple mapping
        self._api_keys = {
            "demo-api-key": "demo@data-lineage-hub.com",
            "team-data-platform-key": "admin@team-data-platform.com",
            "team-ml-platform-key": "admin@team-ml-platform.com",
            "enterprise-admin-key": "admin@enterprise.com",
        }

    def validate_api_key(self, api_key: str) -> str | None:
        """
        Validate API key and return associated user email.
        
        Args:
            api_key: The API key to validate
            
        Returns:
            User email if valid, None if invalid
        """
        return self._api_keys.get(api_key)

    def extract_user_from_token(self, token: HTTPAuthorizationCredentials) -> str | None:
        """
        Extract user email from Bearer token.
        
        Args:
            token: HTTP authorization credentials
            
        Returns:
            User email if valid token, None if invalid
        """
        if not token or token.scheme.lower() != "bearer":
            return None

        return self.validate_api_key(token.credentials)


# Global validator instance
api_key_validator = APIKeyValidator()


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = None
) -> str | None:
    """
    Get current authenticated user from request.
    
    Args:
        request: FastAPI request object
        credentials: HTTP authorization credentials
        
    Returns:
        User email if authenticated, None if not required
        
    Raises:
        HTTPException: If authentication is required but invalid
    """
    # Skip authentication if disabled in settings
    if not settings.api_key_validation:
        logger.debug("API key validation disabled, allowing request")
        return None

    # Extract user from credentials
    user_email = None
    if credentials:
        user_email = api_key_validator.extract_user_from_token(credentials)

    # Check if authentication is required for this endpoint
    endpoint = request.url.path
    requires_auth = _requires_authentication(endpoint)

    if requires_auth and not user_email:
        logger.warning(
            "Authentication required but not provided",
            endpoint=endpoint,
            has_credentials=bool(credentials)
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Valid API key required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if user_email:
        logger.debug("Authenticated user", user_email=user_email, endpoint=endpoint)

    return user_email


async def validate_namespace_access(
    namespace: str,
    user_email: str | None = None,
    require_owner: bool = False
) -> None:
    """
    Validate user has access to the specified namespace.
    
    Args:
        namespace: Namespace to validate access for
        user_email: User email (if authenticated)
        require_owner: Whether owner-level access is required
        
    Raises:
        HTTPException: If access is denied
    """
    # Skip validation if namespace isolation is disabled
    if not settings.namespace_isolation_enabled:
        logger.debug("Namespace isolation disabled, allowing access", namespace=namespace)
        return

    # Validate access
    has_access = namespace_service.validate_namespace_access(
        namespace, user_email, require_owner
    )

    if not has_access:
        # Try auto-creating namespace if enabled
        if settings.auto_create_namespaces and not require_owner:
            created = namespace_service.auto_create_namespace_if_needed(namespace)
            if created:
                logger.info("Auto-created namespace for access", namespace=namespace)
                return

        logger.warning(
            "Namespace access denied",
            namespace=namespace,
            user_email=user_email,
            require_owner=require_owner
        )

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied to namespace '{namespace}'"
        )

    logger.debug(
        "Namespace access granted",
        namespace=namespace,
        user_email=user_email,
        require_owner=require_owner
    )


def _requires_authentication(endpoint: str) -> bool:
    """
    Determine if an endpoint requires authentication.
    
    Args:
        endpoint: API endpoint path
        
    Returns:
        True if authentication is required
    """
    # Public endpoints that don't require authentication
    public_endpoints = {
        "/health",
        "/docs",
        "/openapi.json",
        "/redoc",
    }

    # Check exact matches
    if endpoint in public_endpoints:
        return False

    # Check prefix matches for public endpoints
    public_prefixes = [
        "/docs",
        "/redoc",
        "/static",
    ]

    for prefix in public_prefixes:
        if endpoint.startswith(prefix):
            return False

    # All other endpoints require authentication when enabled
    return True
