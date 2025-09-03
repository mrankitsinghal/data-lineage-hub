# Code Style and Conventions

## Python Version

- Python 3.11+ with modern syntax features
- Uses new union syntax: `str | None` instead of `Optional[str]`

## Code Formatting & Linting

- **Ruff** for formatting and linting
- Line length: 88 characters (Black-compatible)
- Double quotes for strings
- 4 spaces indentation
- Comprehensive Ruff rule set enabled (see pyproject.toml)

## Type Hints

- **Strict type checking** with MyPy
- All functions must have type hints
- Pydantic for data validation and settings
- No implicit optional types

## Code Organization

- Imports sorted by isort rules (via Ruff)
- 2 blank lines after imports
- Known first-party: `src`
- Clear module structure with `__init__.py` files

## Naming Conventions

- Snake_case for functions and variables
- PascalCase for classes
- UPPER_CASE for constants
- Descriptive names preferred

## Documentation

- Triple-quoted docstrings for functions and classes
- Structured logging via structlog
- Clear error messages
- Type hints serve as inline documentation

## Async Patterns

- Async/await for I/O operations
- FastAPI async endpoints where appropriate
- Proper async context managers

## Error Handling

- Explicit exception handling
- Structured logging for errors
- Graceful degradation

## Testing

- pytest for testing
- Test files: `test_*.py` or `*_test.py`
- Test classes: `Test*`
- Test functions: `test_*`

## Special Patterns

- OpenLineage decorators for lineage tracking
- Background tasks for async processing
- Dependency injection via FastAPI
- Configuration via Pydantic Settings with .env support
