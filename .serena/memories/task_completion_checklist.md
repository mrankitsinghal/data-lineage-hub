# Task Completion Checklist

When completing any coding task in this project, ALWAYS run these commands in order:

## 1. Code Quality Checks (MANDATORY)

```bash
# Run the comprehensive check first
make check

# If check fails, fix issues:
make format              # Auto-format code
make fix                # Auto-fix linting issues
make check              # Verify all issues resolved
```

## 2. Run Tests

```bash
make test               # Run all tests
```

## 3. Test Functionality (if applicable)

```bash
# For pipeline changes:
make test-pipeline

# For API changes:
curl http://localhost:8000/docs  # Check API documentation
curl http://localhost:8000/api/v1/health  # Check health endpoint
```

## 4. Verify Services (if running)

```bash
make status             # Check all services are healthy
```

## Important Notes

- NEVER commit code without running `make check` first
- If MyPy type errors occur, fix them before proceeding
- If Ruff formatting issues exist, run `make format`
- Always ensure tests pass with `make test`
- For new dependencies, update pyproject.toml and run `poetry install --with dev`

## Common Issues to Check

- Type hints on all functions
- No unused imports
- Proper error handling
- Structured logging instead of print statements
- Async functions properly awaited
- Configuration values from settings, not hardcoded
