.PHONY: help venv install install-dev start stop clean test run-api run-lineage-consumer format lint check fix poetry-check venv-info venv-remove venv-recreate setup-grafana-plugins

help:  ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

poetry-check:  ## Check if Poetry is installed
	@command -v poetry >/dev/null 2>&1 || { \
		echo "❌ Poetry is not installed!"; \
		echo "Install it with: curl -sSL https://install.python-poetry.org | python3 -"; \
		echo "Or visit: https://python-poetry.org/docs/#installation"; \
		exit 1; \
	}
	@echo "✅ Poetry is installed: $$(poetry --version)"

venv:  ## Create Poetry virtual environment
	$(MAKE) poetry-check
	@if [ -d ".venv" ]; then \
		echo "✅ Virtual environment already exists at .venv"; \
		echo "Location: $$(poetry env info --path)"; \
	else \
		echo "🔧 Creating Poetry virtual environment..."; \
		poetry config virtualenvs.in-project true; \
		poetry env use python3; \
		echo "✅ Poetry virtual environment created!"; \
		echo "Location: $$(poetry env info --path)"; \
	fi

install:  ## Install Python dependencies with Poetry
	$(MAKE) poetry-check
	@if [ ! -d ".venv" ]; then \
		echo "🔧 Virtual environment not found, creating one..."; \
		$(MAKE) venv; \
	fi
	poetry install
	@echo "✅ Dependencies installed!"

install-dev:  ## Install dependencies including dev dependencies
	$(MAKE) poetry-check
	@if [ ! -d ".venv" ]; then \
		echo "🔧 Virtual environment not found, creating one..."; \
		$(MAKE) venv; \
	fi
	poetry install --with dev
	@echo "✅ Development environment ready!"
	@echo "To activate: poetry shell"

start:  ## Start all infrastructure services
	./scripts/start.sh

stop:  ## Stop all services
	docker-compose down

clean:  ## Clean up containers and volumes
	docker-compose down -v --remove-orphans

run-api:  ## Run the FastAPI server
	poetry run python -m src.main

run-lineage-consumer:  ## Run the OpenLineage consumer
	poetry run python -m src.consumers.lineage_consumer

run-otel-consumer:  ## Run the OpenTelemetry consumer
	poetry run python -m src.consumers.otel_consumer

format:  ## Format code with Ruff
	$(MAKE) poetry-check
	poetry run ruff format src/ tests/
	@echo "✅ Code formatted!"

lint:  ## Lint code with Ruff
	$(MAKE) poetry-check
	poetry run ruff check src/ tests/
	@echo "✅ Code linted!"

fix:  ## Fix code issues with Ruff
	$(MAKE) poetry-check
	poetry run ruff check --fix src/ tests/
	@echo "✅ Code issues fixed!"

check:  ## Run all code quality checks
	$(MAKE) poetry-check
	@echo "🔍 Running code quality checks..."
	poetry run ruff check src/ tests/
	poetry run ruff format --check src/ tests/
	poetry run mypy src/
	@echo "✅ All checks passed!"

test:  ## Run tests with pytest
	$(MAKE) poetry-check
	poetry run pytest tests/ -v
	@echo "✅ Tests completed!"

venv-info:  ## Show virtual environment information
	$(MAKE) poetry-check
	@echo "📍 Virtual Environment Info:"
	@poetry env info
	@if [ -d ".venv" ]; then \
		echo "✅ Local .venv directory exists"; \
	else \
		echo "❌ Local .venv directory not found"; \
	fi

venv-remove:  ## Remove Poetry virtual environment
	$(MAKE) poetry-check
	poetry env remove --all
	@echo "🗑️  Virtual environment removed!"

venv-recreate:  ## Recreate virtual environment from scratch
	$(MAKE) venv-remove
	$(MAKE) venv
	$(MAKE) install-dev
	@echo "🔄 Virtual environment recreated!"

test-lineage:  ## Test the central lineage ingestion API
	curl -X POST http://localhost:8000/api/v1/lineage/ingest \
		-H 'Content-Type: application/json' \
		-d '{"namespace": "demo-team", "events": [{"eventType": "START", "job": {"name": "test_job", "namespace": "demo-team"}}]}'

status:  ## Check service status
	@echo "=== Docker Services ==="
	@docker-compose ps
	@echo ""
	@echo "=== API Health ==="
	@curl -s http://localhost:8000/api/v1/health | jq . || echo "API not available"
	@echo ""
	@echo "=== Marquez ==="
	@curl -s http://localhost:5000/api/v1/namespaces | jq . || echo "Marquez not available"

logs:  ## View all service logs
	docker-compose logs -f

setup-grafana-plugins:  ## Install required Grafana plugins
	@echo "🔌 Installing Grafana plugins..."
	@docker exec grafana grafana cli plugins install grafana-clickhouse-datasource || echo "Failed to install ClickHouse plugin"
	@echo "🔄 Restarting Grafana to load plugins..."
	@docker restart grafana
	@echo "✅ Grafana plugins configured!"

dev-setup:  ## Complete development setup
	@echo "Setting up development environment..."
	@if [ ! -f .env ]; then cp .env.example .env 2>/dev/null || echo "No .env.example found, skipping..."; fi
	$(MAKE) install-dev
	$(MAKE) start
	@echo "⏳ Waiting for services to start..."
	@sleep 30
	$(MAKE) setup-grafana-plugins
	@echo "✅ Development environment ready!"
	@echo "🌐 Access the services:"
	@echo "   • API: http://localhost:8000/docs"
	@echo "   • Marquez: http://localhost:3000"
	@echo "   • Grafana: http://localhost:3001 (admin/admin)"
