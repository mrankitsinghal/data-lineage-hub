# Copyright 2024 Data Lineage Hub Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main FastAPI application entry point."""

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes import router
from .config import settings
from .utils.kafka_client import get_kafka_publisher
from .utils.logging_config import configure_logging, get_logger
from .utils.otel_config import configure_opentelemetry, instrument_app


if TYPE_CHECKING:
    from utils.kafka_client import KafkaEventPublisher


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Application lifespan manager."""
    # Startup
    configure_logging(settings.otel_service_name)
    logger = get_logger(__name__)

    # Configure OpenTelemetry
    configure_opentelemetry()
    logger.info("OpenTelemetry configured")

    logger.info(
        "Starting Data Lineage Hub POC",
        service=settings.app_name,
        version=settings.app_version,
    )

    # Initialize Kafka publisher
    try:
        get_kafka_publisher()
        logger.info("Kafka publisher initialized")
    except Exception as e:
        logger.exception("Failed to initialize Kafka publisher", error=str(e))

    yield

    # Shutdown
    try:
        publisher: KafkaEventPublisher = get_kafka_publisher()
        publisher.close()
        logger.info("Kafka publisher closed")
    except Exception as e:
        logger.exception("Error closing Kafka publisher", error=str(e))


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="A POC for data pipeline observability with OpenLineage and OpenTelemetry",
    version=settings.app_version,
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v1")

# Instrument the app with OpenTelemetry
instrument_app(app)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": settings.app_name,
        "version": settings.app_version,
        "status": "running",
        "docs_url": "/docs",
    }


if __name__ == "__main__":
    import uvicorn

    configure_logging(settings.otel_service_name)
    logger = get_logger(__name__)

    logger.info(
        "Starting server", host=settings.host, port=settings.port, debug=settings.debug
    )

    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_config=None,  # Use our custom logging
    )
