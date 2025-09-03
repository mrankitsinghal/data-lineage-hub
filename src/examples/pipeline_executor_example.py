"""
Updated pipeline executor using simplified @lineage_track decorator.
Demonstrates complete ETL orchestration with dataset lineage chains.
"""

import time
import uuid
from typing import Any

import structlog
from pipeline_stages_example import PipelineStages

from src.sdk import configure, lineage_track


logger = structlog.get_logger(__name__)

# Configure the SDK
configure(
    hub_endpoint="http://localhost:8000",
    api_key="demo-api-key",
    namespace="pipeline-orchestrator",
    debug=True,
)


class PipelineRunRequest:
    """Simple request model for pipeline execution."""

    def __init__(self, pipeline_name: str, input_path: str, output_path: str):
        self.pipeline_name = pipeline_name
        self.input_path = input_path
        self.output_path = output_path


class PipelineExecutor:
    """Executes the complete data pipeline with simplified lineage tracking."""

    def __init__(self, run_id: str, request: PipelineRunRequest):
        self.run_id = run_id
        self.request = request
        self.stages = PipelineStages(run_id)

    @lineage_track(
        job_name="complete_etl_pipeline",
        description="Complete ETL pipeline orchestrator",
        inputs=[
            {
                "type": "file",
                "name": "pipeline_input",
                "format": "csv",
                "namespace": "raw-data",
            }
        ],
        outputs=[
            {
                "type": "file",
                "name": "pipeline_output",
                "format": "csv",
                "namespace": "processed-data",
            }
        ],
        tags={"pipeline": "etl", "orchestrator": "main", "type": "batch"},
    )
    async def execute(self) -> dict[str, Any]:
        """Execute the complete pipeline with overall lineage tracking."""
        start_time = time.time()

        logger.info(
            "Starting pipeline execution",
            run_id=self.run_id,
            pipeline_name=self.request.pipeline_name,
            input_path=self.request.input_path,
            output_path=self.request.output_path,
        )

        try:
            # Stage 1: Extract
            logger.info("Executing extract stage", run_id=self.run_id)
            extracted_data = self.stages.extract(self.request.input_path)

            # Stage 2: Transform
            logger.info("Executing transform stage", run_id=self.run_id)
            transformed_data = self.stages.transform(extracted_data)

            # Stage 3: Load
            logger.info("Executing load stage", run_id=self.run_id)
            load_result = self.stages.load(transformed_data, self.request.output_path)

            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)

            logger.info(
                "Pipeline execution completed successfully",
                run_id=self.run_id,
                duration_ms=duration_ms,
                records_processed=load_result["records_written"],
            )

            return {
                "status": "completed",
                "duration_ms": duration_ms,
                "records_processed": load_result["records_written"],
                "output_path": load_result["output_path"],
                "stages_completed": 3,
                "run_id": self.run_id,
                "pipeline_name": self.request.pipeline_name,
            }

        except Exception as e:
            # Calculate duration even for failed runs
            duration_ms = int((time.time() - start_time) * 1000)

            logger.exception(
                "Pipeline execution failed",
                run_id=self.run_id,
                error=str(e),
                duration_ms=duration_ms,
            )

            return {
                "status": "failed",
                "duration_ms": duration_ms,
                "error_message": str(e),
                "run_id": self.run_id,
                "pipeline_name": self.request.pipeline_name,
            }


# Multi-Pipeline Examples with Different Data Sources
class DataEngineeringPipelines:
    """Examples of different pipeline types for various teams."""

    @lineage_track(
        job_name="s3_to_warehouse_pipeline",
        description="Ingest S3 data into data warehouse",
        inputs=[
            {
                "type": "s3",
                "name": "s3://data-lake/raw/events/2024/",
                "format": "parquet",
                "namespace": "production",
            }
        ],
        outputs=[
            {
                "type": "clickhouse",
                "name": "warehouse.processed_events",
                "format": "table",
                "namespace": "analytics",
            }
        ],
        tags={
            "team": "data-engineering",
            "pattern": "batch-ingestion",
            "frequency": "daily",
        },
    )
    def s3_to_warehouse_pipeline(self) -> dict[str, Any]:
        """Data Engineering: S3 → ClickHouse pipeline."""
        logger.info("Running S3 to warehouse pipeline")

        # Simulate S3 data processing
        time.sleep(2)

        return {
            "status": "completed",
            "records_processed": 1250000,
            "partitions_processed": 24,
            "duration_minutes": 8.5,
            "warehouse_table": "warehouse.processed_events",
        }

    @lineage_track(
        job_name="streaming_aggregation_pipeline",
        description="Real-time event aggregation pipeline",
        inputs=[
            {
                "type": "event",
                "name": "user-events",
                "format": "avro",
                "namespace": "kafka",
            },
            {
                "type": "mysql",
                "name": "reference.user_segments",
                "format": "table",
                "namespace": "prod-db",
            },
        ],
        outputs=[
            {
                "type": "event",
                "name": "aggregated-metrics",
                "format": "json",
                "namespace": "kinesis",
            },
            {
                "type": "clickhouse",
                "name": "realtime.user_metrics",
                "format": "table",
                "namespace": "streaming",
            },
        ],
        tags={"team": "streaming", "pattern": "real-time", "latency": "sub-second"},
    )
    def streaming_aggregation_pipeline(self) -> dict[str, Any]:
        """Streaming: Kafka + MySQL → Kinesis + ClickHouse pipeline."""
        logger.info("Running streaming aggregation pipeline")

        # Simulate streaming processing
        time.sleep(1)

        return {
            "status": "completed",
            "events_processed_per_second": 5000,
            "aggregation_windows": ["1min", "5min", "1hour"],
            "output_topics": ["aggregated-metrics"],
            "latency_p99_ms": 150,
        }


class AnalyticsPipelines:
    """Analytics team pipeline examples."""

    @lineage_track(
        job_name="daily_business_metrics",
        description="Generate daily business metrics and reports",
        inputs=[
            {
                "type": "clickhouse",
                "name": "warehouse.processed_events",
                "format": "table",
                "namespace": "analytics",
            },
            {
                "type": "postgres",
                "name": "public.customer_segments",
                "format": "table",
                "namespace": "crm",
            },
        ],
        outputs=[
            {
                "type": "s3",
                "name": "s3://reports/daily/business_metrics.csv",
                "format": "csv",
                "namespace": "reporting",
            },
            {
                "type": "api",
                "name": "https://dashboard.company.com/api/metrics",
                "format": "json",
                "namespace": "external",
            },
        ],
        tags={
            "team": "analytics",
            "frequency": "daily",
            "type": "business-intelligence",
        },
    )
    def daily_business_metrics(self) -> dict[str, Any]:
        """Analytics: ClickHouse + Postgres → S3 + API pipeline."""
        logger.info("Running daily business metrics pipeline")

        # Simulate analytics processing
        time.sleep(3)

        return {
            "status": "completed",
            "metrics_calculated": [
                "daily_active_users",
                "revenue",
                "conversion_rate",
                "churn_rate",
                "customer_lifetime_value",
            ],
            "segments_analyzed": 12,
            "report_size_mb": 25.6,
            "dashboard_updated": True,
        }


class MLPipelines:
    """ML Engineering team pipeline examples."""

    @lineage_track(
        job_name="feature_engineering_pipeline",
        description="Create ML features from multiple data sources",
        inputs=[
            {
                "type": "clickhouse",
                "name": "analytics.user_metrics",
                "format": "table",
                "namespace": "streaming",
            },
            {
                "type": "s3",
                "name": "s3://ml-data/external/demographics.parquet",
                "format": "parquet",
                "namespace": "external",
            },
            {
                "type": "api",
                "name": "https://feature-store.company.com/v1/behavioral-features",
                "format": "json",
                "namespace": "feature-store",
            },
        ],
        outputs=[
            {
                "type": "s3",
                "name": "s3://ml-features/training/feature_matrix_v2.parquet",
                "format": "parquet",
                "namespace": "ml-training",
            }
        ],
        tags={
            "team": "ml-engineering",
            "stage": "feature-engineering",
            "version": "v2",
        },
    )
    def feature_engineering_pipeline(self) -> dict[str, Any]:
        """ML Engineering: Multi-source → ML Features pipeline."""
        logger.info("Running feature engineering pipeline")

        # Simulate ML feature processing
        time.sleep(4)

        return {
            "status": "completed",
            "features_created": 127,
            "training_samples": 500000,
            "feature_types": ["numerical", "categorical", "temporal", "behavioral"],
            "feature_store_updated": True,
            "data_quality_score": 0.94,
        }

    @lineage_track(
        job_name="model_training_pipeline",
        description="Train ML model with feature data",
        inputs=[
            {
                "type": "s3",
                "name": "s3://ml-features/training/feature_matrix_v2.parquet",
                "format": "parquet",
                "namespace": "ml-training",
            }
        ],
        outputs=[
            {
                "type": "file",
                "name": "/models/customer_churn_v3.pkl",
                "format": "binary",
                "namespace": "local",
            },
            {
                "type": "s3",
                "name": "s3://ml-models/production/customer_churn_v3/",
                "format": "binary",
                "namespace": "ml-serving",
            },
        ],
        tags={
            "team": "ml-engineering",
            "stage": "training",
            "model": "customer_churn",
            "version": "v3",
        },
    )
    def model_training_pipeline(self) -> dict[str, Any]:
        """ML Engineering: Features → Trained Model pipeline."""
        logger.info("Running model training pipeline")

        # Simulate model training
        time.sleep(6)

        return {
            "status": "completed",
            "model_type": "XGBoost",
            "training_duration_minutes": 45,
            "cross_validation_scores": [0.89, 0.91, 0.88, 0.90, 0.89],
            "final_accuracy": 0.89,
            "model_size_mb": 12.3,
            "ready_for_deployment": True,
        }


# Demonstration functions
async def run_basic_etl_example():
    """Run the basic ETL pipeline example."""

    run_id = str(uuid.uuid4())
    request = PipelineRunRequest(
        pipeline_name="basic_etl",
        input_path="/tmp/input_data.csv",
        output_path="/tmp/output_data.csv",
    )

    executor = PipelineExecutor(run_id, request)
    return await executor.execute()


def run_multi_team_pipeline_examples():
    """Run pipeline examples from different teams."""

    # Data Engineering pipelines
    de_pipelines = DataEngineeringPipelines()

    de_pipelines.s3_to_warehouse_pipeline()

    de_pipelines.streaming_aggregation_pipeline()

    # Analytics pipelines
    analytics_pipelines = AnalyticsPipelines()

    analytics_pipelines.daily_business_metrics()

    # ML Engineering pipelines
    ml_pipelines = MLPipelines()

    ml_pipelines.feature_engineering_pipeline()

    ml_pipelines.model_training_pipeline()


async def main():
    """Run all pipeline examples."""

    # Run basic ETL
    await run_basic_etl_example()

    # Run multi-team examples
    run_multi_team_pipeline_examples()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
