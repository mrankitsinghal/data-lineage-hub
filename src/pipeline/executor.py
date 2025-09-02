"""Pipeline executor that orchestrates the entire pipeline with lineage tracking."""

import time
from datetime import UTC, datetime
from typing import Any

import structlog

from src.api.models import PipelineRunRequest, PipelineStatus
from src.config import settings
from src.utils.openlineage_client import DatasetInfo, JobInfo, OpenLineageTracker
from src.utils.otel_config import get_pipeline_metrics

from .stages import PipelineStages


logger = structlog.get_logger(__name__)


class PipelineExecutor:
    """Executes the complete data pipeline with OpenLineage tracking."""

    def __init__(self, run_id: str, request: PipelineRunRequest):
        self.run_id = run_id
        self.request = request
        self.tracker = OpenLineageTracker()
        self.stages = PipelineStages(run_id)
        self.metrics = get_pipeline_metrics()

    async def execute(self) -> dict[str, Any]:
        """Execute the complete pipeline."""
        start_time = time.time()

        logger.info(
            "Starting pipeline execution",
            run_id=self.run_id,
            pipeline_name=self.request.pipeline_name,
            input_path=self.request.input_path,
            output_path=self.request.output_path,
        )

        # Start overall pipeline tracking
        job_info = JobInfo(
            namespace=settings.openlineage_namespace,
            name=self.request.pipeline_name,
            description=f"Complete ETL pipeline: {self.request.pipeline_name}",
        )

        self.tracker.start_run(job_info, self.run_id)

        # Record pipeline start
        self.metrics.record_pipeline_start(
            pipeline_name=self.request.pipeline_name, run_id=self.run_id
        )

        try:
            # Update status
            self._update_run_status(PipelineStatus.RUNNING, stages_completed=0)

            # Stage 1: Extract
            logger.info("Executing extract stage", run_id=self.run_id)
            extracted_data = self.stages.extract(self.request.input_path)
            self._update_run_status(
                PipelineStatus.RUNNING,
                stages_completed=1,
                records_processed=len(extracted_data),
            )

            # Stage 2: Transform
            logger.info("Executing transform stage", run_id=self.run_id)
            transformed_data = self.stages.transform(extracted_data)
            self._update_run_status(
                PipelineStatus.RUNNING,
                stages_completed=2,
                records_processed=len(transformed_data),
            )

            # Stage 3: Load
            logger.info("Executing load stage", run_id=self.run_id)
            load_result = self.stages.load(transformed_data, self.request.output_path)
            self._update_run_status(
                PipelineStatus.RUNNING,
                stages_completed=3,
                records_processed=load_result["records_written"],
            )

            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)

            # Complete overall pipeline tracking
            inputs = [
                DatasetInfo(
                    namespace="file",
                    name=self.request.input_path,
                    description=f"Input data for {self.request.pipeline_name}",
                )
            ]

            outputs = [
                DatasetInfo(
                    namespace="file",
                    name=load_result["output_path"],
                    description=f"Output data from {self.request.pipeline_name}",
                    schema_fields=[
                        {"name": col, "type": "string"}
                        for col in load_result["columns"]
                    ],
                )
            ]

            self.tracker.complete_run(inputs=inputs, outputs=outputs)

            # Record successful pipeline completion
            self.metrics.record_pipeline_success(
                pipeline_name=self.request.pipeline_name,
                run_id=self.run_id,
                duration_ms=duration_ms,
                records=load_result["records_written"],
            )

            # Final status update
            self._update_run_status(
                PipelineStatus.COMPLETED,
                stages_completed=3,
                records_processed=load_result["records_written"],
                duration_ms=duration_ms,
                completed_at=datetime.now(UTC),
            )

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
            }

        except Exception as e:
            # Calculate duration even for failed runs
            duration_ms = int((time.time() - start_time) * 1000)

            # Mark pipeline as failed
            self.tracker.fail_run(str(e))

            # Record pipeline failure
            self.metrics.record_pipeline_failure(
                pipeline_name=self.request.pipeline_name,
                run_id=self.run_id,
                duration_ms=duration_ms,
                error=str(e),
            )

            # Update status
            self._update_run_status(
                PipelineStatus.FAILED,
                duration_ms=duration_ms,
                error_message=str(e),
                completed_at=datetime.now(UTC),
            )

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
            }

    def _update_run_status(self, status: PipelineStatus, **kwargs) -> None:
        """Update pipeline run status."""
        try:
            from src.api.routes import update_pipeline_run

            update_data = {"status": status}
            update_data.update(kwargs)

            update_pipeline_run(self.run_id, **update_data)

        except Exception as e:
            logger.exception(
                "Failed to update run status", error=str(e), run_id=self.run_id
            )
