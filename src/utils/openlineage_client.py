"""OpenLineage client and event generation utilities."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.facet import (
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.transport.kafka import KafkaConfig, KafkaTransport

from src.config import settings


logger = structlog.get_logger(__name__)


@dataclass
class DatasetInfo:
    """Dataset information for lineage tracking."""

    namespace: str
    name: str
    schema_fields: list[dict[str, str]] | None = None
    description: str | None = None


@dataclass
class JobInfo:
    """Job information for lineage tracking."""

    namespace: str
    name: str
    description: str | None = None


class OpenLineageTracker:
    """OpenLineage event tracker for pipeline stages."""

    def __init__(self):
        # Configure Kafka transport for OpenLineage events
        kafka_config = KafkaConfig(
            config={"bootstrap.servers": settings.kafka_bootstrap_servers},
            topic=settings.kafka_openlineage_topic,
            messageKey="lineage-events",
            flush=True,
        )

        self.client = OpenLineageClient(transport=KafkaTransport(kafka_config))
        self.run_id = None
        self.job_info = None
        self.parent_run_id = None

    def start_run(
        self,
        job_info: JobInfo,
        run_id: str | None = None,
        parent_run_id: str | None = None,
        nominal_start_time: datetime | None = None,
    ) -> str:
        """Start a new pipeline run."""
        self.run_id = run_id or str(uuid.uuid4())
        self.job_info = job_info
        self.parent_run_id = parent_run_id

        # Create job
        job_facets = {}
        # DocumentationJobFacet not available in current version
        # if job_info.description:
        #     job_facets["documentation"] = DocumentationJobFacet(
        #         description=job_info.description
        #     )

        job = Job(namespace=job_info.namespace, name=job_info.name, facets=job_facets)

        # Create run
        run_facets = {}
        # NominalTimeRunFacet and ParentRunFacet not available in current version
        # if nominal_start_time:
        #     run_facets["nominalTime"] = NominalTimeRunFacet(
        #         nominalStartTime=nominal_start_time.isoformat()
        #     )
        # if parent_run_id:
        #     run_facets["parent"] = ParentRunFacet(
        #         run={"runId": parent_run_id},
        #         job={"namespace": job_info.namespace, "name": "parent-job"},
        #     )

        run = Run(runId=self.run_id, facets=run_facets)

        # Create and emit START event
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(UTC).isoformat(),
            job=job,
            run=run,
            producer=settings.openlineage_producer,
        )

        self._emit_event(event)
        logger.info("Started OpenLineage run", run_id=self.run_id, job=job_info.name)

        return self.run_id

    def complete_run(
        self,
        inputs: list[DatasetInfo] | None = None,
        outputs: list[DatasetInfo] | None = None,
    ) -> None:
        """Complete the pipeline run with input/output datasets."""
        if not self.run_id or not self.job_info:
            logger.warning("No active run to complete")
            return

        # Create datasets
        input_datasets = []
        if inputs:
            for dataset_info in inputs:
                input_datasets.append(self._create_dataset(dataset_info))

        output_datasets = []
        if outputs:
            for dataset_info in outputs:
                output_datasets.append(self._create_dataset(dataset_info))

        # Create job with facets
        job_facets = {}
        # DocumentationJobFacet not available in current version
        # if self.job_info.description:
        #     job_facets["documentation"] = DocumentationJobFacet(
        #         description=self.job_info.description
        #     )

        job = Job(
            namespace=self.job_info.namespace,
            name=self.job_info.name,
            facets=job_facets,
        )

        # Create run
        run_facets = {}
        # ParentRunFacet not available in current version
        # if self.parent_run_id:
        #     run_facets["parent"] = ParentRunFacet(
        #         run={"runId": self.parent_run_id},
        #         job={"namespace": self.job_info.namespace, "name": "parent-job"},
        #     )

        run = Run(runId=self.run_id, facets=run_facets)

        # Create and emit COMPLETE event
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now(UTC).isoformat(),
            job=job,
            run=run,
            inputs=input_datasets,
            outputs=output_datasets,
            producer=settings.openlineage_producer,
        )

        self._emit_event(event)
        logger.info(
            "Completed OpenLineage run", run_id=self.run_id, job=self.job_info.name
        )

    def fail_run(self, error_message: str | None = None) -> None:
        """Mark the pipeline run as failed."""
        if not self.run_id or not self.job_info:
            logger.warning("No active run to fail")
            return

        job = Job(namespace=self.job_info.namespace, name=self.job_info.name)

        run = Run(runId=self.run_id)

        # Create and emit FAIL event
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now(UTC).isoformat(),
            job=job,
            run=run,
            producer=settings.openlineage_producer,
        )

        self._emit_event(event)
        logger.error(
            "Failed OpenLineage run",
            run_id=self.run_id,
            job=self.job_info.name,
            error=error_message,
        )

    def _create_dataset(self, dataset_info: DatasetInfo) -> Dataset:
        """Create a Dataset object from DatasetInfo."""
        facets = {}

        if dataset_info.schema_fields:
            schema_fields = []
            for field in dataset_info.schema_fields:
                schema_fields.append(
                    SchemaField(
                        name=field["name"],
                        type=field["type"],
                        description=field.get("description"),
                    )
                )

            facets["schema"] = SchemaDatasetFacet(fields=schema_fields)

        return Dataset(
            namespace=dataset_info.namespace, name=dataset_info.name, facets=facets
        )

    def _emit_event(self, event: RunEvent) -> None:
        """Emit OpenLineage event."""
        try:
            # Use the OpenLineage client directly to emit events
            self.client.emit(event)

            logger.debug(
                "Emitted OpenLineage event",
                event_type=event.eventType,
                run_id=self.run_id,
            )

        except Exception as e:
            logger.exception(
                "Error emitting OpenLineage event", error=str(e), run_id=self.run_id
            )


def openlineage_job(
    job_name: str,
    namespace: str | None = None,
    description: str | None = None,
    inputs: list[DatasetInfo] | None = None,
    outputs: list[DatasetInfo] | None = None,
):
    """Decorator for automatic OpenLineage tracking of job functions."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            tracker = OpenLineageTracker()

            job_info = JobInfo(
                namespace=namespace or settings.openlineage_namespace,
                name=job_name,
                description=description,
            )

            # Start tracking
            tracker.start_run(job_info)

            try:
                # Execute the function
                result = func(*args, **kwargs)

                # Complete successfully
                tracker.complete_run(inputs=inputs, outputs=outputs)
                return result

            except Exception as e:
                # Mark as failed
                tracker.fail_run(str(e))
                raise

        return wrapper

    return decorator
