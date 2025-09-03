"""
Updated pipeline stages using the simplified @lineage_track decorator.
Demonstrates real ETL pipeline with dict-based dataset specifications.
"""

import os
import time
from typing import Any

import pandas as pd
import structlog

from src.sdk import configure, lineage_track


logger = structlog.get_logger(__name__)

# Configure the SDK
configure(
    hub_endpoint="http://localhost:8000",
    api_key="demo-api-key",
    namespace="pipeline-example",
    debug=True,
)


class PipelineStages:
    """Collection of pipeline stages with simplified lineage tracking."""

    def __init__(self, run_id: str):
        self.run_id = run_id

    @lineage_track(
        job_name="extract_stage",
        description="Extract data from input source",
        inputs=[
            {
                "type": "file",
                "name": "input_data.csv",
                "format": "csv",
                "namespace": "raw-data",
            }
        ],
        outputs=[
            {
                "type": "dataframe",
                "name": "extracted_df",
                "format": "table",
                "namespace": "memory",
            }
        ],
        tags={"stage": "extract", "pipeline": "etl"},
    )
    def extract(self, input_path: str) -> pd.DataFrame:
        """Extract stage - read data from input source."""
        logger.info("Starting extract stage", input_path=input_path, run_id=self.run_id)

        # Simulate processing time
        time.sleep(1)

        try:
            if input_path.endswith(".csv"):
                df = pd.read_csv(input_path)
            elif input_path.endswith(".json"):
                df = pd.read_json(input_path)
            else:
                # Create sample data if file doesn't exist or unknown format
                df = self._create_sample_data()

            logger.info(
                "Extract stage completed",
                records_count=len(df),
                columns=list(df.columns),
                run_id=self.run_id,
            )

            return df

        except Exception as e:
            logger.exception("Extract stage failed", error=str(e), run_id=self.run_id)
            raise

    @lineage_track(
        job_name="transform_stage",
        description="Transform and clean data",
        inputs=[
            {
                "type": "dataframe",
                "name": "extracted_df",
                "format": "table",
                "namespace": "memory",
            }
        ],
        outputs=[
            {
                "type": "dataframe",
                "name": "transformed_df",
                "format": "table",
                "namespace": "memory",
            }
        ],
        tags={"stage": "transform", "pipeline": "etl", "operation": "clean-enrich"},
    )
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform stage - clean and enrich data."""
        logger.info(
            "Starting transform stage", input_records=len(df), run_id=self.run_id
        )

        # Simulate processing time
        time.sleep(2)

        try:
            # Create a copy for transformation
            transformed_df = df.copy()

            # Data cleaning and transformation
            if "name" in transformed_df.columns:
                # Clean name field
                transformed_df["name"] = transformed_df["name"].str.strip().str.title()

            if "value" in transformed_df.columns:
                # Normalize values (example: scale to 0-100)
                max_val = transformed_df["value"].max()
                if max_val > 0:
                    transformed_df["value"] = (transformed_df["value"] / max_val) * 100

            # Add derived fields
            if "value" in transformed_df.columns:
                transformed_df["category"] = pd.cut(
                    transformed_df["value"],
                    bins=[0, 25, 50, 75, 100],
                    labels=["Low", "Medium", "High", "Very High"],
                    include_lowest=True,
                )

            # Add validation flag
            transformed_df["is_valid"] = True  # Simplified validation
            if "name" in transformed_df.columns:
                transformed_df.loc[transformed_df["name"].isna(), "is_valid"] = False

            # Add processing timestamp
            transformed_df["processed_at"] = pd.Timestamp.now()

            logger.info(
                "Transform stage completed",
                output_records=len(transformed_df),
                valid_records=transformed_df["is_valid"].sum()
                if "is_valid" in transformed_df.columns
                else len(transformed_df),
                run_id=self.run_id,
            )

            return transformed_df

        except Exception as e:
            logger.exception("Transform stage failed", error=str(e), run_id=self.run_id)
            raise

    @lineage_track(
        job_name="load_stage",
        description="Load data to output destination",
        inputs=[
            {
                "type": "dataframe",
                "name": "transformed_df",
                "format": "table",
                "namespace": "memory",
            }
        ],
        outputs=[
            {
                "type": "file",
                "name": "processed_output.csv",
                "format": "csv",
                "namespace": "processed-data",
            }
        ],
        tags={"stage": "load", "pipeline": "etl", "operation": "persist"},
    )
    def load(self, df: pd.DataFrame, output_path: str) -> dict[str, Any]:
        """Load stage - write data to output destination."""
        logger.info(
            "Starting load stage",
            output_path=output_path,
            records=len(df),
            run_id=self.run_id,
        )

        # Simulate processing time
        time.sleep(1)

        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            # Write data based on file extension
            if output_path.endswith(".csv"):
                df.to_csv(output_path, index=False)
            elif output_path.endswith(".json"):
                df.to_json(output_path, orient="records", date_format="iso")
            elif output_path.endswith(".parquet"):
                df.to_parquet(output_path, index=False)
            else:
                # Default to CSV
                df.to_csv(output_path + ".csv", index=False)
                output_path += ".csv"

            # Calculate output statistics
            file_size = (
                os.path.getsize(output_path) if os.path.exists(output_path) else 0
            )

            result = {
                "output_path": output_path,
                "records_written": len(df),
                "file_size_bytes": file_size,
                "columns": list(df.columns),
            }

            logger.info(
                "Load stage completed",
                output_path=output_path,
                records_written=len(df),
                file_size_bytes=file_size,
                run_id=self.run_id,
            )

            return result

        except Exception as e:
            logger.exception("Load stage failed", error=str(e), run_id=self.run_id)
            raise

    def _create_sample_data(self, num_records: int = 1000) -> pd.DataFrame:
        """Create sample data for demonstration."""
        import numpy as np

        np.random.seed(42)  # For reproducible data

        data = {
            "id": range(1, num_records + 1),
            "name": [f"Record_{i}" for i in range(1, num_records + 1)],
            "value": np.random.uniform(0, 1000, num_records),
            "timestamp": pd.date_range("2024-01-01", periods=num_records, freq="H"),
        }

        df = pd.DataFrame(data)
        logger.info("Created sample data", records=len(df))

        return df


# Example usage and demonstration
def run_pipeline_example() -> None:
    """Run the complete pipeline example."""
    import uuid

    run_id = str(object=uuid.uuid4())

    # Initialize pipeline
    pipeline = PipelineStages(run_id=run_id)

    # Stage 1: Extract
    input_path = (
        "/tmp/sample_input.csv"  # Will create sample data since file doesn't exist
    )
    extracted_data = pipeline.extract(input_path)

    # Stage 2: Transform
    transformed_data = pipeline.transform(extracted_data)
    if "is_valid" in transformed_data.columns:
        transformed_data["is_valid"].sum()

    # Stage 3: Load
    output_path = "/tmp/pipeline_output.csv"
    pipeline.load(transformed_data, output_path)


if __name__ == "__main__":
    run_pipeline_example()
