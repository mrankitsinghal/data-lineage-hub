"""Basic usage examples for Data Lineage Hub SDK."""

import asyncio
import contextlib

from src.sdk import LineageHubClient, configure, lineage_track


def main():
    """Demonstrate basic SDK usage."""

    # Configure the SDK
    configure(
        hub_endpoint="http://localhost:8000",
        api_key="demo-api-key",
        namespace="example-namespace",
        debug=True,
    )

    # Example 1: Simple function tracking

    @lineage_track(
        job_name="data_validation",
        inputs=[
            {
                "type": "file",
                "name": "/data/raw/users.csv",
                "format": "csv",
                "namespace": "raw-data",
            }
        ],
        outputs=[
            {
                "type": "file",
                "name": "/data/validated/users.csv",
                "format": "csv",
                "namespace": "validated-data",
            }
        ],
        description="Validate user data format and constraints",
    )
    def validate_user_data():
        # Simulate data validation work
        return {"valid_records": 1000, "invalid_records": 5}

    validate_user_data()

    # Example 2: ETL Pipeline

    @lineage_track(
        job_name="extract_user_data",
        inputs=[
            {
                "type": "postgres",
                "name": "public.users",
                "format": "table",
                "namespace": "prod-db",
            }
        ],
        outputs=[
            {
                "type": "file",
                "name": "/tmp/extracted_users.json",
                "format": "json",
                "namespace": "temp",
            }
        ],
        tags={"stage": "extract", "source": "postgres"},
    )
    def extract_users():
        return "/tmp/extracted_users.json"

    @lineage_track(
        job_name="transform_user_data",
        inputs=[
            {
                "type": "file",
                "name": "/tmp/extracted_users.json",
                "format": "json",
                "namespace": "temp",
            }
        ],
        outputs=[
            {
                "type": "file",
                "name": "/tmp/transformed_users.parquet",
                "format": "parquet",
                "namespace": "temp",
            }
        ],
        tags={"stage": "transform", "format": "parquet"},
    )
    def transform_users(input_path):
        return "/tmp/transformed_users.parquet"

    @lineage_track(
        job_name="load_user_data",
        inputs=[
            {
                "type": "file",
                "name": "/tmp/transformed_users.parquet",
                "format": "parquet",
                "namespace": "temp",
            }
        ],
        outputs=[
            {
                "type": "postgres",
                "name": "warehouse.users_table",
                "format": "table",
                "namespace": "warehouse",
            }
        ],
        tags={"stage": "load", "destination": "warehouse"},
    )
    def load_users(input_path):
        return "warehouse://users_table"

    # Execute pipeline
    extracted_path = extract_users()
    transformed_path = transform_users(extracted_path)
    load_users(transformed_path)

    # Example 3: Async function with error handling

    async def async_examples():
        @lineage_track(
            job_name="async_data_processing",
            inputs=[
                {
                    "type": "file",
                    "name": "/data/large_dataset.parquet",
                    "format": "parquet",
                    "namespace": "raw-data",
                }
            ],
            outputs=[
                {
                    "type": "file",
                    "name": "/data/processed/aggregated_data.csv",
                    "format": "csv",
                    "namespace": "processed-data",
                }
            ],
            description="Async processing of large dataset",
        )
        async def process_large_dataset():
            await asyncio.sleep(0.1)  # Simulate async work
            return {"processed_records": 50000, "processing_time": "2.5s"}

        with contextlib.suppress(Exception):
            await process_large_dataset()

        # Example with failure
        @lineage_track(job_name="failing_job")
        async def failing_job():
            raise ValueError("Simulated processing error")

        with contextlib.suppress(ValueError):
            await failing_job()

    # Run async examples
    asyncio.run(async_examples())


# Example 4: Direct API usage
async def direct_api_example():
    """Example of using the client directly without decorators."""

    async with LineageHubClient() as client:
        # Check service health
        try:
            await client.health_check()
        except Exception:
            return

        # Send custom lineage events
        events = [
            {
                "eventType": "START",
                "eventTime": "2024-01-01T10:00:00.000Z",
                "run": {"runId": "manual-run-123"},
                "job": {"namespace": "example-namespace", "name": "manual_data_export"},
                "inputs": [
                    {"namespace": "database", "name": "users_table"},
                    {"namespace": "database", "name": "orders_table"},
                ],
                "producer": "manual-example",
            },
            {
                "eventType": "COMPLETE",
                "eventTime": "2024-01-01T10:15:00.000Z",
                "run": {"runId": "manual-run-123"},
                "job": {"namespace": "example-namespace", "name": "manual_data_export"},
                "outputs": [{"namespace": "file", "name": "/exports/user_orders.csv"}],
                "producer": "manual-example",
            },
        ]

        try:
            response = await client.send_lineage_events(events)
            if response.errors:
                pass
        except Exception:
            pass

        # List available namespaces
        try:
            namespaces = await client.list_namespaces()
            for _ns in namespaces[:3]:  # Show first 3
                pass
        except Exception:
            pass


if __name__ == "__main__":
    main()

    asyncio.run(direct_api_example())
