"""Basic usage examples for Data Lineage Hub SDK."""

import asyncio
from data_lineage_hub_sdk import configure, lineage_track, LineageHubClient


def main():
    """Demonstrate basic SDK usage."""
    
    # Configure the SDK
    configure(
        hub_endpoint="http://localhost:8000",
        api_key="demo-api-key",
        namespace="example-namespace",
        debug=True,
    )
    
    print("Basic SDK Usage Examples")
    print("=" * 40)
    
    # Example 1: Simple function tracking
    print("\n1. Simple Function Tracking")
    
    @lineage_track(
        job_name="data_validation",
        inputs=["/data/raw/users.csv"],
        outputs=["/data/validated/users.csv"],
        description="Validate user data format and constraints"
    )
    def validate_user_data():
        # Simulate data validation work
        print("   Validating user data...")
        return {"valid_records": 1000, "invalid_records": 5}
    
    result = validate_user_data()
    print(f"   Result: {result}")
    
    
    # Example 2: ETL Pipeline
    print("\n2. ETL Pipeline with Multiple Stages")
    
    @lineage_track(
        job_name="extract_user_data",
        outputs=["/tmp/extracted_users.json"],
        tags={"stage": "extract", "source": "postgres"}
    )
    def extract_users():
        print("   Extracting users from database...")
        return "/tmp/extracted_users.json"
    
    @lineage_track(
        job_name="transform_user_data", 
        inputs=["/tmp/extracted_users.json"],
        outputs=["/tmp/transformed_users.parquet"],
        tags={"stage": "transform", "format": "parquet"}
    )
    def transform_users(input_path):
        print(f"   Transforming data from {input_path}...")
        return "/tmp/transformed_users.parquet"
    
    @lineage_track(
        job_name="load_user_data",
        inputs=["/tmp/transformed_users.parquet"],
        outputs=["warehouse://users_table"],
        tags={"stage": "load", "destination": "warehouse"}
    )
    def load_users(input_path):
        print(f"   Loading data from {input_path} to warehouse...")
        return "warehouse://users_table"
    
    # Execute pipeline
    extracted_path = extract_users()
    transformed_path = transform_users(extracted_path)
    final_destination = load_users(transformed_path)
    
    print(f"   Pipeline completed: {final_destination}")
    
    
    # Example 3: Async function with error handling
    print("\n3. Async Function with Error Handling")
    
    async def async_examples():
        
        @lineage_track(
            job_name="async_data_processing",
            inputs=["/data/large_dataset.parquet"],
            outputs=["/data/processed/aggregated_data.csv"],
            description="Async processing of large dataset"
        )
        async def process_large_dataset():
            print("   Starting async data processing...")
            await asyncio.sleep(0.1)  # Simulate async work
            print("   Async processing completed")
            return {"processed_records": 50000, "processing_time": "2.5s"}
        
        try:
            result = await process_large_dataset()
            print(f"   Result: {result}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Example with failure
        @lineage_track(job_name="failing_job")
        async def failing_job():
            print("   This job will fail...")
            raise ValueError("Simulated processing error")
        
        try:
            await failing_job()
        except ValueError as e:
            print(f"   Caught expected error: {e}")
    
    # Run async examples
    asyncio.run(async_examples())


# Example 4: Direct API usage
async def direct_api_example():
    """Example of using the client directly without decorators."""
    
    print("\n4. Direct API Usage")
    
    async with LineageHubClient() as client:
        # Check service health
        try:
            health = await client.health_check()
            print(f"   Service status: {health.status}")
        except Exception as e:
            print(f"   Health check failed: {e}")
            return
        
        # Send custom lineage events
        events = [
            {
                "eventType": "START",
                "eventTime": "2024-01-01T10:00:00.000Z",
                "run": {"runId": "manual-run-123"},
                "job": {
                    "namespace": "example-namespace",
                    "name": "manual_data_export"
                },
                "inputs": [
                    {"namespace": "database", "name": "users_table"},
                    {"namespace": "database", "name": "orders_table"}
                ],
                "producer": "manual-example"
            },
            {
                "eventType": "COMPLETE",
                "eventTime": "2024-01-01T10:15:00.000Z",
                "run": {"runId": "manual-run-123"},
                "job": {
                    "namespace": "example-namespace",
                    "name": "manual_data_export"
                },
                "outputs": [
                    {"namespace": "file", "name": "/exports/user_orders.csv"}
                ],
                "producer": "manual-example"
            }
        ]
        
        try:
            response = await client.send_lineage_events(events)
            print(f"   Sent events: {response.accepted} accepted, {response.rejected} rejected")
            if response.errors:
                print(f"   Errors: {response.errors}")
        except Exception as e:
            print(f"   Failed to send events: {e}")
        
        # List available namespaces
        try:
            namespaces = await client.list_namespaces()
            print(f"   Available namespaces: {len(namespaces)}")
            for ns in namespaces[:3]:  # Show first 3
                print(f"     - {ns.name}: {ns.display_name}")
        except Exception as e:
            print(f"   Failed to list namespaces: {e}")


if __name__ == "__main__":
    print("Running basic usage examples...")
    main()
    
    print("\nRunning direct API examples...")
    asyncio.run(direct_api_example())
    
    print("\nExamples completed!")