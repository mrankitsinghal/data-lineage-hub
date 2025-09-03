#!/usr/bin/env python3
"""Test script to verify the data pipeline POC is working correctly."""

import time
from typing import Any

import requests


BASE_URL = "http://localhost:8000/api/v1"


# HTTP status constants
HTTP_OK = 200
HTTP_CREATED = 201


def test_health_endpoint() -> bool:
    """Test the health endpoint."""
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        if response.status_code != HTTP_OK:
            print(f"❌ Health check failed - Status: {response.status_code}")
            return False
    except requests.RequestException as e:
        print(f"❌ Health check error: {e}")
        return False
    else:
        health_data = response.json()
        print(f"✅ Health check passed - Service: {health_data['service']}")
        return True


def run_sample_pipeline() -> dict[str, Any]:
    """Run a sample pipeline and return the response."""
    pipeline_request = {
        "pipeline_name": "sample-etl",
        "input_path": "data/sample_input.csv",
        "output_path": "data/output.csv",
        "parameters": {"test": True},
    }

    try:
        print("🚀 Starting sample pipeline...")
        response = requests.post(
            f"{BASE_URL}/pipeline/run", json=pipeline_request, timeout=30
        )

        if response.status_code != HTTP_OK:
            print(f"❌ Pipeline start failed - Status: {response.status_code}")
            print(f"Response: {response.text}")
            return {}
    except requests.RequestException as e:
        print(f"❌ Pipeline start error: {e}")
        return {}
    else:
        run_data = response.json()
        print(f"✅ Pipeline started - Run ID: {run_data['run_id']}")
        return run_data


def check_pipeline_status(run_id: str) -> dict[str, Any]:
    """Check the status of a pipeline run."""
    try:
        response = requests.get(f"{BASE_URL}/pipeline/run/{run_id}", timeout=10)
        if response.status_code != HTTP_OK:
            print(f"❌ Status check failed - Status: {response.status_code}")
            return {}
        return response.json()
    except requests.RequestException as e:
        print(f"❌ Status check error: {e}")
        return {}


def wait_for_pipeline_completion(run_id: str, max_wait_seconds: int = 60) -> bool:
    """Wait for pipeline to complete."""
    print(f"⏳ Waiting for pipeline {run_id} to complete...")

    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        status_data = check_pipeline_status(run_id)
        if status_data:
            status = status_data.get("status")
            print(
                f"   Status: {status} | Stages: {status_data.get('stages_completed', 0)}/3"
            )

            if status == "completed":
                duration = status_data.get("duration_ms", 0)
                records = status_data.get("records_processed", 0)
                print(
                    f"✅ Pipeline completed! Duration: {duration}ms, Records: {records}"
                )
                return True
            if status == "failed":
                error = status_data.get("error_message", "Unknown error")
                print(f"❌ Pipeline failed: {error}")
                return False

        time.sleep(2)

    print(f"⏰ Pipeline did not complete within {max_wait_seconds} seconds")
    return False


def check_metrics() -> None:
    """Check pipeline metrics."""
    try:
        response = requests.get(f"{BASE_URL}/metrics", timeout=10)
        if response.status_code == HTTP_OK:
            metrics = response.json()
            print("📊 Pipeline Metrics:")
            print(f"   Total runs: {metrics['pipeline_runs_total']}")
            print(f"   Successful: {metrics['pipeline_runs_success']}")
            print(f"   Failed: {metrics['pipeline_runs_failed']}")
            print(f"   Avg duration: {metrics['avg_duration_ms']:.1f}ms")
        else:
            print(f"❌ Metrics check failed - Status: {response.status_code}")
    except requests.RequestException as e:
        print(f"❌ Metrics check error: {e}")


def main():
    """Run the complete test suite."""
    print("🧪 Testing Data Lineage Hub POC")
    print("=" * 50)

    # Test 1: Health check
    if not test_health_endpoint():
        print("❌ Health check failed. Is the API server running?")
        return

    # Test 2: Run pipeline
    run_data = run_sample_pipeline()
    if not run_data:
        print("❌ Failed to start pipeline")
        return

    run_id = run_data.get("run_id")
    if not run_id:
        print("❌ No run ID returned")
        return

    # Test 3: Wait for completion
    success = wait_for_pipeline_completion(run_id)

    # Test 4: Check metrics
    print("\n" + "=" * 50)
    check_metrics()

    # Summary
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests passed! The Data Lineage Hub POC is working correctly.")
        print("\n🌐 Check the following UIs:")
        print("   • Marquez: http://localhost:3000 (for lineage visualization)")
        print("   • Grafana: http://localhost:3001 (for metrics dashboards)")
        print("   • Jaeger: http://localhost:16686 (for distributed tracing)")
    else:
        print("❌ Some tests failed. Check the logs and service status.")


if __name__ == "__main__":
    main()
