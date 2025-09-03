"""
Simple lineage tracking examples using explicit dict-based specifications.
Demonstrates KISS principle approach for different team usage patterns.
"""

from src.sdk import configure, lineage_track


# Configure the SDK
configure(
    hub_endpoint="http://localhost:8000",
    api_key="demo-api-key",
    namespace="simple-lineage-example",
    debug=True,
)

# Note: Job relationships are established through dataset lineage chains
# When Job A outputs Dataset X and Job B inputs Dataset X, Marquez shows Job A as upstream of Job B


# =============================================================================
# DATA ENGINEERING TEAM EXAMPLES
# =============================================================================


@lineage_track(
    job_name="s3_to_warehouse_ingestion",
    description="Ingest raw events from S3 to ClickHouse warehouse",
    inputs=[
        {
            "type": "s3",
            "name": "s3://data-lake/raw/events/2024/01/",
            "format": "parquet",
            "namespace": "production",
        }
    ],
    outputs=[
        {
            "type": "clickhouse",
            "name": "warehouse.raw_events",
            "format": "table",
            "namespace": "analytics",
        }
    ],
    tags={"team": "data-engineering", "stage": "ingestion"},
)
def ingest_events_to_warehouse():
    """Data Engineering: S3 → ClickHouse"""
    return {"rows_processed": 50000, "duration": "2.3 minutes"}


@lineage_track(
    job_name="multi_source_etl",
    description="ETL combining MySQL users and Postgres orders",
    inputs=[
        {
            "type": "mysql",
            "name": "users.user_profiles",
            "format": "table",
            "namespace": "prod-db",
        },
        {
            "type": "postgres",
            "name": "public.orders",
            "format": "table",
            "namespace": "warehouse",
        },
    ],
    outputs=[
        {
            "type": "clickhouse",
            "name": "analytics.user_orders",
            "format": "table",
            "namespace": "analytics",
        }
    ],
    tags={"team": "data-engineering", "stage": "etl"},
)
def combine_user_orders():
    """Data Engineering: Multiple DB sources → Analytics warehouse"""
    return {"users": 10000, "orders": 25000, "joined_records": 23500}


# =============================================================================
# ANALYTICS TEAM EXAMPLES
# =============================================================================


@lineage_track(
    job_name="daily_analytics_report",
    description="Generate daily analytics from ClickHouse and export to S3",
    inputs=[
        {
            "type": "clickhouse",
            "name": "analytics.user_orders",
            "format": "table",
            "namespace": "analytics",
        }
    ],
    outputs=[
        {
            "type": "s3",
            "name": "s3://reports/daily/analytics_summary.csv",
            "format": "csv",
            "namespace": "reporting",
        },
        {
            "type": "dataframe",
            "name": "analytics_df",
            "format": "table",
            "namespace": "memory",
        },
    ],
    tags={"team": "analytics", "report": "daily"},
)
def generate_daily_report():
    """Analytics: ClickHouse → S3 + DataFrame"""
    return {
        "total_orders": 1250,
        "revenue": 125000.50,
        "top_product": "Widget Pro",
        "report_path": "s3://reports/daily/analytics_summary.csv",
    }


@lineage_track(
    job_name="dataframe_processing",
    description="Process DataFrames and save results",
    inputs=[
        {
            "type": "dataframe",
            "name": "raw_data_df",
            "format": "table",
            "namespace": "memory",
        },
        {
            "type": "file",
            "name": "/data/reference/categories.csv",
            "format": "csv",
            "namespace": "local",
        },
    ],
    outputs=[
        {
            "type": "dataframe",
            "name": "processed_data_df",
            "format": "table",
            "namespace": "memory",
        }
    ],
    tags={"team": "analytics", "operation": "transform"},
)
def process_dataframes():
    """Analytics: DataFrame + File → DataFrame"""
    return {"processed_rows": 8500, "new_columns": 3}


# =============================================================================
# ML ENGINEERING TEAM EXAMPLES
# =============================================================================


@lineage_track(
    job_name="ml_feature_engineering",
    description="Create ML features from multiple sources",
    inputs=[
        {
            "type": "s3",
            "name": "s3://ml-data/features/user_behavior.parquet",
            "format": "parquet",
            "namespace": "ml",
        },
        {
            "type": "api",
            "name": "https://feature-store.company.com/v1/demographics",
            "format": "json",
            "namespace": "external",
        },
        {
            "type": "clickhouse",
            "name": "analytics.user_metrics",
            "format": "table",
            "namespace": "analytics",
        },
    ],
    outputs=[
        {
            "type": "s3",
            "name": "s3://ml-data/processed/ml_features.parquet",
            "format": "parquet",
            "namespace": "ml",
        }
    ],
    tags={"team": "ml-engineering", "stage": "feature-engineering"},
)
def create_ml_features():
    """ML Engineering: S3 + API + ClickHouse → S3"""
    return {
        "feature_count": 47,
        "samples": 100000,
        "feature_importance": [0.23, 0.19, 0.15, 0.12, 0.08],
    }


@lineage_track(
    job_name="model_training",
    description="Train ML model and save artifacts",
    inputs=[
        {
            "type": "s3",
            "name": "s3://ml-data/processed/ml_features.parquet",
            "format": "parquet",
            "namespace": "ml",
        }
    ],
    outputs=[
        {
            "type": "file",
            "name": "/models/customer_churn_v2.pkl",
            "format": "binary",
            "namespace": "local",
        },
        {
            "type": "s3",
            "name": "s3://ml-models/customer_churn_v2/",
            "format": "binary",
            "namespace": "ml",
        },
    ],
    tags={"team": "ml-engineering", "stage": "training", "model": "customer_churn"},
)
def train_churn_model():
    """ML Engineering: S3 features → Model artifacts"""
    return {
        "model_type": "RandomForest",
        "accuracy": 0.89,
        "precision": 0.85,
        "recall": 0.82,
        "model_size": "45MB",
    }


# =============================================================================
# DATA SCIENCE TEAM EXAMPLES
# =============================================================================


@lineage_track(
    job_name="hypothesis_testing",
    description="A/B test analysis using multiple data sources",
    inputs=[
        {
            "type": "event",
            "name": "user-events",
            "format": "avro",
            "namespace": "kafka",
        },
        {
            "type": "api",
            "name": "https://external-data.company.com/experiments/ab-test-123",
            "format": "json",
            "namespace": "external",
        },
        {
            "type": "file",
            "name": "/data/experiments/control_group.csv",
            "format": "csv",
            "namespace": "local",
        },
    ],
    outputs=[
        {
            "type": "dataframe",
            "name": "test_results_df",
            "format": "table",
            "namespace": "memory",
        },
        {
            "type": "file",
            "name": "/reports/ab_test_analysis.json",
            "format": "json",
            "namespace": "local",
        },
    ],
    tags={
        "team": "data-science",
        "experiment": "ab-test-123",
        "type": "hypothesis-testing",
    },
)
def analyze_ab_test():
    """Data Science: Kafka events + API + CSV → Analysis results"""
    return {
        "test_significant": True,
        "p_value": 0.032,
        "effect_size": 0.15,
        "control_conversion": 0.12,
        "test_conversion": 0.14,
        "confidence": 0.95,
    }


@lineage_track(
    job_name="exploratory_analysis",
    description="Exploratory data analysis across multiple formats",
    inputs=[
        {
            "type": "postgres",
            "name": "analytics.daily_metrics",
            "format": "table",
            "namespace": "warehouse",
        },
        {
            "type": "s3",
            "name": "s3://external-data/market_trends.xml",
            "format": "xml",
            "namespace": "external",
        },
        {
            "type": "api",
            "name": "https://api.competitor.com/pricing",
            "format": "json",
            "namespace": "partner",
        },
    ],
    outputs=[
        {
            "type": "dataframe",
            "name": "insights_df",
            "format": "table",
            "namespace": "memory",
        },
        {
            "type": "file",
            "name": "/analysis/market_insights.yaml",
            "format": "yaml",
            "namespace": "local",
        },
    ],
    tags={"team": "data-science", "type": "exploration", "domain": "market-analysis"},
)
@lineage_track(
    job_name="iceberg_table_maintenance",
    inputs=[
        {
            "type": "iceberg",
            "name": "warehouse.dim_users",
            "format": "table",
            "namespace": "data-lake",
        },
        {
            "type": "iceberg",
            "name": "warehouse.fact_orders",
            "format": "table",
            "namespace": "data-lake",
        },
    ],
    outputs=[
        {
            "type": "iceberg",
            "name": "warehouse.user_order_summary",
            "format": "table",
            "namespace": "data-lake",
        },
        {
            "type": "s3",
            "name": "s3://data-lake/snapshots/user_order_summary_v2.parquet",
            "format": "parquet",
            "namespace": "backup",
        },
    ],
    tags={"team": "data-platform", "type": "maintenance", "technology": "iceberg"},
)
def maintain_iceberg_tables():
    """Maintain Iceberg tables with compaction and snapshotting."""
    return "iceberg_maintenance_complete"


def explore_market_data():
    """Data Science: Multi-format exploration"""
    return {
        "key_insights": [
            "Market growth trending upward",
            "Competitor pricing 15% higher",
            "Seasonal pattern detected",
        ],
        "correlation_scores": [0.73, 0.68, 0.45],
        "data_quality_score": 0.87,
    }


# =============================================================================
# EVENT STREAMING TEAM EXAMPLES
# =============================================================================


@lineage_track(
    job_name="stream_processing",
    description="Process streaming events and enrich with reference data",
    inputs=[
        {
            "type": "event",
            "name": "order-events",
            "format": "avro",
            "namespace": "kafka",
        },
        {
            "type": "event",
            "name": "user-activity",
            "format": "json",
            "namespace": "kinesis",
        },
        {
            "type": "mysql",
            "name": "reference.product_catalog",
            "format": "table",
            "namespace": "prod-db",
        },
    ],
    outputs=[
        {
            "type": "event",
            "name": "enriched-orders",
            "format": "json",
            "namespace": "sqs",
        },
        {
            "type": "clickhouse",
            "name": "realtime.enriched_orders",
            "format": "table",
            "namespace": "streaming",
        },
    ],
    tags={"team": "streaming", "pattern": "enrichment"},
)
def process_order_stream():
    """Streaming: Multiple event sources + DB → Enriched stream + Storage"""
    return {
        "events_processed": 15000,
        "enrichment_rate": 0.95,
        "latency_p99": "120ms",
        "throughput": "1250 events/sec",
    }


# =============================================================================
# DEMONSTRATION FUNCTIONS
# =============================================================================


def run_data_engineering_examples():
    """Run Data Engineering team examples."""

    ingest_events_to_warehouse()

    combine_user_orders()


def run_analytics_examples():
    """Run Analytics team examples."""

    generate_daily_report()

    process_dataframes()


def run_ml_examples():
    """Run ML Engineering team examples."""

    create_ml_features()

    train_churn_model()


def run_data_science_examples():
    """Run Data Science team examples."""

    analyze_ab_test()

    explore_market_data()


def run_streaming_examples():
    """Run streaming team examples."""

    process_order_stream()


def main():
    """Run all team examples."""

    run_data_engineering_examples()
    run_analytics_examples()
    run_ml_examples()
    run_data_science_examples()
    run_streaming_examples()


if __name__ == "__main__":
    main()
