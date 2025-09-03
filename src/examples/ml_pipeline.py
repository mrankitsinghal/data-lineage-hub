"""ML pipeline tracking example."""

import asyncio

from src.sdk import configure, lineage_track


def main():
    """Demonstrate ML pipeline lineage tracking."""

    configure(hub_endpoint="http://localhost:8000", namespace="ml-team", debug=True)

    @lineage_track(
        job_name="feature_extraction",
        inputs=[
            {
                "type": "s3",
                "name": "s3://data-lake/raw/customer_events.parquet",
                "format": "parquet",
                "namespace": "raw-data",
            }
        ],
        outputs=[
            {
                "type": "s3",
                "name": "s3://data-lake/features/customer_features.parquet",
                "format": "parquet",
                "namespace": "ml-features",
            }
        ],
        description="Extract customer behavioral features",
        tags={
            "pipeline": "customer_churn",
            "team": "ml",
            "stage": "feature_engineering",
        },
    )
    def extract_features():
        # Simulate feature extraction
        return {
            "features_extracted": 245,
            "customers_processed": 50000,
            "feature_version": "v1.2.0",
        }

    @lineage_track(
        job_name="model_training",
        inputs=[
            {
                "type": "s3",
                "name": "s3://data-lake/features/customer_features.parquet",
                "format": "parquet",
                "namespace": "ml-features",
            },
            {
                "type": "s3",
                "name": "s3://models/config/churn_model_config.yaml",
                "format": "yaml",
                "namespace": "ml-config",
            },
        ],
        outputs=[
            {
                "type": "s3",
                "name": "s3://models/customer_churn/model_v1.2.pkl",
                "format": "binary",
                "namespace": "ml-models",
            },
            {
                "type": "s3",
                "name": "s3://models/customer_churn/metrics_v1.2.json",
                "format": "json",
                "namespace": "ml-models",
            },
        ],
        description="Train customer churn prediction model",
        tags={"pipeline": "customer_churn", "model_type": "xgboost", "version": "v1.2"},
    )
    def train_model(feature_data):
        # Simulate model training
        return {
            "model_accuracy": 0.89,
            "model_auc": 0.92,
            "training_samples": feature_data["customers_processed"],
            "model_path": "s3://models/customer_churn/model_v1.2.pkl",
        }

    @lineage_track(
        job_name="model_evaluation",
        inputs=[
            {
                "type": "s3",
                "name": "s3://models/customer_churn/model_v1.2.pkl",
                "format": "binary",
                "namespace": "ml-models",
            },
            {
                "type": "s3",
                "name": "s3://data-lake/test/customer_test_set.parquet",
                "format": "parquet",
                "namespace": "test-data",
            },
        ],
        outputs=[
            {
                "type": "s3",
                "name": "s3://models/customer_churn/evaluation_report_v1.2.html",
                "format": "text",
                "namespace": "ml-reports",
            },
            {
                "type": "s3",
                "name": "s3://models/customer_churn/confusion_matrix_v1.2.png",
                "format": "binary",
                "namespace": "ml-reports",
            },
        ],
        description="Evaluate model performance on test set",
        tags={"pipeline": "customer_churn", "stage": "evaluation"},
    )
    def evaluate_model(model_results):
        return {
            "test_accuracy": 0.87,
            "test_auc": 0.90,
            "precision": 0.85,
            "recall": 0.88,
            "model_approved": True,
        }

    @lineage_track(
        job_name="model_deployment",
        inputs=[
            {
                "type": "s3",
                "name": "s3://models/customer_churn/model_v1.2.pkl",
                "format": "binary",
                "namespace": "ml-models",
            }
        ],
        outputs=[
            {
                "type": "api",
                "name": "k8s://ml-serving/customer-churn-service:v1.2",
                "format": "binary",
                "namespace": "k8s-services",
            },
            {
                "type": "api",
                "name": "registry://models/customer-churn:v1.2",
                "format": "binary",
                "namespace": "model-registry",
            },
        ],
        description="Deploy model to production serving",
        tags={
            "pipeline": "customer_churn",
            "stage": "deployment",
            "environment": "production",
        },
    )
    def deploy_model(evaluation_results):
        if not evaluation_results["model_approved"]:
            raise ValueError("Model evaluation failed - deployment cancelled")

        return {
            "deployment_status": "success",
            "service_url": "https://ml-api.company.com/churn/predict",
            "model_version": "v1.2",
            "replicas": 3,
        }

    # Execute the ML pipeline

    try:
        feature_data = extract_features()
        model_results = train_model(feature_data)
        eval_results = evaluate_model(model_results)
        deploy_model(eval_results)

    except Exception:
        pass


async def async_ml_pipeline():
    """Demonstrate async ML pipeline operations."""

    @lineage_track(
        job_name="batch_prediction",
        inputs=[
            {
                "type": "api",
                "name": "k8s://ml-serving/customer-churn-service:v1.2",
                "format": "binary",
                "namespace": "k8s-services",
            },
            {
                "type": "s3",
                "name": "s3://data-lake/daily/customer_data_2024-01-01.parquet",
                "format": "parquet",
                "namespace": "daily-data",
            },
        ],
        outputs=[
            {
                "type": "s3",
                "name": "s3://predictions/churn_predictions_2024-01-01.parquet",
                "format": "parquet",
                "namespace": "predictions",
            }
        ],
        description="Generate daily churn predictions",
        tags={"pipeline": "batch_scoring", "date": "2024-01-01"},
    )
    async def generate_predictions():
        await asyncio.sleep(0.1)  # Simulate async processing
        return {
            "predictions_generated": 50000,
            "high_risk_customers": 2500,
            "processing_time": "45 minutes",
        }

    @lineage_track(
        job_name="prediction_monitoring",
        inputs=[
            {
                "type": "s3",
                "name": "s3://predictions/churn_predictions_2024-01-01.parquet",
                "format": "parquet",
                "namespace": "predictions",
            }
        ],
        outputs=[
            {
                "type": "api",
                "name": "monitoring://dashboards/churn_predictions_daily",
                "format": "json",
                "namespace": "monitoring",
            },
            {
                "type": "api",
                "name": "alerts://slack/ml-team/prediction-alerts",
                "format": "json",
                "namespace": "alerts",
            },
        ],
        description="Monitor prediction quality and drift",
        tags={"pipeline": "monitoring", "type": "drift_detection"},
    )
    async def monitor_predictions(prediction_results):
        await asyncio.sleep(0.05)
        return {
            "drift_detected": False,
            "prediction_confidence": 0.85,
            "data_quality_score": 0.92,
            "alerts_triggered": 0,
        }

    # Execute async pipeline
    prediction_results = await generate_predictions()
    await monitor_predictions(prediction_results)


if __name__ == "__main__":
    main()
    asyncio.run(async_ml_pipeline())
