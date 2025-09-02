"""ML pipeline tracking example."""

import asyncio
from data_lineage_hub_sdk import configure, lineage_track


def main():
    """Demonstrate ML pipeline lineage tracking."""
    
    configure(
        hub_endpoint="http://localhost:8000", 
        namespace="ml-team",
        debug=True
    )
    
    print("ML Pipeline Lineage Tracking")
    print("=" * 40)
    
    @lineage_track(
        job_name="feature_extraction",
        inputs=["s3://data-lake/raw/customer_events.parquet"],
        outputs=["s3://data-lake/features/customer_features.parquet"],
        description="Extract customer behavioral features",
        tags={"pipeline": "customer_churn", "team": "ml", "stage": "feature_engineering"}
    )
    def extract_features():
        print("   Extracting customer features...")
        # Simulate feature extraction
        return {
            "features_extracted": 245,
            "customers_processed": 50000,
            "feature_version": "v1.2.0"
        }
    
    @lineage_track(
        job_name="model_training",
        inputs=[
            "s3://data-lake/features/customer_features.parquet",
            "s3://models/config/churn_model_config.yaml"
        ],
        outputs=[
            "s3://models/customer_churn/model_v1.2.pkl",
            "s3://models/customer_churn/metrics_v1.2.json"
        ],
        description="Train customer churn prediction model",
        tags={"pipeline": "customer_churn", "model_type": "xgboost", "version": "v1.2"}
    )
    def train_model(feature_data):
        print("   Training churn prediction model...")
        # Simulate model training
        return {
            "model_accuracy": 0.89,
            "model_auc": 0.92,
            "training_samples": feature_data["customers_processed"],
            "model_path": "s3://models/customer_churn/model_v1.2.pkl"
        }
    
    @lineage_track(
        job_name="model_evaluation", 
        inputs=[
            "s3://models/customer_churn/model_v1.2.pkl",
            "s3://data-lake/test/customer_test_set.parquet"
        ],
        outputs=[
            "s3://models/customer_churn/evaluation_report_v1.2.html",
            "s3://models/customer_churn/confusion_matrix_v1.2.png"
        ],
        description="Evaluate model performance on test set",
        tags={"pipeline": "customer_churn", "stage": "evaluation"}
    )
    def evaluate_model(model_results):
        print("   Evaluating model performance...")
        return {
            "test_accuracy": 0.87,
            "test_auc": 0.90,
            "precision": 0.85,
            "recall": 0.88,
            "model_approved": True
        }
    
    @lineage_track(
        job_name="model_deployment",
        inputs=["s3://models/customer_churn/model_v1.2.pkl"],
        outputs=[
            "k8s://ml-serving/customer-churn-service:v1.2",
            "registry://models/customer-churn:v1.2"
        ],
        description="Deploy model to production serving",
        tags={"pipeline": "customer_churn", "stage": "deployment", "environment": "production"}
    )
    def deploy_model(evaluation_results):
        if not evaluation_results["model_approved"]:
            raise ValueError("Model evaluation failed - deployment cancelled")
        
        print("   Deploying model to production...")
        return {
            "deployment_status": "success",
            "service_url": "https://ml-api.company.com/churn/predict",
            "model_version": "v1.2",
            "replicas": 3
        }
    
    # Execute the ML pipeline
    print("\nExecuting ML Pipeline:")
    
    try:
        feature_data = extract_features()
        model_results = train_model(feature_data)
        eval_results = evaluate_model(model_results)
        deployment_results = deploy_model(eval_results)
        
        print(f"\nPipeline completed successfully!")
        print(f"Model deployed at: {deployment_results['service_url']}")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")


async def async_ml_pipeline():
    """Demonstrate async ML pipeline operations."""
    
    print("\n\nAsync ML Operations")
    print("=" * 25)
    
    @lineage_track(
        job_name="batch_prediction",
        inputs=[
            "k8s://ml-serving/customer-churn-service:v1.2",
            "s3://data-lake/daily/customer_data_2024-01-01.parquet"
        ],
        outputs=["s3://predictions/churn_predictions_2024-01-01.parquet"],
        description="Generate daily churn predictions",
        tags={"pipeline": "batch_scoring", "date": "2024-01-01"}
    )
    async def generate_predictions():
        print("   Generating batch predictions...")
        await asyncio.sleep(0.1)  # Simulate async processing
        return {
            "predictions_generated": 50000,
            "high_risk_customers": 2500,
            "processing_time": "45 minutes"
        }
    
    @lineage_track(
        job_name="prediction_monitoring",
        inputs=["s3://predictions/churn_predictions_2024-01-01.parquet"],
        outputs=[
            "monitoring://dashboards/churn_predictions_daily",
            "alerts://slack/ml-team/prediction-alerts"
        ],
        description="Monitor prediction quality and drift",
        tags={"pipeline": "monitoring", "type": "drift_detection"}
    )
    async def monitor_predictions(prediction_results):
        print("   Monitoring prediction quality...")
        await asyncio.sleep(0.05)
        return {
            "drift_detected": False,
            "prediction_confidence": 0.85,
            "data_quality_score": 0.92,
            "alerts_triggered": 0
        }
    
    # Execute async pipeline
    prediction_results = await generate_predictions()
    monitoring_results = await monitor_predictions(prediction_results)
    
    print(f"Async pipeline completed - drift detected: {monitoring_results['drift_detected']}")


if __name__ == "__main__":
    main()
    asyncio.run(async_ml_pipeline())