#!/bin/bash

# Data Lineage Hub POC Startup Script

set -e

echo "🚀 Starting Data Lineage Hub POC..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

# Start infrastructure services
echo "📦 Starting infrastructure services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for Kafka
echo "   Waiting for Kafka..."
while ! docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 2
done

# Create Kafka topics
echo "🔧 Creating Kafka topics..."
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic openlineage-events --partitions 3 --replication-factor 1 --if-not-exists
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic otel-spans --partitions 3 --replication-factor 1 --if-not-exists
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic otel-metrics --partitions 3 --replication-factor 1 --if-not-exists

# Wait for Marquez
echo "   Waiting for Marquez..."
while ! curl -s http://localhost:5000/api/v1/namespaces > /dev/null; do
    sleep 2
done

# Wait for ClickHouse
echo "   Waiting for ClickHouse..."
while ! curl -s http://localhost:8123/ > /dev/null; do
    sleep 2
done

echo "✅ All services are ready!"
echo ""
echo "🌐 Service URLs:"
echo "   • API Server: http://localhost:8000"
echo "   • API Docs: http://localhost:8000/docs"
echo "   • Marquez UI: http://localhost:3000"
echo "   • Grafana: http://localhost:3001 (admin/admin)"
echo "   • Jaeger: http://localhost:16686"
echo ""
echo "🚀 You can now start the API server with:"
echo "   python -m src.main"
echo ""
echo "🔄 And start the consumers with:"
echo "   python -m src.consumers.lineage_consumer"
echo "   python -m src.consumers.otel_consumer"
echo ""
echo "📋 To run a sample pipeline:"
echo "   curl -X POST http://localhost:8000/api/v1/pipeline/run \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d '{\"pipeline_name\": \"sample-etl\", \"input_path\": \"data/sample_input.csv\", \"output_path\": \"data/output.csv\"}'"
