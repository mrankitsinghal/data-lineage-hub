-- Create database for OTEL data
CREATE DATABASE IF NOT EXISTS otel;

-- Create table for OTEL traces (spans)
CREATE TABLE IF NOT EXISTS otel.traces (
    timestamp DateTime64(9),
    trace_id String,
    span_id String,
    parent_span_id String,
    operation_name String,
    service_name String,
    duration_ns UInt64,
    status_code String,
    span_kind String,
    namespace String,
    attributes Map(String, String),
    resource_attributes Map(String, String),
    events Array(Tuple(timestamp DateTime64(9), name String, attributes Map(String, String)))
) ENGINE = MergeTree()
ORDER BY (namespace, service_name, operation_name, timestamp)
PARTITION BY toYYYYMM(timestamp);

-- Create table for OTEL metrics
CREATE TABLE IF NOT EXISTS otel.metrics (
    timestamp DateTime64(9),
    metric_name String,
    metric_type String,
    value Float64,
    unit String,
    service_name String,
    namespace String,
    attributes Map(String, String),
    resource_attributes Map(String, String)
) ENGINE = MergeTree()
ORDER BY (namespace, service_name, metric_name, timestamp)
PARTITION BY toYYYYMM(timestamp);

-- Create table for pipeline execution tracking
CREATE TABLE IF NOT EXISTS otel.pipeline_runs (
    timestamp DateTime64(9),
    run_id String,
    pipeline_name String,
    stage String,
    status String,
    namespace String,
    duration_ms UInt64,
    records_processed UInt64,
    bytes_processed UInt64,
    error_message String
) ENGINE = MergeTree()
ORDER BY (namespace, pipeline_name, timestamp)
PARTITION BY toYYYYMM(timestamp);
