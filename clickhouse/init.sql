-- Run automatically on container startup via Docker volume mount

CREATE DATABASE IF NOT EXISTS event_pipeline;

-- Main events table - columnar vectorisation for optimized time-series analytics
CREATE TABLE IF NOT EXISTS event_pipeline.events_queue (
    event_id String,
    user_id String,
    event_type LowCardinality(String),
    product_id String,
    price Float32,
    timestamp DateTime64(3),
    category LowCardinality(String),
    processed_at DateTime DEFAULT now(),
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, event_type)
SETTINGS index_granularity = 8192;

-- Quarantine table: why did a data quality check fail?
CREATE TABLE IF NOT EXISTS event_pipeline.events_quarantine (
    event_id String,
    user_id String,
    event_type String,
    product_id String,
    price Float32,
    timestamp DateTime64(3),
    category String,
    processed_at DateTime DEFAULT now(),
    rejection_reason String,
    quality_check_name String,
    raw_payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, event_type);

-- Schema violations
CREATE TABLE IF NOT EXISTS event_pipeline.schema_violations (
    raw_payload String,
    detected_at DateTime DEFAULT now(),
    violation_type LowCardinality(String),
    details String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(detected_at)
ORDER BY detected_at;
