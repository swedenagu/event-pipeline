CREATE DATABASE IF NOT EXISTS event_pipeline;

-- Main events queue
CREATE TABLE IF NOT EXISTS event_pipeline.events_queue (
    event_id String,
    user_id String,
    event_type LowCardinality(String),
    product_id String,
    price Float32,
    timestamp DateTime64(3),
    category String,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, event_type)
SETTINGS index_granularity = 8192;

-- Quarantine table
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

CREATE TABLE IF NOT EXISTS event_pipeline.events_kafka
(
    event_id    String,
    user_id     String,
    event_type  String,
    product_id  String,
    price       Float32,
    timestamp   DateTime64(3),
    category    String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'user-events',
    kafka_group_name                = 'clickhouse_events_consumer',
    kafka_format                    = 'JSONEachRow',
    format_avro_schema_registry_url = 'https://clickhouse:8081',
    kafka_num_consumers             = 3,
    kafka_thread_per_consumer       = 1,
    kafka_max_block_size            = 65536,
    kafka_handle_error_mode         = 'stream';

-- parse failures
CREATE MATERIALIZED VIEW event_pipeline.mv_schema_violations
TO event_pipeline.schema_violations AS
SELECT
    _raw_message  AS raw_payload,
    now()         AS detected_at,
    'parse_error' AS violation_type,
    _error        AS details
FROM event_pipeline.events_kafka
WHERE length(_error) > 0;

-- parsed but fails quality rules
CREATE MATERIALIZED VIEW event_pipeline.mv_quarantine
TO event_pipeline.events_quarantine AS
SELECT
    event_id, user_id, event_type, product_id, price, timestamp, category,
    now() AS processed_at,
    multiIf(price <= 0, 'invalid_price',
            user_id = '', 'missing_user_id',
            product_id = '', 'missing_product_id',
            'unknown') AS rejection_reason,
    'clickhouse_inline_check' AS quality_check_name,
    toJSONString(CAST((event_id, user_id, event_type, product_id, price, timestamp, category)
        AS Tuple(event_id String, user_id String, event_type String, product_id String,
                 price Float32, timestamp DateTime64(3), category String))) AS raw_payload
FROM event_pipeline.events_kafka
WHERE length(_error) = 0
  AND (price <= 0 OR user_id = '' OR product_id = '');

-- good rows
CREATE MATERIALIZED VIEW event_pipeline.mv_events
TO event_pipeline.events_queue AS
SELECT event_id, user_id, event_type, product_id, price, timestamp, category, now() AS processed_at
FROM event_pipeline.events_kafka
WHERE length(_error) = 0
  AND NOT (price <= 0 OR user_id = '' OR product_id = '');
