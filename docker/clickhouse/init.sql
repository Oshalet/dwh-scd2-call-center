-- Таблица с историей изменений (SCD2) в ClickHouse
CREATE TABLE IF NOT EXISTS dwd_call_center_requests_scd2
(
    request_id UInt64,
    client_phone String,
    operator_id UInt32,
    status String,
    created_at DateTime,
    updated_at DateTime,
    valid_from DateTime,
    valid_to Nullable(DateTime),
    is_active UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(valid_from)
ORDER BY (request_id, valid_from);