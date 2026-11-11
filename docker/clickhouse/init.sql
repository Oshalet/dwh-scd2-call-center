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

-- ODS: сырые данные из источника, append-only
CREATE TABLE IF NOT EXISTS ods_call_center_requests
(
    id UInt64,
    client_phone String,
    operator_id UInt32,
    status String,
    created_at DateTime,
    updated_at DateTime,
    _loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (id, updated_at);

-- Витрина 1: Производительность операторов (Daily Snapshot)
CREATE TABLE IF NOT EXISTS dws_operator_performance_daily
(
    stat_date Date,
    operator_id UInt32,
    total_requests UInt64,          -- Всего обращений к оператору за день
    closed_requests UInt64,         -- Успешно закрытых
    rejected_requests UInt64,       -- Отклоненных
    avg_resolution_time_seconds Float64, -- Среднее время от new до terminal status
    sla_violation_count UInt64      -- Количество заявок, решенных дольше SLA (например, > 2 часов)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY (stat_date, operator_id);

-- Витрина 2: Общая статистика колл-центра (Daily Aggregation)
CREATE TABLE IF NOT EXISTS dws_call_center_stats_daily
(
    stat_date Date,
    total_incoming UInt64,          -- Всего новых заявок за день
    total_closed UInt64,            -- Всего закрытых за день
    total_rejected UInt64,          -- Всего отклоненных за день
    avg_cycle_time_minutes Float64, -- Среднее время жизни заявки (от created_at до valid_to)
    peak_hour Int8                  -- Час пиковой нагрузки (0-23)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY stat_date;