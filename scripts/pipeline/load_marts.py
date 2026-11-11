"""Load aggregated data marts from DWD to DWS tables in ClickHouse."""

from scripts.db.clickhouse import execute


def load_operator_performance():
    """Aggregate operator metrics from DWD SCD2 table."""
    
    # Очищаем таблицу перед загрузкой новой порции данных
    execute("TRUNCATE TABLE dws_operator_performance_daily")
    
    sql = """
    INSERT INTO dws_operator_performance_daily
    SELECT
        toDate(valid_from) AS stat_date,
        operator_id,
        count(*) AS total_requests,
        
        -- Считаем закрытые и отклоненные среди ЗАКРЫТЫХ версий (is_active=0)
        sumIf(status = 'closed', 1) AS closed_requests,
        sumIf(status = 'rejected', 1) AS rejected_requests,
        
        -- Среднее время решения: только для тех, кто реально завершился (valid_to IS NOT NULL)
        avgIf(
            dateDiff('second', valid_from, valid_to),
            valid_to IS NOT NULL AND status IN ('closed', 'rejected')
        ) AS avg_resolution_time_seconds,
        
        -- Нарушения SLA (> 2 часа)
        countIf(
            valid_to IS NOT NULL 
            AND status IN ('closed', 'rejected')
            AND dateDiff('second', valid_from, valid_to) > 7200
        ) AS sla_violation_count
        
    FROM dwd_call_center_requests_scd2
    WHERE is_active = 0  -- Берем только исторические сегменты (завершенные)
    GROUP BY stat_date, operator_id
    ORDER BY stat_date, operator_id
    """
    
    print("Loading Operator Performance...")
    try:
        execute(sql)
        print(f"Operator Performance loaded.")
    except Exception as e:
        print(f"Error loading Operator Performance: {e}")
        raise e


def load_call_center_stats():
    """Aggregate daily stats for the whole call center."""
    
    execute("TRUNCATE TABLE dws_call_center_stats_daily")
    
    # Используем CTE через подзапросы, но гарантируем уникальность дат
    # Логика: считаем общую статистику по created_at, а пик часа берем отдельно и джойним
    sql_corrected = """
    INSERT INTO dws_call_center_stats_daily
    SELECT
        base.stat_date,
        base.total_incoming,
        base.total_closed,
        base.total_rejected,
        base.avg_cycle_time_minutes,
        COALESCE(peak.peak_hr, 0) AS peak_hour
    FROM (
        -- Основной поток: статистика по датам создания заявок
        SELECT
            toDate(created_at) AS stat_date,
            count(*) AS total_incoming,
            sumIf(status = 'closed', 1) AS total_closed,
            sumIf(status = 'rejected', 1) AS total_rejected,
            avg(dateDiff('minute', created_at, updated_at)) AS avg_cycle_time_minutes
        FROM dwd_call_center_requests_scd2
        GROUP BY stat_date
    ) AS base
    LEFT JOIN (
        -- Подпоток: расчет пикового часа
        SELECT
            stat_date,
            argMax(hr, cnt) AS peak_hr
        FROM (
            SELECT
                toDate(created_at) AS stat_date,
                toHour(created_at) AS hr,
                count(*) AS cnt
            FROM dwd_call_center_requests_scd2
            GROUP BY stat_date, hr
        ) AS hourly_agg
        GROUP BY stat_date
    ) AS peak ON base.stat_date = peak.stat_date
    ORDER BY base.stat_date
    """

    print("Loading Call Center Stats...")
    try:
        execute(sql_corrected)
        print("Call Center Stats loaded.")
    except Exception as e:
        print(f"Error loading Call Center Stats: {e}")
        raise e


def refresh_marts():
    """Refresh all data marts."""
    print("Refreshing Data Marts...")
    load_operator_performance()
    load_call_center_stats()
    print("All Marts refreshed.")