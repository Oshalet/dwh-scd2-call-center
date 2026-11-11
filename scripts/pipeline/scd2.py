"""SCD Type 2 transformation: ODS → DWD."""

from datetime import datetime
from typing import List, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, IntegerType

from scripts.db.clickhouse import fetch_all, insert_rows

ODS_TABLE = "ods_call_center_requests"
DWD_TABLE = "dwd_call_center_requests_scd2"


def get_spark() -> SparkSession:
    """Create or get Spark session in local mode."""
    return (
        SparkSession.builder
        .appName("scd2_transformation")
        .master("local[*]")  # Использовать все доступные ядра
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def ods_to_dataframe(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """Read ODS table into Spark DataFrame."""
    rows = fetch_all(f"SELECT * FROM {ODS_TABLE} ORDER BY id, updated_at")
    
    if not rows:
        schema = StructType([
            StructField("id", LongType()),
            StructField("client_phone", StringType()),
            StructField("operator_id", IntegerType()),
            StructField("status", StringType()),
            StructField("created_at", TimestampType()),
            StructField("updated_at", TimestampType()),
            StructField("_loaded_at", TimestampType()),
        ])
        return spark.createDataFrame([], schema)
    
    return spark.createDataFrame(rows)

def dwd_to_dataframe(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """Read current DWD table into Spark DataFrame with explicit schema."""
    
    # Определяем схему явно один раз
    schema = StructType([
        StructField("request_id", LongType(), nullable=False),
        StructField("client_phone", StringType(), nullable=True),
        StructField("operator_id", IntegerType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("valid_from", TimestampType(), nullable=True),
        StructField("valid_to", TimestampType(), nullable=True),  # Ключевой момент: nullable=True
        StructField("is_active", IntegerType(), nullable=True),
    ])
    
    rows = fetch_all(f"SELECT * FROM {DWD_TABLE}")
    
    if not rows:
        return spark.createDataFrame([], schema)

    
    return spark.createDataFrame(rows, schema=schema)


def apply_scd2(ods_df: "pyspark.sql.DataFrame", dwd_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    Apply SCD Type 2 logic using window functions for robustness.
    """
    
    # 1. Получаем самую свежую версию каждой заявки из ODS
    # Используем row_number() PARTITION BY id ORDER BY updated_at DESC
    from pyspark.sql.window import Window
    
    w_spec = Window.partitionBy("id").orderBy(F.col("updated_at").desc())
    
    ods_latest = (
        ods_df
        .withColumn("rn", F.row_number().over(w_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        # Переименуем колонки под ожидаемые имена для join, если нужно, но здесь они совпадают
    )
    
    # 2. Активные версии в DWD
    dwd_active = dwd_df.filter(F.col("is_active") == 1)
    
    # --- ЛОГИКА ИЗМЕНЕНИЙ ---
    
    # Join ODS latest с DWD active
    joined_state = ods_latest.alias("ods").join(
        dwd_active.alias("dwd"),
        F.col("ods.id") == F.col("dwd.request_id"),
        "inner"
    )
    
    # Заявки, которые ЕСТЬ в обоих, но статус разный -> CHANGED
    changed_ids_df = joined_state.filter(F.col("ods.status") != F.col("dwd.status")).select(F.col("ods.id").alias("changed_id"))
    
    # Список ID всех изменённых заявок
    changed_ids_list = [row.changed_id for row in changed_ids_df.collect()]
    
    # --- ПОСТРОЕНИЕ РЕЗУЛЬТАТОВ ---
    
    # A. Новые заявки (в ODS, но нет в DWD active)
    new_requests_df = ods_latest.join(dwd_active, ods_latest.id == dwd_active.request_id, "left_anti")
    
    new_versions = new_requests_df.select(
        F.col("id").alias("request_id"),
        "client_phone",
        "operator_id",
        "status",
        "created_at",
        "updated_at",
        F.col("updated_at").alias("valid_from"),
        F.lit(None).cast(TimestampType()).alias("valid_to"),
        F.lit(1).alias("is_active"),
    )
    
    # B. Изменённые заявки
    changed_rows_full = joined_state.filter(F.col("ods.status") != F.col("dwd.status"))
    
    # Закрываем старую версию
    closed_versions = changed_rows_full.select(
        F.col("dwd.request_id"),
        F.col("dwd.client_phone"),
        F.col("dwd.operator_id"),
        F.col("dwd.status"),       # Старый статус
        F.col("dwd.created_at"),
        F.col("dwd.updated_at"),   
        F.col("dwd.valid_from"),
        F.col("ods.updated_at").alias("valid_to"), # Новое время закрытия
        F.lit(0).alias("is_active"),
    )
    
    # Создаём новую активную версию
    new_active_versions = changed_rows_full.select(
        F.col("ods.id").alias("request_id"),
        F.col("ods.client_phone"),
        F.col("ods.operator_id"),
        F.col("ods.status"),       # Новый статус
        F.col("ods.created_at"),
        F.col("ods.updated_at"),
        F.col("ods.updated_at").alias("valid_from"),
        F.lit(None).cast(TimestampType()).alias("valid_to"),
        F.lit(1).alias("is_active"),
    )
    
    # C. Неизменённые заявки
    if changed_ids_list:
        unchanged = dwd_active.filter(~F.col("request_id").isin(changed_ids_list))
    else:
        unchanged = dwd_active
        
    # D. Исторические неактивные версии
    dwd_inactive = dwd_df.filter(F.col("is_active") == 0)
    
    # --- ОБЪЕДИНЕНИЕ ---
    result = (
        dwd_inactive
        .unionByName(closed_versions)
        .unionByName(unchanged)
        .unionByName(new_versions)
        .unionByName(new_active_versions)
    )
    
    return result

def dataframe_to_rows(df: "pyspark.sql.DataFrame") -> List[Dict]:
    """Convert Spark DataFrame to list of dicts for ClickHouse insert."""
    return [row.asDict() for row in df.collect()]


def transform_scd2() -> int:
    """
    Main SCD2 transformation: ODS → DWD.
    Returns number of rows in resulting DWD table.
    """
    spark = get_spark()
    
    try:
        ods_df = ods_to_dataframe(spark)
        dwd_df = dwd_to_dataframe(spark)
        
        result_df = apply_scd2(ods_df, dwd_df)
        rows = dataframe_to_rows(result_df)
        
        if not rows:
            return 0
        
        # Перезаписываем DWD таблицу (truncate + insert)
        from scripts.db.clickhouse import get_client
        with get_client() as client:
            client.command(f"TRUNCATE TABLE {DWD_TABLE}")
        
        columns = ["request_id", "client_phone", "operator_id", "status", 
                   "created_at", "updated_at", "valid_from", "valid_to", "is_active"]
        insert_rows(DWD_TABLE, rows, columns)
        
        return len(rows)
    
    finally:
        spark.stop()