"""
=============================================================================
DAG: bronze_ingestion
Thành viên 1 - Infrastructure & Bronze Layer
=============================================================================
Mô tả:
    DAG tự động hóa toàn bộ luồng Ingestion nạp dữ liệu vào Bronze Layer
    (Delta Table trên S3). Bao gồm 3 bước chính:

    1. Nạp CSV Olist      (ingest_csv_to_bronze.py)  - full_load / incremental
    2. Nạp API Holidays   (ingest_api_to_bronze.py)  - ngày lễ Brazil
    3. Kiểm định dữ liệu  (validate_bronze.py)       - validation sau khi nạp

Lịch chạy:
    - Mặc định: Chạy incremental hàng ngày lúc 2:00 AM (UTC)
    - Có thể trigger thủ công với chế độ full_load qua Airflow UI
      bằng cách truyền conf: {"load_mode": "full_load"}

Luồng thực thi:
    start → pick_mode ─┬─ csv_full_load ───┬─ api_holidays → validate → log → end
                       └─ csv_incremental ─┘
=============================================================================
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator


# ─── Cấu hình mặc định ──────────────────────────────────────────────────────
default_args = {
    "owner": "thanhvien1",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ─── Cấu hình Spark Submit chung ────────────────────────────────────────────
SPARK_CONN_ID = "spark_default"          # Connection ID trong Airflow
SPARK_MASTER = "spark://spark-master:7077"

# Đường dẫn các script Ingestion (trong container Spark)
SPARK_APP_CSV = "/opt/spark/etl_pipeline/ingestion/ingest_csv_to_bronze.py"
SPARK_APP_API = "/opt/spark/etl_pipeline/ingestion/ingest_api_to_bronze.py"
SPARK_APP_VALIDATE = "/opt/spark/etl_pipeline/ingestion/validate_bronze.py"

# Các JAR cần thiết cho Delta Lake và S3 (đã có trong image Spark)
SPARK_JARS = ",".join([
    "/opt/spark/jars/delta-spark_2.12-3.3.2.jar",
    "/opt/spark/jars/delta-storage-3.3.2.jar",
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
])

# Spark configs cho Delta Lake + S3
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

# PySpark cần PYTHONPATH để import config và etl_pipeline
SPARK_ENV_VARS = {
    "PYTHONPATH": "/opt/spark:/opt/spark/etl_pipeline",
}


# ─── Hàm phân nhánh: xác định chế độ nạp CSV ────────────────────────────────
def _pick_load_mode(**context):
    """
    Kiểm tra DAG Run conf để quyết định chạy full_load hay incremental.
    - Trigger thủ công với conf {"load_mode": "full_load"} → chạy full_load
    - Lịch tự động hoặc không truyền conf → chạy incremental
    """
    conf = context.get("dag_run").conf or {}
    mode = conf.get("load_mode", "incremental")

    if mode == "full_load":
        return "csv_full_load"
    return "csv_incremental_load"


# ─── Hàm log kết quả ─────────────────────────────────────────────────────────
def _log_completion(**context):
    """Ghi log tổng kết khi toàn bộ pipeline Bronze hoàn tất."""
    dag_run = context["dag_run"]
    mode = (dag_run.conf or {}).get("load_mode", "incremental")

    print("=" * 60)
    print("BRONZE INGESTION PIPELINE - HOÀN TẤT")
    print("=" * 60)
    print(f"  Chế độ CSV  : {mode}")
    print(f"  DAG Run ID  : {dag_run.run_id}")
    print(f"  Thời gian   : {datetime.now().isoformat()}")
    print(f"  Các bước đã chạy:")
    print(f"    1. CSV Ingestion ({mode})")
    print(f"    2. API Holidays Ingestion")
    print(f"    3. Bronze Validation")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════════
#  ĐỊNH NGHĨA DAG
# ═══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id="bronze_ingestion",
    default_args=default_args,
    description="[TV1] Nạp dữ liệu vào Bronze Layer: CSV Olist + API Holidays + Validation",
    schedule_interval="0 2 * * *",      # Chạy hàng ngày lúc 02:00 UTC
    catchup=False,
    max_active_runs=1,                   # Chỉ cho chạy 1 lần tại 1 thời điểm
    tags=["bronze", "ingestion", "thanhvien1"],
) as dag:

    # ── Task 1: Bắt đầu ─────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Task 2: Phân nhánh chọn chế độ nạp CSV ──────────────────────────────
    pick_mode = BranchPythonOperator(
        task_id="pick_load_mode",
        python_callable=_pick_load_mode,
        provide_context=True,
    )

    # ── Task 3a: CSV Full Load ───────────────────────────────────────────────
    #    Nạp toàn bộ 9 bảng Olist CSV → Delta Table (ghi đè)
    csv_full_load = SparkSubmitOperator(
        task_id="csv_full_load",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APP_CSV,
        application_args=["--mode", "full_load"],
        name="bronze_csv_full_load",
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        verbose=True,
    )

    # ── Task 3b: CSV Incremental Load ────────────────────────────────────────
    #    Nạp gia tăng (MERGE) dữ liệu mới từ thư mục incremental
    csv_incremental_load = SparkSubmitOperator(
        task_id="csv_incremental_load",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APP_CSV,
        application_args=["--mode", "incremental"],
        name="bronze_csv_incremental_load",
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        verbose=True,
    )

    # ── Task 4: API Holidays Ingestion ───────────────────────────────────────
    #    Kéo dữ liệu ngày lễ Brazil (2016-2018) từ Nager.Date API
    #    Chạy song song sau khi CSV ingestion xong
    api_holidays = SparkSubmitOperator(
        task_id="api_holidays_ingestion",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APP_API,
        name="bronze_api_holidays",
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        verbose=True,
        trigger_rule="one_success",      # Chạy khi 1 trong 2 nhánh CSV thành công
    )

    # ── Task 5: Validate Bronze Layer ────────────────────────────────────────
    #    Kiểm định tất cả bảng Bronze đã nạp đúng và đầy đủ
    validate_bronze = SparkSubmitOperator(
        task_id="validate_bronze",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APP_VALIDATE,
        name="bronze_validation",
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        verbose=True,
    )

    # ── Task 6: Ghi log hoàn tất ─────────────────────────────────────────────
    log_done = PythonOperator(
        task_id="log_completion",
        python_callable=_log_completion,
        provide_context=True,
    )

    # ── Task 7: Kết thúc ─────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Luồng thực thi ──────────────────────────────────────────────────────
    #
    #                        ┌── csv_full_load ───────┐
    #   start → pick_mode ──┤                         ├→ api_holidays → validate → log → end
    #                        └── csv_incremental_load ┘
    #
    start >> pick_mode
    pick_mode >> [csv_full_load, csv_incremental_load]
    csv_full_load >> api_holidays
    csv_incremental_load >> api_holidays
    api_holidays >> validate_bronze >> log_done >> end
