from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

# Import cấu hình chung từ dag_config 
from dag_config import (
    get_default_args,
    create_spark_submit_kwargs,
    ETL_SCRIPTS,
    DAG_IDS,
    DAG_SCHEDULES,
    DAG_TAGS,
)


# Cấu hình mặc định 
default_args = get_default_args(owner="thanhvien1")


# Hàm phân nhánh: xác định chế độ nạp CSV 
def _pick_load_mode(**context):
    """
    Tự động chọn chế độ nạp dựa trên cách trigger:
    - Bấm nút Trigger trên UI (manual)  → full_load  (nạp toàn bộ)
    - Chạy theo lịch hẹn giờ (scheduled) → incremental (nạp gia tăng)
    """
    dag_run = context.get("dag_run")

    if dag_run.run_type == "manual":
        return "csv_full_load"
    return "csv_incremental_load"


# Hàm log kết quả 
def _log_completion(**context):
    """Ghi log tổng kết khi toàn bộ pipeline Bronze hoàn tất."""
    dag_run = context["dag_run"]
    mode = (dag_run.conf or {}).get("load_mode", "incremental")

    print("=" * 60)
    print("BRONZE INGESTION PIPELINE - HOÀN TẤT")
    print("=" * 60)
    print(f"  Chế độ      : {mode}")
    print(f"  DAG Run ID  : {dag_run.run_id}")
    print(f"  Thời gian   : {datetime.now().isoformat()}")
    print(f"  Các bước đã chạy:")
    print(f"    1. CSV Olist Ingestion ({mode})")
    print(f"    2. Bronze Validation")
    print("=" * 60)



#  ĐỊNH NGHĨA DAG
with DAG(
    dag_id=DAG_IDS["bronze"],
    default_args=default_args,
    description="[TV1] Nạp dữ liệu Olist CSV vào Bronze Layer + Validation",
    schedule_interval=DAG_SCHEDULES["bronze"],
    catchup=False,
    max_active_runs=1,                  
    tags=DAG_TAGS["bronze"],
) as dag:

    # Task 1: Bắt đầu 
    start = EmptyOperator(task_id="start")

    # Task 2: Phân nhánh chọn chế độ nạp CSV 
    pick_mode = BranchPythonOperator(
        task_id="pick_load_mode",
        python_callable=_pick_load_mode,
    )

    # Task 3a: CSV Full Load 
    # Nạp toàn bộ 9 bảng Olist CSV → Delta Table (ghi đè)
    csv_full_load = SparkSubmitOperator(
        task_id="csv_full_load",
        **create_spark_submit_kwargs(
            application=ETL_SCRIPTS["ingest_csv_to_bronze"],
            app_name="bronze_csv_full_load",
            application_args=["--mode", "full_load"],
        ),
    )

    # Task 3b: CSV Incremental Load 
    # Nạp gia tăng (MERGE) dữ liệu mới từ thư mục incremental
    csv_incremental_load = SparkSubmitOperator(
        task_id="csv_incremental_load",
        **create_spark_submit_kwargs(
            application=ETL_SCRIPTS["ingest_csv_to_bronze"],
            app_name="bronze_csv_incremental_load",
            application_args=["--mode", "incremental"],
        ),
    )

    # Task 4: Validate Bronze Layer 
    # Kiểm định tất cả bảng Olist Bronze đã nạp đúng và đầy đủ
    validate_bronze = SparkSubmitOperator(
        task_id="validate_bronze",
        **create_spark_submit_kwargs(
            application=ETL_SCRIPTS["validate_bronze"],
            app_name="bronze_validation",
        ),
        trigger_rule="one_success",      
    )

    # Task 5: Ghi log hoàn tất 
    log_done = PythonOperator(
        task_id="log_completion",
        python_callable=_log_completion,
    )

    # Task 6: Kết thúc 
    end = EmptyOperator(task_id="end")

    start >> pick_mode
    pick_mode >> [csv_full_load, csv_incremental_load]
    csv_full_load >> validate_bronze
    csv_incremental_load >> validate_bronze
    validate_bronze >> log_done >> end
