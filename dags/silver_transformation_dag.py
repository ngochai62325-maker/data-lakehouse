from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

# ── Import cấu hình chung từ dag_config ─────────────────────────────────────
from dag_config import (
    get_default_args,
    create_spark_submit_kwargs,
    ETL_BASE_PATH,
    DAG_IDS,
    DAG_SCHEDULES,
    DAG_TAGS,
)

# ─── Cấu hình mặc định ──────────────────────────────────────────────────────
# Theo phân công trong dag_config.py, Silver Layer do thanhvien2 phụ trách
default_args = get_default_args(owner="thanhvien2")

# Đường dẫn đến script biến đổi dữ liệu từ Bronze sang Silver
BRONZE_TO_SILVER_SCRIPT = f"{ETL_BASE_PATH}/processing/bronze_to_silver.py"

#  ĐỊNH NGHĨA DAG
with DAG(
    dag_id=DAG_IDS["silver"],
    default_args=default_args,
    description="[TV2] Xử lý và làm sạch dữ liệu từ Bronze Layer sang Silver Layer",
    schedule_interval=DAG_SCHEDULES["silver"],
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS["silver"],
) as dag:

    # ── Task 1: Bắt đầu ─────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")


    # ── Task 3: Chạy script Spark chuyển đổi Bronze -> Silver ───────────────
    # Chúng ta nạp đối số --table all để xử lý toàn bộ các bảng trong script
    transform_bronze_to_silver = SparkSubmitOperator(
        task_id="transform_bronze_to_silver_all",
        **create_spark_submit_kwargs(
            application=BRONZE_TO_SILVER_SCRIPT,
            app_name="silver_transformation_all_tables",
            application_args=["--table", "all"],
        ),
    )

    # ── Task 4: Kết thúc ─────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Xác định luồng chạy (Pipeline Flow) ─────────────────────────────────
    start >> transform_bronze_to_silver >> end
