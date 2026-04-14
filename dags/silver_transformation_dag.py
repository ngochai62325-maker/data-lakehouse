from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from dag_config import (
    get_default_args,
    create_spark_submit_kwargs,
    ETL_SCRIPTS,
    DAG_IDS,
    DAG_SCHEDULES,
    DAG_TAGS,
)

# Khởi tạo đối số mặc định cho cấu trúc luồng DAG, thiết lập quyền sở hữu
default_args = get_default_args(owner="thanhvien2")

with DAG(
    dag_id=DAG_IDS["silver"],
    default_args=default_args,
    description="[TV2] Xử lý và làm sạch dữ liệu từ Bronze Layer sang Silver Layer",
    schedule_interval=DAG_SCHEDULES["silver"],
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS["silver"],
) as dag:

    # Task 1: Nút khởi tạo luồng dữ liệu
    start = EmptyOperator(task_id="start")

    # Task 2: Bộ cảm biến (Sensor) không còn cần thiết do Master Pipeline (TriggerDagRunOperator) đã tự động wait_for_completion=True
    # Do đó, chỉ cần gọi lớp Silver là đã biết chắc lớp Bronze đã chạy xong.

    # Task 3: Kích hoạt ứng dụng Spark Submit để thực hiện tiền xử lý dữ liệu cho tất cả các bảng
    transform_bronze_to_silver = SparkSubmitOperator(
        task_id="transform_bronze_to_silver_all",
        **create_spark_submit_kwargs(
            application=ETL_SCRIPTS["transform_silver"],
            app_name="silver_transformation_all_tables",
            application_args=["--table", "all"],
        ),
    )

    # Task 4: Nút kết thúc luồng dữ liệu
    end = EmptyOperator(task_id="end")

    # Thiết lập trình tự luồng thực thi công việc (Pipeline Flow)
    start >> transform_bronze_to_silver >> end