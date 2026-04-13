from datetime import datetime

from airflow import DAG
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
# Theo phân công trong dag_config.py, Gold Layer do thanhvien3 phụ trách
default_args = get_default_args(owner="thanhvien3")

# ĐỊNH NGHĨA DAG
with DAG(
    dag_id=DAG_IDS["gold"],
    default_args=default_args,
    description="[TV3] Xử lý và tạo các bảng Dimension/Fact cho Gold Layer (Star Schema)",
    schedule_interval=DAG_SCHEDULES["gold"], # Đã được cấu hình là None trong dag_config
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2, # Giới hạn 2 task chạy đồng thời để tránh tràn RAM (OOM)
    tags=DAG_TAGS["gold"],
) as dag:

    # Task 1: Bắt đầu 
    start_gold = EmptyOperator(task_id="start_gold")

    # Task 2: Hàm tiện ích tạo SparkSubmitOperator 
    def create_gold_task(table_name: str, task_alias: str):
        """
        Sử dụng hàm create_spark_submit_kwargs từ dag_config để đảm bảo
        cấu hình Spark, Delta Lake và S3 đồng nhất cho tất cả các bảng.
        """
        return SparkSubmitOperator(
            task_id=f"transform_{table_name}",
            **create_spark_submit_kwargs(
                application=ETL_SCRIPTS["aggregate_gold"],
                app_name=f"gold_{task_alias}",
                application_args=["--table", table_name],
                extra_conf={
                    "spark.driver.memory": "512m", 
                    "spark.executor.memory": "512m"
                }
            ),
        )

    # Task 3: Chạy script xử lý các bảng Dimension & Fact song song 
    # --- BẢNG DIMENSIONS ---
    task_dim_products  = create_gold_task('dim_products', 'products')
    task_dim_sellers   = create_gold_task('dim_sellers', 'sellers')
    task_dim_customers = create_gold_task('dim_customers', 'customers')
    task_dim_date      = create_gold_task('dim_date', 'date')

    # --- BẢNG FACTS ---
    task_fact_sales             = create_gold_task('fact_sales', 'sales')
    task_fact_order_fulfillment = create_gold_task('fact_order_fulfillment', 'fulfillment')

    # Task 4: Kết thúc 
    end_gold = EmptyOperator(task_id="end_gold")

    # Xác định luồng chạy (Pipeline Flow) 
    # Tỏa nhánh (Fan-out) để chạy song song 6 bảng, sau đó hội tụ (Fan-in) tại end_gold
    start_gold >> [
        task_dim_products, 
        task_dim_sellers, 
        task_dim_customers, 
        task_dim_date, 
        task_fact_sales, 
        task_fact_order_fulfillment
    ] >> end_gold