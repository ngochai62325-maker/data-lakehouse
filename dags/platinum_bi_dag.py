from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_config import (
    get_default_args,
    create_spark_submit_kwargs,
    DAG_IDS,
    DAG_SCHEDULES,
    DAG_TAGS,
    ETL_SCRIPTS,
)


# ─── Cấu hình mặc định ──────────────────────────────────────────────────────
default_args = get_default_args(owner="thanhvien4", retries=2)

# ─── Danh sách các mart cần build ────────────────────────────────────────────
PLATINUM_MARTS = [
    "sales_summary_mart",
    "customer_mart",
    "product_mart",
    "kpi_summary",
]


# ─── Hàm log ─────────────────────────────────────────────────────────────────
def _log_platinum_start(**context):
    """Ghi log khi Platinum pipeline bắt đầu."""
    print("=" * 70)
    print("  PLATINUM BI LAYER PIPELINE — BẮT ĐẦU")
    print("=" * 70)
    print(f"  DAG Run ID  : {context['dag_run'].run_id}")
    print(f"  Run Type    : {context['dag_run'].run_type}")
    print(f"  Thời gian   : {datetime.now().isoformat()}")
    print(f"  Số mart     : {len(PLATINUM_MARTS)}")
    print(f"  Danh sách   :")
    for i, mart in enumerate(PLATINUM_MARTS, 1):
        print(f"    {i}. {mart}")
    print("=" * 70)


def _log_platinum_end(**context):
    """Ghi log khi Platinum pipeline hoàn tất."""
    print("=" * 70)
    print("  PLATINUM BI LAYER PIPELINE — HOÀN TẤT")
    print("=" * 70)
    print(f"  DAG Run ID  : {context['dag_run'].run_id}")
    print(f"  Thời gian   : {datetime.now().isoformat()}")
    print(f"  Tất cả {len(PLATINUM_MARTS)} mart đã được tạo thành công!")
    print("=" * 70)


#  ĐỊNH NGHĨA DAG — PLATINUM BI LAYER
with DAG(
    dag_id=DAG_IDS["platinum"],
    default_args=default_args,
    description="[TV4] Platinum BI Layer — Data Mart & KPI Transformations từ Gold",
    schedule_interval=DAG_SCHEDULES["platinum"],
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS["platinum"],
) as dag:

    # ── Start ─────────────────────────────────────────────────────────────────
    start = PythonOperator(
        task_id="platinum_start",
        python_callable=_log_platinum_start,
    )

    # ── Tạo các task cho từng mart ────────────────────────────────────────────
    # Mỗi mart là một SparkSubmitOperator task riêng biệt.
    mart_tasks = {}

    for mart_name in PLATINUM_MARTS:
        task_id = f"platinum_{mart_name}"
        
        mart_tasks[mart_name] = SparkSubmitOperator(
            task_id=task_id,
            **create_spark_submit_kwargs(
                application=ETL_SCRIPTS["gold_to_platinum"],
                app_name=f"platinum_{mart_name}",
                application_args=["--table", mart_name],
            ),
        )

    # ── End ───────────────────────────────────────────────────────────────────
    end = PythonOperator(
        task_id="platinum_end",
        python_callable=_log_platinum_end,
    )

    # TASK DEPENDENCIES
    #  Phase 1 (Parallel — independent base marts):
    #    Sales Summary, Customer, Product

    #  Phase 2 (Depends on all base marts):
    #    KPI Summary (uses fact_sales + fact_order_fulfillment,
    #    runs after base marts to ensure consistency)

    # Phase 1: Base marts can run in parallel
    phase1_tasks = [
        mart_tasks["sales_summary_mart"],
        mart_tasks["customer_mart"],
        mart_tasks["product_mart"],
    ]

    # Phase 2: KPI summary aggregates global metrics — run after base marts
    phase2_tasks = [
        mart_tasks["kpi_summary"],
    ]

    # Wire dependencies
    # start → phase1 (parallel) → phase2 → end
    start >> phase1_tasks

    for t in phase1_tasks:
        t >> phase2_tasks

    for t in phase2_tasks:
        t >> end
