from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dag_config import (
    get_default_args,
    DAG_IDS,
    DAG_SCHEDULES,
    DAG_TAGS,
)


# Default Args 
default_args = get_default_args(owner="admin", retries=1)


# Hàm log 
def _log_pipeline_start(**context):
    """Ghi log khi Master Pipeline bắt đầu chạy."""
    print("=" * 70)
    print("  MASTER DATA LAKEHOUSE PIPELINE — BẮT ĐẦU")
    print("=" * 70)
    print(f"  DAG Run ID  : {context['dag_run'].run_id}")
    print(f"  Run Type    : {context['dag_run'].run_type}")
    print(f"  Thời gian   : {datetime.now().isoformat()}")
    print(f"  Thứ tự chạy : Bronze → Silver → Gold → Platinum")
    print("=" * 70)


def _log_pipeline_end(**context):
    """Ghi log khi Master Pipeline hoàn tất."""
    print("=" * 70)
    print("  MASTER DATA LAKEHOUSE PIPELINE — HOÀN TẤT")
    print("=" * 70)
    print(f"  DAG Run ID  : {context['dag_run'].run_id}")
    print(f"  Thời gian   : {datetime.now().isoformat()}")
    print("  Tất cả các layer đã được xử lý thành công!")
    print("=" * 70)


#  ĐỊNH NGHĨA DAG
with DAG(
    dag_id=DAG_IDS["master"],
    default_args=default_args,
    description="[Master] Điều phối toàn bộ pipeline: Bronze → Silver → Gold → Platinum",
    schedule_interval=DAG_SCHEDULES["master"],
    catchup=False,
    max_active_runs=1,
    tags=DAG_TAGS["master"],
) as dag:

    # Start 
    start = PythonOperator(
        task_id="pipeline_start",
        python_callable=_log_pipeline_start,
    )

    # Trigger Bronze Ingestion 
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id=DAG_IDS["bronze"],
        wait_for_completion=True,           
        poke_interval=30,                   
        allowed_states=["success"],          
        failed_states=["failed"],            
        reset_dag_run=True,                  
        retries=0,                           
    )

    # Trigger Silver Transformation 
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",
        trigger_dag_id=DAG_IDS["silver"],
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        retries=0,
    )

    # Trigger Gold Aggregation 
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_aggregation",
        trigger_dag_id=DAG_IDS["gold"],
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        retries=0,
    )

    # Trigger Platinum BI 
    trigger_platinum = TriggerDagRunOperator(
        task_id="trigger_platinum_bi",
        trigger_dag_id=DAG_IDS["platinum"],
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        retries=0,
    )

    # End 
    end = PythonOperator(
        task_id="pipeline_end",
        python_callable=_log_pipeline_end,
    )

    start >> trigger_bronze >> trigger_silver >> trigger_gold >> trigger_platinum >> end
