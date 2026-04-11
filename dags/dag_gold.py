from airflow import DAG, Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import dag_config  # Importing your team's generic config

# ==========================================
# 1. DEFINE SILVER DATASET DEPENDENCIES
# ==========================================
silver_base_path = "s3a://olist-lakehouse-2026/silver"
silver_tables = [
    "silver_olist_customers_dataset", "silver_olist_geolocation_dataset",
    "silver_olist_order_items_dataset", "silver_olist_order_payments_dataset",
    "silver_olist_order_reviews_dataset", "silver_olist_orders_dataset",
    "silver_olist_products_dataset", "silver_olist_sellers_dataset",
    "silver_product_category_name_translation"
]

# Map to Dataset objects
silver_dependencies = [Dataset(f"{silver_base_path}/{table}") for table in silver_tables]

# ==========================================
# 2. GET CONFIG FROM DAG_CONFIG
# ==========================================
# Use the helper function to get standardized args for TV3 (Gold)
default_args = dag_config.get_default_args(
    owner='thanhvien3', 
    start_date=datetime(2026, 4, 1)
)

# Path to your pyspark script from the config dictionary
PYSPARK_SCRIPT_PATH = dag_config.ETL_SCRIPTS.get(
    "aggregate_gold", 
    f"{dag_config.ETL_BASE_PATH}/processing/silver_to_gold.py"
)

# ==========================================
# 3. DEFINE THE GOLD DAG
# ==========================================
with DAG(
    dag_id=dag_config.DAG_IDS["gold"],
    default_args=default_args,
    schedule=silver_dependencies,
    catchup=False,
    tags=dag_config.DAG_TAGS["gold"] + ['olist']
) as dag:

    start_gold = EmptyOperator(task_id='start_gold')
    end_gold = EmptyOperator(task_id='end_gold')

    # Helper function to create tasks and reduce boilerplate
    def create_gold_task(table_name: str, task_alias: str):
        # We use the helper from dag_config to inject all Spark/Delta/S3 settings
        spark_kwargs = dag_config.create_spark_submit_kwargs(
            application=PYSPARK_SCRIPT_PATH,
            app_name=f"gold-{task_alias}",
            application_args=['--table', table_name]
        )
        
        return SparkSubmitOperator(
            task_id=f'transform_{table_name}',
            **spark_kwargs
        )

    # --- DIMENSION TASKS ---
    task_dim_products = create_gold_task('dim_products', 'products')
    task_dim_sellers = create_gold_task('dim_sellers', 'sellers')
    task_dim_customers = create_gold_task('dim_customers', 'customers')
    task_dim_date = create_gold_task('dim_date', 'date')

    # --- FACT TASKS ---
    task_fact_sales = create_gold_task('fact_sales', 'sales')
    task_fact_order_fulfillment = create_gold_task('fact_order_fulfillment', 'fulfillment')

    # ==========================================
    # 4. SET TASK DEPENDENCIES
    # ==========================================
    start_gold >> [
        task_dim_products, 
        task_dim_sellers, 
        task_dim_customers, 
        task_dim_date, 
        task_fact_sales, 
        task_fact_order_fulfillment
    ] >> end_gold