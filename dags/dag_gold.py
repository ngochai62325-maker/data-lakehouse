from airflow import DAG, Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ==========================================
# 1. DEFINE SILVER DATASET DEPENDENCIES
# ==========================================
# These are the "contracts" you are waiting for TV2 (Silver) to update
silver_customers = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_customers_dataset")
silver_geolocation = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_geolocation_dataset")
silver_order_items = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_order_items_dataset")
silver_order_payments = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_order_payments_dataset")
silver_order_reviews = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_order_reviews_dataset")
silver_orders = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_orders_dataset")
silver_products = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_products_dataset")
silver_sellers = Dataset("s3a://olist-lakehouse-2026/silver/silver_olist_sellers_dataset")
silver_translation = Dataset("s3a://olist-lakehouse-2026/silver/silver_product_category_name_translation")

# Airflow will wait for ALL of these to be updated before running the Gold DAG
silver_dependencies = [
    silver_customers, silver_geolocation, silver_order_items, 
    silver_order_payments, silver_order_reviews, silver_orders, 
    silver_products, silver_sellers, silver_translation
]

default_args = {
    'owner': 'tv3_data_engineer',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
}

# ==========================================
# ADD DELTA LAKE CONFIGURATIONS HERE
# ==========================================
# Adjusted to Delta 3.3.2 to match your team's environment requirements!
# We are adding the Hadoop-AWS and AWS-Java-SDK bundles (standard for Spark 3.5)
DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}

# ==========================================
# 2. DEFINE THE GOLD DAG
# ==========================================
with DAG(
    dag_id='dag_gold_layer_modeling',
    default_args=default_args,
    schedule=silver_dependencies,  # Triggered automatically when Silver finishes!
    catchup=False,
    tags=['lakehouse', 'gold', 'olist']
) as dag:

    # Path to your pyspark script inside your Airflow/Spark container
    PYSPARK_SCRIPT_PATH = '/opt/airflow/etl_pipeline/processing/silver_to_gold.py' 

    # --- VISUAL STRUCTURE (START & END) ---
    start_gold = EmptyOperator(task_id='start_gold')
    end_gold = EmptyOperator(task_id='end_gold')

    # --- DIMENSION TASKS ---
    task_dim_products = SparkSubmitOperator(
        task_id='transform_dim_products',
        application=PYSPARK_SCRIPT_PATH,
        application_args=['--table', 'dim_products'], 
        conn_id='spark_default',
        name='gold-dim-products',
        packages=DELTA_PACKAGES, # Added Delta Packages
        conf=DELTA_CONF          # Added Delta Configurations
    )

    task_dim_sellers = SparkSubmitOperator(
        task_id='transform_dim_sellers',
        application=PYSPARK_SCRIPT_PATH,
        application_args=['--table', 'dim_sellers'],
        conn_id='spark_default',
        name='gold-dim-sellers',
        packages=DELTA_PACKAGES,
        conf=DELTA_CONF
    )

    task_dim_customers = SparkSubmitOperator(
        task_id='transform_dim_customers',
        application=PYSPARK_SCRIPT_PATH,
        application_args=['--table', 'dim_customers'],
        conn_id='spark_default',
        name='gold-dim-customers',
        packages=DELTA_PACKAGES,
        conf=DELTA_CONF
    )

    task_dim_date = SparkSubmitOperator(
        task_id='transform_dim_date',
        application=PYSPARK_SCRIPT_PATH,
        application_args=['--table', 'dim_date'],
        conn_id='spark_default',
        name='gold-dim-date',
        packages=DELTA_PACKAGES,
        conf=DELTA_CONF
    )

    # --- FACT TASKS ---
    task_fact_sales = SparkSubmitOperator(
        task_id='transform_fact_sales',
        application=PYSPARK_SCRIPT_PATH,
        application_args=['--table', 'fact_sales'],
        conn_id='spark_default',
        name='gold-fact-sales',
        packages=DELTA_PACKAGES,
        conf=DELTA_CONF
    )

    task_fact_order_fulfillment = SparkSubmitOperator(
        task_id='transform_fact_order_fulfillment',
        application=PYSPARK_SCRIPT_PATH,
        application_args=['--table', 'fact_order_fulfillment'],
        conn_id='spark_default',
        name='gold-fact-fulfillment',
        packages=DELTA_PACKAGES,
        conf=DELTA_CONF
    )

    # ==========================================
    # 3. SET TASK DEPENDENCIES (FAN-OUT / FAN-IN)
    # ==========================================
    # The DAG starts, triggers all 6 transformations in parallel, then ends.
    
    start_gold >> [
        task_dim_products, 
        task_dim_sellers, 
        task_dim_customers, 
        task_dim_date, 
        task_fact_sales, 
        task_fact_order_fulfillment
    ] >> end_gold