import sys
import os
import argparse
from pyspark.sql import functions as F

# Fix the path so Python can find your custom modules
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table

# ==========================================
# DIMENSION TABLES
# ==========================================

def transform_dim_products(spark):
    print("Transforming dim_products...")
    df_silver_products = read_delta_table(spark, "silver", "silver_olist_products_dataset")
    df_silver_translation = read_delta_table(spark, "silver", "silver_product_category_name_translation")
    
    df_dim_products = df_silver_products.join(df_silver_translation, on="product_category_name", how="left")
    df_final = df_dim_products.select(
        F.col("product_id"),
        F.col("product_category_name").alias("product_category_pt"),
        F.col("product_category_name_english").alias("product_category_en"),
        F.col("product_weight_g"),
        F.col("product_length_cm"),
        F.col("product_height_cm"),
        F.col("product_width_cm")
    )
    # OPTIMIZATION: coalesce(1) to avoid small files in S3
    write_delta_table(df_final.coalesce(1), "gold", "dim_products", mode="overwrite")

def transform_dim_sellers(spark):
    print("Transforming dim_sellers...")
    df_silver_sellers = read_delta_table(spark, "silver", "silver_olist_sellers_dataset")
    df_silver_geo = read_delta_table(spark, "silver", "silver_olist_geolocation_dataset")
    
    df_geo_centroid = df_silver_geo.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng")
    )
    
    df_dim_sellers = df_silver_sellers.join(
        df_geo_centroid,
        df_silver_sellers["seller_zip_code_prefix"] == df_geo_centroid["geolocation_zip_code_prefix"],
        how="left"
    )
    df_final = df_dim_sellers.select(
        F.col("seller_id"), F.col("seller_city"), F.col("seller_state"), 
        F.col("seller_zip_code_prefix"), F.col("geolocation_lat").alias("seller_lat"), 
        F.col("geolocation_lng").alias("seller_lng")
    )
    # OPTIMIZATION: coalesce(1) to avoid small files in S3
    write_delta_table(df_final.coalesce(1), "gold", "dim_sellers", mode="overwrite")

def transform_dim_customers(spark):
    print("Transforming dim_customers...")
    df_silver_customers = read_delta_table(spark, "silver", "silver_olist_customers_dataset")
    df_silver_geo = read_delta_table(spark, "silver", "silver_olist_geolocation_dataset")
    
    df_geo_centroid = df_silver_geo.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng")
    )
    
    df_dim_customers = df_silver_customers.join(
        df_geo_centroid,
        df_silver_customers["customer_zip_code_prefix"] == df_geo_centroid["geolocation_zip_code_prefix"],
        how="left"
    )
    df_final = df_dim_customers.select(
        F.col("customer_id"), F.col("customer_unique_id"), F.col("customer_city"), 
        F.col("customer_state"), F.col("customer_zip_code_prefix"), 
        F.col("geolocation_lat").alias("customer_lat"), F.col("geolocation_lng").alias("customer_lng")
    )
    # OPTIMIZATION: coalesce(1) to avoid small files in S3
    write_delta_table(df_final.coalesce(1), "gold", "dim_customers", mode="overwrite")

def transform_dim_date(spark):
    print("Generating dim_date...")
    start_date = "2016-01-01"
    end_date = "2018-12-31"
    
    df_dim_date = spark.sql(f"""
        SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_array
    """).withColumn("full_date", F.explode("date_array")).drop("date_array")
    
    df_final = df_dim_date.select(
        F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date"),
        F.dayofmonth(F.col("full_date")).alias("day_of_month"),
        F.dayofweek(F.col("full_date")).alias("day_of_week"),
        F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
        F.month(F.col("full_date")).alias("month"),
        F.quarter(F.col("full_date")).alias("quarter"),
        F.year(F.col("full_date")).alias("year")
    ).withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
    
    # OPTIMIZATION: coalesce(1) to avoid small files in S3
    write_delta_table(df_final.coalesce(1), "gold", "dim_date", mode="overwrite")

# ==========================================
# FACT TABLES
# ==========================================

def transform_fact_sales(spark):
    print("Transforming fact_sales...")
    df_silver_orders = read_delta_table(spark, "silver", "silver_olist_orders_dataset")
    df_silver_items = read_delta_table(spark, "silver", "silver_olist_order_items_dataset")
    df_silver_payments = read_delta_table(spark, "silver", "silver_olist_order_payments_dataset")

    df_sales_base = df_silver_items.join(df_silver_orders, on="order_id", how="inner")
    df_payments_agg = df_silver_payments.groupBy("order_id").agg(F.sum("payment_value").alias("total_payment_value"))
    df_sales_joined = df_sales_base.join(df_payments_agg, on="order_id", how="left")
    
    df_final = df_sales_joined.select(
        F.col("order_id"), F.col("order_item_id"), F.col("product_id"), F.col("seller_id"), F.col("customer_id"), F.col("order_status"),
        F.date_format(F.col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("purchase_date_key"),
        F.round(F.col("price"), 2).alias("price"),
        F.round(F.col("freight_value"), 2).alias("freight_value"),
        F.round(F.col("price") + F.col("freight_value"), 2).alias("total_item_value"),
        F.round(F.col("total_payment_value"), 2).alias("total_payment_value")
    )
    # OPTIMIZATION: coalesce(1) to avoid small files in S3
    write_delta_table(df_final.coalesce(1), "gold", "fact_sales", mode="overwrite")

def transform_fact_order_fulfillment(spark):
    print("Transforming fact_order_fulfillment...")
    df_silver_orders = read_delta_table(spark, "silver", "silver_olist_orders_dataset")
    df_silver_reviews = read_delta_table(spark, "silver", "silver_olist_order_reviews_dataset")

    df_fulfillment_base = df_silver_orders.join(df_silver_reviews, on="order_id", how="left")
    
    df_final = df_fulfillment_base.select(
        F.col("order_id"), F.col("customer_id"), F.col("review_id"), F.col("order_status"), F.col("review_score"),
        F.date_format(F.col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("purchase_date_key"),
        F.date_format(F.col("order_estimated_delivery_date"), "yyyyMMdd").cast("int").alias("estimated_delivery_date_key"),
        F.date_format(F.col("order_delivered_customer_date"), "yyyyMMdd").cast("int").alias("actual_delivery_date_key"),
        F.datediff(F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date")).alias("delivery_delay_days"),
        F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")).alias("shipping_duration_days")
    ).withColumn("is_late_delivery", F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0))
    
    # OPTIMIZATION: coalesce(1) to avoid small files in S3
    write_delta_table(df_final.coalesce(1), "gold", "fact_order_fulfillment", mode="overwrite")

# ==========================================
# MAIN EXECUTION PIPELINE
# ==========================================

if __name__ == "__main__":
    # Setup argument parser for Airflow/Docker integration
    parser = argparse.ArgumentParser(description="Run Gold Layer Transformations")
    parser.add_argument("--table", type=str, required=True, 
                        help="Name of the table to transform (e.g., 'dim_products', 'fact_sales', 'all')")
    args = parser.parse_args()

    # Initialize Spark
    spark = get_spark_session(app_name=f"GoldLayer-{args.table.capitalize()}")

    # Route to the correct function based on the argument
    if args.table == "dim_products":
        transform_dim_products(spark)
    elif args.table == "dim_sellers":
        transform_dim_sellers(spark)
    elif args.table == "dim_customers":
        transform_dim_customers(spark)
    elif args.table == "dim_date":
        transform_dim_date(spark)
    elif args.table == "fact_sales":
        transform_fact_sales(spark)
    elif args.table == "fact_order_fulfillment":
        transform_fact_order_fulfillment(spark)
    elif args.table == "all":
        print("========== Running complete Gold layer pipeline sequentially...")
        transform_dim_products(spark)
        transform_dim_sellers(spark)
        transform_dim_customers(spark)
        transform_dim_date(spark)
        transform_fact_sales(spark)
        transform_fact_order_fulfillment(spark)
        print("========== Gold layer pipeline complete!")
    else:
        print(f"Unknown table parameter: {args.table}")

    spark.stop()