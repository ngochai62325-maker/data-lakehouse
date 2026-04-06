import sys
import os
import argparse

# --- PATH FIX ---
# Get the absolute path of the root directory (/opt/spark)
# Since this file is in /opt/spark/etl_pipeline/processing/, we go up two levels
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))

# Add it to sys.path so Python can find the 'etl_pipeline' module
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
# ----------------

# NOW we can safely do our custom imports
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace

from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table

# ... the rest of your functions below ...

def transform_orders(spark):
    print("Transforming Orders...")
    df = read_delta_table(spark, "bronze", "olist_orders_dataset")
    
    # Optional: I recommend adding the timestamp conversions here 
    # that we discussed earlier to make it truly "Silver" quality!
    
    write_delta_table(df, "silver", "silver_olist_orders_dataset", mode="overwrite")

def transform_order_items(spark):
    print("Transforming Order Items...")
    df = read_delta_table(spark, "bronze", "olist_order_items_dataset")
    write_delta_table(df, "silver", "silver_olist_order_items_dataset", mode="overwrite")

def transform_customers(spark):
    print("Transforming Customers...")
    df = read_delta_table(spark, "bronze", "olist_customers_dataset")
    write_delta_table(df, "silver", "silver_olist_customers_dataset", mode="overwrite")

def transform_geolocation(spark):
    print("Transforming Geolocation...")
    df = read_delta_table(spark, "bronze", "olist_geolocation_dataset")
    write_delta_table(df, "silver", "silver_olist_geolocation_dataset", mode="overwrite")

def transform_order_payments(spark):
    print("Transforming Order Payments...")
    df = read_delta_table(spark, "bronze", "olist_order_payments_dataset")
    
    payment_mapping = {
        "credit_card": "credit card",
        "debit_card": "debit card",
        "not_defined": "not defined"
    }
    df_cleaned = df.replace(payment_mapping, subset=["payment_type"])
    
    write_delta_table(df_cleaned, "silver", "silver_olist_order_payments_dataset", mode="overwrite")

def transform_order_reviews(spark):
    print("Transforming Order Reviews...")
    df = read_delta_table(spark, "bronze", "olist_order_reviews_dataset")
    
    # Convert data types
    df_cleaned = df.withColumn("review_creation_date", to_date(col("review_creation_date"))) \
                   .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp"))) \
                   .withColumn("review_score", col("review_score").cast("int"))
    
    # Remove outliers
    df_cleaned = df_cleaned.filter(col("review_score").isNull() | col("review_score").between(1, 5))
    
    write_delta_table(df_cleaned, "silver", "silver_olist_order_reviews_dataset", mode="overwrite")

def transform_products(spark):
    print("Transforming Products...")
    df = read_delta_table(spark, "bronze", "olist_products_dataset")
    
    df_cleaned = df.withColumn("product_category_name", regexp_replace(col("product_category_name"), "_", " "))
    
    write_delta_table(df_cleaned, "silver", "silver_olist_products_dataset", mode="overwrite")

def transform_sellers(spark):
    print("Transforming Sellers...")
    df = read_delta_table(spark, "bronze", "olist_sellers_dataset")
    write_delta_table(df, "silver", "silver_olist_sellers_dataset", mode="overwrite")

def transform_category_translation(spark):
    print("Transforming Category Translations...")
    df = read_delta_table(spark, "bronze", "product_category_name_translation")
    
    columns_to_clean = ["product_category_name", "product_category_name_english"]
    df_cleaned = df
    for c in columns_to_clean:
        df_cleaned = df_cleaned.withColumn(c, regexp_replace(col(c), "_", " "))
        
    write_delta_table(df_cleaned, "silver", "silver_product_category_name_translation", mode="overwrite")


if __name__ == "__main__":
    # Setup argument parser for Airflow integration
    parser = argparse.ArgumentParser(description="Run Silver Layer Transformations")
    parser.add_argument("--table", type=str, required=True, 
                        help="Name of the table to transform (e.g., 'orders', 'products', 'all')")
    args = parser.parse_args()

    # Initialize Spark
    spark = get_spark_session(app_name=f"SilverLayer-{args.table.capitalize()}")

    # Route to the correct function based on the argument
    if args.table == "orders":
        transform_orders(spark)
    elif args.table == "order_items":
        transform_order_items(spark)
    elif args.table == "customers":
        transform_customers(spark)
    elif args.table == "geolocation":
        transform_geolocation(spark)
    elif args.table == "order_payments":
        transform_order_payments(spark)
    elif args.table == "order_reviews":
        transform_order_reviews(spark)
    elif args.table == "products":
        transform_products(spark)
    elif args.table == "sellers":
        transform_sellers(spark)
    elif args.table == "translation":
        transform_category_translation(spark)
    elif args.table == "all":
        # Runs everything sequentially if you just want to trigger the whole layer at once
        transform_orders(spark)
        transform_order_items(spark)
        transform_customers(spark)
        transform_geolocation(spark)
        transform_order_payments(spark)
        transform_order_reviews(spark)
        transform_products(spark)
        transform_sellers(spark)
        transform_category_translation(spark)
    else:
        print(f"Unknown table parameter: {args.table}")

    spark.stop()