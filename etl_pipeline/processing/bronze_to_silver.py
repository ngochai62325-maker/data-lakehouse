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
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace, lpad, lower, trim

from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table

def transform_orders(spark):
    print("Transforming Orders...")
    df = read_delta_table(spark, "bronze", "olist_orders_dataset")
    
    # Ép kiểu 5 cột sang Timestamp (không drop Null -> các đơn đang giao sẽ vẫn giữ Null)
    timestamp_cols = [
        "order_purchase_timestamp", "order_approved_at", 
        "order_delivered_carrier_date", "order_delivered_customer_date", 
        "order_estimated_delivery_date"
    ]
    df_cleaned = df
    for c in timestamp_cols:
        df_cleaned = df_cleaned.withColumn(c, to_timestamp(col(c)))
        
    write_delta_table(df_cleaned, "silver", "silver_olist_orders_dataset", mode="overwrite")

def transform_order_items(spark):
    print("Transforming Order Items...")
    df = read_delta_table(spark, "bronze", "olist_order_items_dataset")
    
    # Ép kiểu Timestamp và Float/Double
    df_cleaned = df.withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))) \
                   .withColumn("price", col("price").cast("double")) \
                   .withColumn("freight_value", col("freight_value").cast("double"))
                   
    write_delta_table(df_cleaned, "silver", "silver_olist_order_items_dataset", mode="overwrite")

def transform_customers(spark):
    print("Transforming Customers...")
    df = read_delta_table(spark, "bronze", "olist_customers_dataset")
    
    # Pading Zip code, Trim & Lower city/state
    df_cleaned = df.withColumn("customer_zip_code_prefix", lpad(col("customer_zip_code_prefix").cast("string"), 5, "0")) \
                   .withColumn("customer_city", lower(trim(col("customer_city")))) \
                   .withColumn("customer_state", lower(trim(col("customer_state"))))
                   
    write_delta_table(df_cleaned, "silver", "silver_olist_customers_dataset", mode="overwrite")

def transform_geolocation(spark):
    print("Transforming Geolocation...")
    df = read_delta_table(spark, "bronze", "olist_geolocation_dataset")
    
    # Drop exact duplicates and pad zip codes
    df_cleaned = df.dropDuplicates(["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])
    # Pad zip codes to 5 characters with leading zeros
    df_cleaned = df_cleaned.withColumn(
        "geolocation_zip_code_prefix", 
        lpad(col("geolocation_zip_code_prefix").cast("string"), 5, "0")
    )
    df_cleaned = (
        df_cleaned
        .withColumn("geolocation_city",  trim(lower(col("geolocation_city"))))
        .withColumn("geolocation_state", trim(lower(col("geolocation_state"))))
    )
    
    write_delta_table(df_cleaned, "silver", "silver_olist_geolocation_dataset", mode="overwrite")

def transform_order_payments(spark):
    print("Transforming Order Payments...")
    df = read_delta_table(spark, "bronze", "olist_order_payments_dataset")

    payment_mapping = {
        "credit_card": "credit card",
        "debit_card": "debit card",
        "not_defined": "not defined"
    }
    df_cleaned = df.replace(payment_mapping, subset=["payment_type"])

    df_orders = read_delta_table(spark, "silver", "silver_olist_orders_dataset").select("order_id")
    df_cleaned = df_cleaned.join(df_orders, on="order_id", how="left_semi")

    write_delta_table(df_cleaned, "silver", "silver_olist_order_payments_dataset", mode="overwrite")

def transform_order_reviews(spark):
    print("Transforming Order Reviews...")
    df = read_delta_table(spark, "bronze", "olist_order_reviews_dataset")

    df_cleaned = (
        df
        .dropna(subset=["review_id", "order_id"])
        .dropDuplicates(["review_id"])
        # ✅ Cast types trước
        .withColumn("review_creation_date",    to_timestamp(col("review_creation_date")))
        .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))
        .withColumn("review_score",            col("review_score").cast("int"))
        # Filter outlier
        .filter(col("review_score").isNull() | col("review_score").between(1, 5))
        # ✅ fillna sau cùng
        .fillna({"review_comment_title": "No Title", "review_comment_message": "No Message"})
    )

    # ✅ Chỉ select "order_id" để tối ưu shuffle
    df_orders = read_delta_table(spark, "silver", "silver_olist_orders_dataset").select("order_id")
    df_cleaned = df_cleaned.join(df_orders, on="order_id", how="left_semi")

    write_delta_table(df_cleaned, "silver", "silver_olist_order_reviews_dataset", mode="overwrite")


def transform_products(spark):
    print("Transforming Products...")
    df = read_delta_table(spark, "bronze", "olist_products_dataset")

    df_cleaned = (
        df
        .fillna({"product_category_name": "unknown"})
        .withColumn("product_category_name", regexp_replace(col("product_category_name"), "_", " "))
    )

    # ✅ Đọc từ Silver — đã clean sẵn, không cần regexp_replace lại
    df_translation = read_delta_table(spark, "silver", "silver_product_category_name_translation")

    df_cleaned = df_cleaned.join(df_translation, on="product_category_name", how="left")
    df_cleaned = df_cleaned.fillna({"product_category_name_english": "unknown"})

    write_delta_table(df_cleaned, "silver", "silver_olist_products_dataset", mode="overwrite")
    
def transform_sellers(spark):
    print("Transforming Sellers...")
    df = read_delta_table(spark, "bronze", "olist_sellers_dataset")
    
    # Cast to string and pad with leading zeros up to 5 characters
    df_cleaned = df.withColumn("seller_zip_code_prefix", lpad(col("seller_zip_code_prefix").cast("string"), 5, "0")) \
                   .withColumn("seller_city", lower(trim(col("seller_city")))) \
                   .withColumn("seller_state", lower(trim(col("seller_state"))))
                   
    write_delta_table(df_cleaned, "silver", "silver_olist_sellers_dataset", mode="overwrite")

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
        # NOTE: Orders must run first because Reviews and Payments use it for validation
        transform_orders(spark)
        transform_order_items(spark)
        transform_customers(spark)
        transform_geolocation(spark)
        transform_order_payments(spark)
        transform_order_reviews(spark)
        transform_category_translation(spark)
        transform_products(spark)
        transform_sellers(spark)
    else:
        print(f"Unknown table parameter: {args.table}")

    spark.stop()