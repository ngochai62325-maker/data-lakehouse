import sys
import os
import argparse
from functools import wraps
from typing import Dict, Any, Callable

# --- PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
# ----------------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, date_format, year, month, quarter, dayofmonth,
    concat_ws, lit, row_number, first, sum as spark_sum, sequence,
    explode, min as spark_min, max as spark_max
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table


# =============================================================================
# ASSET DECORATOR
# =============================================================================

def asset(table_name: str):
    """
    Decorator to register a function as a Gold layer asset.
    Provides metadata logging: table_name, row_count, column_count, columns.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(spark: SparkSession, *args, **kwargs) -> DataFrame:
            print(f"\n{'='*60}")
            print(f"Processing Asset: {table_name}")
            print(f"{'='*60}")
            
            # Execute transformation
            df = func(spark, *args, **kwargs)
            
            # Write to Gold layer
            write_delta_table(df, "gold", table_name, mode="overwrite")
            
            # Log metadata
            metadata = get_metadata(df, table_name)
            log_metadata(metadata)
            
            return df
        
        wrapper.table_name = table_name
        wrapper.is_asset = True
        return wrapper
    return decorator


def get_metadata(df: DataFrame, table_name: str) -> Dict[str, Any]:
    """Extract metadata from DataFrame."""
    return {
        "table_name": table_name,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns
    }


def log_metadata(metadata: Dict[str, Any]) -> None:
    """Log metadata to console."""
    print(f"\n--- Metadata for {metadata['table_name']} ---")
    print(f"  Table Name   : {metadata['table_name']}")
    print(f"  Row Count    : {metadata['row_count']:,}")
    print(f"  Column Count : {metadata['column_count']}")
    print(f"  Columns      : {', '.join(metadata['columns'])}")
    print("-" * 40)


# =============================================================================
# DIMENSION TABLES
# =============================================================================

@asset("dim_customer")
def create_dim_customer(spark: SparkSession) -> DataFrame:
    """
    Create dim_customer dimension table.
    Source: customer + geolocation (joined by zip_code_prefix)
    PK: customer_id
    """
    # Read Silver tables
    customer_df = read_delta_table(spark, "silver", "silver_olist_customers_dataset")
    geolocation_df = read_delta_table(spark, "silver", "silver_olist_geolocation_dataset")
    
    # Deduplicate geolocation by zip_code_prefix (take first occurrence)
    geo_window = Window.partitionBy("geolocation_zip_code_prefix").orderBy("geolocation_lat")
    geo_dedup = (
        geolocation_df
        .withColumn("rn", row_number().over(geo_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            col("geolocation_lat").alias("customer_lat"),
            col("geolocation_lng").alias("customer_lng")
        )
    )
    
    # Join customer with geolocation
    dim_customer = (
        customer_df
        .join(
            geo_dedup,
            customer_df["customer_zip_code_prefix"] == geo_dedup["zip_code_prefix"],
            "left"
        )
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_city"),
            col("customer_state"),
            col("customer_lat"),
            col("customer_lng")
        )
        .dropDuplicates(["customer_id"])
    )
    
    return dim_customer


@asset("dim_product")
def create_dim_product(spark: SparkSession) -> DataFrame:
    """
    Create dim_product dimension table.
    Source: product + product_category_translation (joined by product_category_name)
    PK: product_id
    """
    # Read Silver tables
    product_df = read_delta_table(spark, "silver", "silver_olist_products_dataset")
    category_df = read_delta_table(spark, "silver", "silver_product_category_name_translation")
    
    # Join product with category translation
    dim_product = (
        product_df
        .join(
            category_df,
            product_df["product_category_name"] == category_df["product_category_name"],
            "left"
        )
        .select(
            col("product_id"),
            product_df["product_category_name"],
            col("product_category_name_english"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm")
        )
        .dropDuplicates(["product_id"])
    )
    
    return dim_product


@asset("dim_seller")
def create_dim_seller(spark: SparkSession) -> DataFrame:
    """
    Create dim_seller dimension table.
    Source: seller + geolocation (joined by zip_code_prefix)
    PK: seller_id
    """
    # Read Silver tables
    seller_df = read_delta_table(spark, "silver", "silver_olist_sellers_dataset")
    geolocation_df = read_delta_table(spark, "silver", "silver_olist_geolocation_dataset")
    
    # Deduplicate geolocation by zip_code_prefix
    geo_window = Window.partitionBy("geolocation_zip_code_prefix").orderBy("geolocation_lat")
    geo_dedup = (
        geolocation_df
        .withColumn("rn", row_number().over(geo_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            col("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            col("geolocation_city").alias("seller_city"),
            col("geolocation_state").alias("seller_state")
        )
    )
    
    # Join seller with geolocation
    dim_seller = (
        seller_df
        .join(
            geo_dedup,
            seller_df["seller_zip_code_prefix"] == geo_dedup["zip_code_prefix"],
            "left"
        )
        .select(
            seller_df["seller_id"],
            geo_dedup["seller_city"],
            geo_dedup["seller_state"]
        )
        .dropDuplicates(["seller_id"])
    )
    
    return dim_seller


@asset("dim_order")
def create_dim_order(spark: SparkSession) -> DataFrame:
    """
    Create dim_order dimension table.
    Source: order + payment (joined by order_id)
    PK: order_id
    """
    # Read Silver tables
    order_df = read_delta_table(spark, "silver", "silver_olist_orders_dataset")
    payment_df = read_delta_table(spark, "silver", "silver_olist_order_payments_dataset")
    
    # Aggregate payment_type per order (take most common or first)
    payment_agg = (
        payment_df
        .groupBy("order_id")
        .agg(first("payment_type").alias("payment_type"))
    )
    
    # Join order with payment
    dim_order = (
        order_df
        .join(payment_agg, "order_id", "left")
        .select(
            col("order_id"),
            col("order_status"),
            col("payment_type"),
            col("order_purchase_timestamp"),
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
        .dropDuplicates(["order_id"])
    )
    
    return dim_order


@asset("dim_date")
def create_dim_date(spark: SparkSession) -> DataFrame:
    """
    Create dim_date dimension table.
    Source: order_purchase_timestamp from orders
    Generates date range from min to max purchase date.
    PK: date_key (YYYYMMDD format)
    """
    # Read Silver orders to get date range
    order_df = read_delta_table(spark, "silver", "silver_olist_orders_dataset")
    
    # Get min and max dates
    date_range = (
        order_df
        .select(
            spark_min(to_date(col("order_purchase_timestamp"))).alias("min_date"),
            spark_max(to_date(col("order_purchase_timestamp"))).alias("max_date")
        )
        .collect()[0]
    )
    
    min_date = date_range["min_date"]
    max_date = date_range["max_date"]
    
    # Generate date sequence
    date_seq_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{min_date}'), 
            to_date('{max_date}'), 
            interval 1 day
        )) as full_date
    """)
    
    # Create dimension columns
    dim_date = (
        date_seq_df
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", year(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("quarter", quarter(col("full_date")))
        .withColumn("day", dayofmonth(col("full_date")))
        .select(
            col("date_key"),
            col("full_date"),
            col("year"),
            col("month"),
            col("quarter"),
            col("day")
        )
        .dropDuplicates(["date_key"])
        .orderBy("date_key")
    )
    
    return dim_date


# =============================================================================
# FACT TABLES
# =============================================================================

@asset("fact_reviews")
def create_fact_reviews(spark: SparkSession) -> DataFrame:
    """
    Create fact_reviews fact table.
    Grain: 1 row = 1 review (order_id + review_id)
    
    Source: order_reviews
    Columns: order_id, review_id, review_score, review_creation_date, review_answer_timestamp
    """
    # Read Silver reviews table
    reviews_df = read_delta_table(spark, "silver", "silver_olist_order_reviews_dataset")
    
    # Read Gold dim_order for FK validation
    dim_order = read_delta_table(spark, "gold", "dim_order")
    
    # Build fact table
    fact_reviews = (
        reviews_df
        # Validate FK: order_id exists in dim_order
        .join(
            dim_order.select("order_id").distinct(),
            "order_id",
            "inner"
        )
        # Select final columns
        .select(
            col("order_id"),
            col("review_id"),
            col("review_score"),
            col("review_creation_date"),
            col("review_answer_timestamp")
        )
        .dropDuplicates(["order_id", "review_id"])
    )
    
    return fact_reviews


@asset("fact_sales")
def create_fact_sales(spark: SparkSession) -> DataFrame:
    """
    Create fact_sales fact table.
    Grain: 1 row = 1 order_item
    
    Joins:
    - order + order_item
    - dim_customer (FK: customer_id)
    - dim_product (FK: product_id)
    - dim_seller (FK: seller_id)
    - dim_order (FK: order_id)
    - dim_date (FK: date_key based on order_purchase_timestamp)
    """
    # Read Silver tables (source data)
    order_df = read_delta_table(spark, "silver", "silver_olist_orders_dataset")
    order_item_df = read_delta_table(spark, "silver", "silver_olist_order_items_dataset")
    payment_df = read_delta_table(spark, "silver", "silver_olist_order_payments_dataset")
    
    # Read Gold dimension tables for FK validation
    dim_customer = read_delta_table(spark, "gold", "dim_customer")
    dim_product = read_delta_table(spark, "gold", "dim_product")
    dim_seller = read_delta_table(spark, "gold", "dim_seller")
    dim_order = read_delta_table(spark, "gold", "dim_order")
    dim_date = read_delta_table(spark, "gold", "dim_date")
    
    # Aggregate payment_value per order
    payment_agg = (
        payment_df
        .groupBy("order_id")
        .agg(spark_sum("payment_value").alias("payment_value"))
    )
    
    # Prepare order with date_key
    order_with_date = (
        order_df
        .withColumn(
            "date_key", 
            date_format(to_date(col("order_purchase_timestamp")), "yyyyMMdd").cast(IntegerType())
        )
        .select(
            col("order_id"),
            col("customer_id"),
            col("date_key")
        )
    )
    
    # Build fact table starting from order_item (grain)
    fact_sales = (
        order_item_df
        # Join with order to get customer_id and date_key
        .join(order_with_date, "order_id", "inner")
        # Join with payment aggregation
        .join(payment_agg, "order_id", "left")
        # Validate FK: customer_id exists in dim_customer
        .join(
            dim_customer.select("customer_id").distinct(),
            "customer_id",
            "inner"
        )
        # Validate FK: product_id exists in dim_product
        .join(
            dim_product.select("product_id").distinct(),
            "product_id",
            "inner"
        )
        # Validate FK: seller_id exists in dim_seller
        .join(
            dim_seller.select("seller_id").distinct(),
            "seller_id",
            "inner"
        )
        # Validate FK: order_id exists in dim_order
        .join(
            dim_order.select("order_id").distinct(),
            "order_id",
            "inner"
        )
        # Validate FK: date_key exists in dim_date
        .join(
            dim_date.select("date_key").distinct(),
            "date_key",
            "inner"
        )
        # Select final columns
        .select(
            col("order_id"),
            col("order_item_id"),
            col("customer_id"),
            col("product_id"),
            col("seller_id"),
            col("date_key"),
            col("price"),
            col("freight_value"),
            col("payment_value")
        )
    )
    
    return fact_sales


# =============================================================================
# ORCHESTRATION
# =============================================================================

def run_all_dimensions(spark: SparkSession) -> None:
    """Run all dimension table transformations in correct order."""
    print("\n" + "="*60)
    print("STARTING DIMENSION TABLES CREATION")
    print("="*60)
    
    create_dim_customer(spark)
    create_dim_product(spark)
    create_dim_seller(spark)
    create_dim_order(spark)
    create_dim_date(spark)
    
    print("\n✓ All dimension tables created successfully!")


def run_fact_table(spark: SparkSession) -> None:
    """Run fact table transformations (requires dimensions to exist)."""
    print("\n" + "="*60)
    print("STARTING FACT TABLES CREATION")
    print("="*60)
    
    create_fact_reviews(spark)
    create_fact_sales(spark)
    
    print("\n✓ All fact tables created successfully!")


def run_all(spark: SparkSession) -> None:
    """Run complete Gold layer transformation."""
    print("\n" + "="*60)
    print("STARTING GOLD LAYER - STAR SCHEMA TRANSFORMATION")
    print("="*60)
    
    # Create dimensions first
    run_all_dimensions(spark)
    
    # Then create fact table
    run_fact_table(spark)
    
    print("\n" + "="*60)
    print("GOLD LAYER TRANSFORMATION COMPLETED!")
    print("="*60)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Gold Layer Transformations (Star Schema)")
    parser.add_argument(
        "--table", 
        type=str, 
        required=True,
        choices=[
            "dim_customer", "dim_product", "dim_seller", 
            "dim_order", "dim_date", "fact_reviews", "fact_sales",
            "dimensions", "fact", "all"
        ],
        help="Name of the table to transform or 'all' for complete transformation"
    )
    args = parser.parse_args()

    # Initialize Spark
    spark = get_spark_session(app_name=f"GoldLayer-{args.table}")

    try:
        # Route to the correct transformation
        if args.table == "dim_customer":
            create_dim_customer(spark)
        elif args.table == "dim_product":
            create_dim_product(spark)
        elif args.table == "dim_seller":
            create_dim_seller(spark)
        elif args.table == "dim_order":
            create_dim_order(spark)
        elif args.table == "dim_date":
            create_dim_date(spark)
        elif args.table == "fact_reviews":
            create_fact_reviews(spark)
        elif args.table == "fact_sales":
            create_fact_sales(spark)
        elif args.table == "dimensions":
            run_all_dimensions(spark)
        elif args.table == "fact":
            run_fact_table(spark)
        elif args.table == "all":
            run_all(spark)
        else:
            print(f"Unknown table parameter: {args.table}")
    finally:
        spark.stop()
