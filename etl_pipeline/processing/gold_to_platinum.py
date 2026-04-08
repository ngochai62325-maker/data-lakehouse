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
    col, countDistinct, sum as spark_sum, avg as spark_avg, count
)

from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table


# =============================================================================
# ASSET DECORATOR
# =============================================================================

def asset(table_name: str):
    """
    Decorator to register a function as a Platinum layer asset.
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
            
            # Write to Platinum layer
            write_delta_table(df, "platinum", table_name, mode="overwrite")
            
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
# DATA MART: MART_SALES (Main Denormalized Table)
# =============================================================================

@asset("mart_sales")
def create_mart_sales(spark: SparkSession) -> DataFrame:
    """
    Create mart_sales - main denormalized table for BI.
    Joins all dimension tables with fact_sales for easy querying.
    
    Output columns:
    - order_id, customer_id, customer_city, customer_state
    - product_id, product_category_name_english
    - seller_id, seller_state
    - date_key, year, month
    - order_status, payment_type
    - price, freight_value, payment_value
    """
    # Read Gold layer tables
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    dim_customer = read_delta_table(spark, "gold", "dim_customer")
    dim_product = read_delta_table(spark, "gold", "dim_product")
    dim_seller = read_delta_table(spark, "gold", "dim_seller")
    dim_order = read_delta_table(spark, "gold", "dim_order")
    dim_date = read_delta_table(spark, "gold", "dim_date")
    
    # Build mart_sales with all necessary joins
    mart_sales = (
        fact_sales
        # Join with dim_customer
        .join(
            dim_customer.select(
                col("customer_id"),
                col("customer_city"),
                col("customer_state")
            ),
            "customer_id",
            "inner"
        )
        # Join with dim_product
        .join(
            dim_product.select(
                col("product_id"),
                col("product_category_name_english")
            ),
            "product_id",
            "inner"
        )
        # Join with dim_seller
        .join(
            dim_seller.select(
                col("seller_id"),
                col("seller_state")
            ),
            "seller_id",
            "inner"
        )
        # Join with dim_order
        .join(
            dim_order.select(
                col("order_id"),
                col("order_status"),
                col("payment_type")
            ),
            "order_id",
            "inner"
        )
        # Join with dim_date
        .join(
            dim_date.select(
                col("date_key"),
                col("year"),
                col("month")
            ),
            "date_key",
            "inner"
        )
        # Select final columns in logical order
        .select(
            col("order_id"),
            col("customer_id"),
            col("customer_city"),
            col("customer_state"),
            col("product_id"),
            col("product_category_name_english"),
            col("seller_id"),
            col("seller_state"),
            col("date_key"),
            col("year"),
            col("month"),
            col("order_status"),
            col("payment_type"),
            col("price"),
            col("freight_value"),
            col("payment_value")
        )
    )
    
    return mart_sales


# =============================================================================
# KPI TABLES
# =============================================================================

@asset("mart_kpi_daily")
def create_mart_kpi_daily(spark: SparkSession) -> DataFrame:
    """
    Create mart_kpi_daily - daily KPI aggregation for dashboard.
    
    Group by: date_key, year, month
    Metrics:
    - total_orders: COUNT(DISTINCT order_id)
    - total_revenue: SUM(payment_value)
    - total_items: COUNT(order_item_id)
    - avg_order_value: total_revenue / total_orders
    """
    # Read Gold layer fact_sales
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    dim_date = read_delta_table(spark, "gold", "dim_date")
    
    # Aggregate KPIs by date
    mart_kpi_daily = (
        fact_sales
        .join(
            dim_date.select("date_key", "year", "month"),
            "date_key",
            "inner"
        )
        .groupBy("date_key", "year", "month")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("payment_value").alias("total_revenue"),
            count("order_item_id").alias("total_items")
        )
        .withColumn(
            "avg_order_value",
            col("total_revenue") / col("total_orders")
        )
        .select(
            col("date_key"),
            col("year"),
            col("month"),
            col("total_orders"),
            col("total_revenue"),
            col("total_items"),
            col("avg_order_value")
        )
        .orderBy("date_key")
    )
    
    return mart_kpi_daily


@asset("mart_top_products")
def create_mart_top_products(spark: SparkSession) -> DataFrame:
    """
    Create mart_top_products - product performance analysis.
    
    Group by: product_id, product_category_name_english
    Metrics:
    - total_sales: SUM(payment_value)
    - total_items: COUNT(order_item_id)
    """
    # Read Gold layer tables
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    dim_product = read_delta_table(spark, "gold", "dim_product")
    
    # Aggregate by product
    mart_top_products = (
        fact_sales
        .join(
            dim_product.select("product_id", "product_category_name_english"),
            "product_id",
            "inner"
        )
        .groupBy("product_id", "product_category_name_english")
        .agg(
            spark_sum("payment_value").alias("total_sales"),
            count("order_item_id").alias("total_items")
        )
        .select(
            col("product_id"),
            col("product_category_name_english"),
            col("total_sales"),
            col("total_items")
        )
        .orderBy(col("total_sales").desc())
    )
    
    return mart_top_products


@asset("mart_customer_analysis")
def create_mart_customer_analysis(spark: SparkSession) -> DataFrame:
    """
    Create mart_customer_analysis - customer behavior analysis.
    
    Group by: customer_id, customer_state
    Metrics:
    - total_orders: COUNT(DISTINCT order_id)
    - total_spent: SUM(payment_value)
    - avg_order_value: total_spent / total_orders
    """
    # Read Gold layer tables
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    dim_customer = read_delta_table(spark, "gold", "dim_customer")
    
    # Aggregate by customer
    mart_customer_analysis = (
        fact_sales
        .join(
            dim_customer.select("customer_id", "customer_state"),
            "customer_id",
            "inner"
        )
        .groupBy("customer_id", "customer_state")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("payment_value").alias("total_spent")
        )
        .withColumn(
            "avg_order_value",
            col("total_spent") / col("total_orders")
        )
        .select(
            col("customer_id"),
            col("customer_state"),
            col("total_orders"),
            col("total_spent"),
            col("avg_order_value")
        )
        .orderBy(col("total_spent").desc())
    )
    
    return mart_customer_analysis


@asset("mart_seller_performance")
def create_mart_seller_performance(spark: SparkSession) -> DataFrame:
    """
    Create mart_seller_performance - seller performance analysis.
    
    Group by: seller_id, seller_state
    Metrics:
    - total_orders: COUNT(DISTINCT order_id)
    - total_revenue: SUM(payment_value)
    - avg_review_score: AVG(review_score) from fact_reviews
    """
    # Read Gold layer tables
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    fact_reviews = read_delta_table(spark, "gold", "fact_reviews")
    dim_seller = read_delta_table(spark, "gold", "dim_seller")
    
    # Calculate seller sales metrics
    seller_sales = (
        fact_sales
        .join(
            dim_seller.select("seller_id", "seller_state"),
            "seller_id",
            "inner"
        )
        .groupBy("seller_id", "seller_state")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("payment_value").alias("total_revenue")
        )
    )
    
    # Calculate avg review score per seller (via order_id)
    seller_reviews = (
        fact_sales
        .select("seller_id", "order_id")
        .distinct()
        .join(
            fact_reviews.select("order_id", "review_score"),
            "order_id",
            "inner"
        )
        .groupBy("seller_id")
        .agg(
            spark_avg("review_score").alias("avg_review_score")
        )
    )
    
    # Join sales and reviews
    mart_seller_performance = (
        seller_sales
        .join(seller_reviews, "seller_id", "left")
        .select(
            col("seller_id"),
            col("seller_state"),
            col("total_orders"),
            col("total_revenue"),
            col("avg_review_score")
        )
        .orderBy(col("total_revenue").desc())
    )
    
    return mart_seller_performance


# =============================================================================
# ORCHESTRATION
# =============================================================================

def run_mart_sales(spark: SparkSession) -> None:
    """Run mart_sales transformation."""
    print("\n" + "="*60)
    print("CREATING MART_SALES")
    print("="*60)
    
    create_mart_sales(spark)
    
    print("\n✓ mart_sales created successfully!")


def run_kpi_tables(spark: SparkSession) -> None:
    """Run all KPI table transformations."""
    print("\n" + "="*60)
    print("CREATING KPI TABLES")
    print("="*60)
    
    create_mart_kpi_daily(spark)
    create_mart_top_products(spark)
    create_mart_customer_analysis(spark)
    create_mart_seller_performance(spark)
    
    print("\n✓ All KPI tables created successfully!")


def run_all(spark: SparkSession) -> None:
    """Run complete Platinum layer transformation."""
    print("\n" + "="*60)
    print("STARTING PLATINUM LAYER - DATA MART TRANSFORMATION")
    print("="*60)
    
    # Create main mart first
    run_mart_sales(spark)
    
    # Then create KPI tables
    run_kpi_tables(spark)
    
    print("\n" + "="*60)
    print("PLATINUM LAYER TRANSFORMATION COMPLETED!")
    print("="*60)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Platinum Layer Transformations (Data Mart / BI Layer)")
    parser.add_argument(
        "--table", 
        type=str, 
        required=True,
        choices=[
            "mart_sales", "mart_kpi_daily", "mart_top_products",
            "mart_customer_analysis", "mart_seller_performance",
            "kpi", "all"
        ],
        help="Name of the table to transform or 'all' for complete transformation"
    )
    args = parser.parse_args()

    # Initialize Spark
    spark = get_spark_session(app_name=f"PlatinumLayer-{args.table}")

    try:
        # Route to the correct transformation
        if args.table == "mart_sales":
            create_mart_sales(spark)
        elif args.table == "mart_kpi_daily":
            create_mart_kpi_daily(spark)
        elif args.table == "mart_top_products":
            create_mart_top_products(spark)
        elif args.table == "mart_customer_analysis":
            create_mart_customer_analysis(spark)
        elif args.table == "mart_seller_performance":
            create_mart_seller_performance(spark)
        elif args.table == "kpi":
            run_kpi_tables(spark)
        elif args.table == "all":
            run_all(spark)
        else:
            print(f"Unknown table parameter: {args.table}")
    finally:
        spark.stop()
