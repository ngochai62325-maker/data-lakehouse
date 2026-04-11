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
    col, countDistinct, count, lit, round as spark_round, when, max as spark_max,
    sum as spark_sum, avg as spark_avg, lag, months_between, to_date, concat_ws,
    year as spark_year, month as spark_month, quarter as spark_quarter,
    date_format, datediff
)
from pyspark.sql.window import Window

from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table


# =============================================================================
# ASSET DECORATOR
# =============================================================================

def asset(table_name: str, partition_by: str = None):
    """
    Decorator to register a function as a Platinum layer asset.
    
    Args:
        table_name: Name of the output table in Platinum layer.
        partition_by: Optional column name to partition output by.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(spark: SparkSession, *args, **kwargs) -> DataFrame:
            print(f"\n{'='*60}")
            print(f"  Processing Platinum Asset: {table_name}")
            print(f"{'='*60}")
            
            # Execute transformation
            df = func(spark, *args, **kwargs)
            
            # Write to Platinum layer with optional partitioning
            if partition_by:
                write_delta_table(
                    df.repartition(col(partition_by)),
                    "platinum", table_name, mode="overwrite"
                )
            else:
                write_delta_table(df, "platinum", table_name, mode="overwrite")
            
            # Log metadata
            metadata = _get_metadata(df, table_name)
            _log_metadata(metadata)
            
            return df
        
        wrapper.table_name = table_name
        wrapper.is_asset = True
        return wrapper
    return decorator


def _get_metadata(df: DataFrame, table_name: str) -> Dict[str, Any]:
    """Extract metadata from DataFrame."""
    return {
        "table_name": table_name,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns,
        "schema": df.dtypes,
    }


def _log_metadata(metadata: Dict[str, Any]) -> None:
    """Log metadata to console."""
    print(f"\n--- Metadata for {metadata['table_name']} ---")
    print(f"  Table Name   : {metadata['table_name']}")
    print(f"  Row Count    : {metadata['row_count']:,}")
    print(f"  Column Count : {metadata['column_count']}")
    print(f"  Columns      : {', '.join(metadata['columns'])}")
    print(f"  Schema       :")
    for col_name, col_type in metadata["schema"]:
        print(f"    - {col_name}: {col_type}")
    print("-" * 40)


# =============================================================================
# HELPER: READ & CACHE GOLD TABLES
# =============================================================================

_gold_cache: Dict[str, DataFrame] = {}


def _read_gold(spark: SparkSession, table_name: str, use_cache: bool = True) -> DataFrame:
    """
    Read a Gold layer table with optional caching.
    Avoids re-reading the same table from S3 multiple times.
    """
    if use_cache and table_name in _gold_cache:
        print(f"  [cache hit] {table_name}")
        return _gold_cache[table_name]
    
    print(f"  [reading] gold/{table_name}")
    df = read_delta_table(spark, "gold", table_name)
    
    if use_cache:
        df = df.cache()
        _gold_cache[table_name] = df
    
    return df


def _clear_cache() -> None:
    """Unpersist all cached DataFrames."""
    for name, df in _gold_cache.items():
        try:
            df.unpersist()
            print(f"  [unpersist] {name}")
        except Exception:
            pass
    _gold_cache.clear()


# =============================================================================
# MART 1: SALES SUMMARY MART
# Granularity: day / month
# Pre-computed sales KPIs for dashboard time-series
# =============================================================================

@asset("sales_summary_mart", partition_by="purchase_date_key")
def create_sales_summary_mart(spark: SparkSession) -> DataFrame:
    """
    Sales Summary Mart — daily/monthly sales KPIs.
    
    Granularity: purchase_date_key (day)
    
    KPIs included:
    - total_revenue       = SUM(total_item_value)
    - total_payment       = SUM(total_payment_value)
    - total_orders        = COUNT(DISTINCT order_id)
    - total_items         = COUNT(order_item_id)
    - avg_order_value     = total_revenue / total_orders
    - avg_item_price      = AVG(price)
    - total_freight       = SUM(freight_value)
    - revenue_mom_growth  = MoM revenue growth %
    """
    fact_sales = _read_gold(spark, "fact_sales")
    dim_date = _read_gold(spark, "dim_date")
    
    # Daily aggregation
    daily_agg = (
        fact_sales
        .groupBy("purchase_date_key")
        .agg(
            spark_sum("total_item_value").alias("total_revenue"),
            spark_sum("total_payment_value").alias("total_payment"),
            countDistinct("order_id").alias("total_orders"),
            count("order_item_id").alias("total_items"),
            spark_avg("price").alias("avg_item_price"),
            spark_sum("freight_value").alias("total_freight"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("total_revenue") / col("total_orders"), 2)
        )
        .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
        .withColumn("total_payment", spark_round(col("total_payment"), 2))
        .withColumn("avg_item_price", spark_round(col("avg_item_price"), 2))
        .withColumn("total_freight", spark_round(col("total_freight"), 2))
    )
    
    # Join with dim_date for calendar attributes
    result = (
        daily_agg
        .join(
            dim_date.select(
                col("date_key"),
                col("full_date"),
                col("year"),
                col("month"),
                col("quarter"),
                col("day_of_week"),
                col("day_name"),
                col("is_weekend"),
            ),
            daily_agg["purchase_date_key"] == dim_date["date_key"],
            "inner"
        )
        .drop("date_key")
    )
    
    # MoM revenue growth (using monthly window)
    monthly_window = Window.orderBy("purchase_date_key")
    
    result = (
        result
        .withColumn(
            "_prev_revenue",
            lag("total_revenue", 1).over(monthly_window)
        )
        .withColumn(
            "revenue_day_growth_pct",
            spark_round(
                when(
                    col("_prev_revenue").isNotNull() & (col("_prev_revenue") != 0),
                    ((col("total_revenue") - col("_prev_revenue")) / col("_prev_revenue")) * 100
                ).otherwise(lit(None)),
                2
            )
        )
        .drop("_prev_revenue")
        .select(
            col("purchase_date_key"),
            col("full_date"),
            col("year"),
            col("month"),
            col("quarter"),
            col("day_of_week"),
            col("day_name"),
            col("is_weekend"),
            col("total_revenue"),
            col("total_payment"),
            col("total_orders"),
            col("total_items"),
            col("avg_order_value"),
            col("avg_item_price"),
            col("total_freight"),
            col("revenue_day_growth_pct"),
        )
        .orderBy("purchase_date_key")
    )
    
    return result


# =============================================================================
# MART 2: CUSTOMER MART
# Granularity: customer
# Customer-level aggregation for segmentation & retention analysis
# =============================================================================

@asset("customer_mart")
def create_customer_mart(spark: SparkSession) -> DataFrame:
    """
    Customer Mart — customer-level behavior metrics.
    
    Granularity: customer_id
    
    KPIs included:
    - total_orders          = COUNT(DISTINCT order_id)
    - total_spent           = SUM(total_item_value)
    - avg_order_value       = total_spent / total_orders
    - total_items_purchased = COUNT(order_item_id)
    - first_purchase_date   = MIN(purchase_date_key)
    - last_purchase_date    = MAX(purchase_date_key)
    - is_repeat_customer    = 1 if total_orders > 1
    - customer_lifetime_days= last - first purchase
    """
    fact_sales = _read_gold(spark, "fact_sales")
    dim_customers = _read_gold(spark, "dim_customers")
    dim_date = _read_gold(spark, "dim_date")
    
    # Customer-level aggregation from fact_sales
    customer_agg = (
        fact_sales
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("total_item_value").alias("total_spent"),
            count("order_item_id").alias("total_items_purchased"),
            spark_sum("freight_value").alias("total_freight_paid"),
            spark_max("purchase_date_key").alias("last_purchase_date_key"),
            spark_avg("price").alias("avg_item_price"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("total_spent") / col("total_orders"), 2)
        )
        .withColumn("total_spent", spark_round(col("total_spent"), 2))
        .withColumn("total_freight_paid", spark_round(col("total_freight_paid"), 2))
        .withColumn("avg_item_price", spark_round(col("avg_item_price"), 2))
        .withColumn(
            "is_repeat_customer",
            when(col("total_orders") > 1, lit(1)).otherwise(lit(0))
        )
    )
    
    # Join with dim_customers for geography
    result = (
        customer_agg
        .join(
            dim_customers.select(
                col("customer_id"),
                col("customer_unique_id"),
                col("customer_city"),
                col("customer_state"),
                col("customer_zip_code_prefix"),
            ),
            "customer_id",
            "inner"
        )
        # Join with dim_date to get the actual last purchase date
        .join(
            dim_date.select(
                col("date_key").alias("last_purchase_date_key_join"),
                col("full_date").alias("last_purchase_date"),
            ),
            col("last_purchase_date_key") == col("last_purchase_date_key_join"),
            "left"
        )
        .drop("last_purchase_date_key_join")
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_city"),
            col("customer_state"),
            col("customer_zip_code_prefix"),
            col("total_orders"),
            col("total_spent"),
            col("avg_order_value"),
            col("total_items_purchased"),
            col("total_freight_paid"),
            col("avg_item_price"),
            col("last_purchase_date_key"),
            col("last_purchase_date"),
            col("is_repeat_customer"),
        )
        .orderBy(col("total_spent").desc())
    )
    
    return result


# =============================================================================
# MART 3: PRODUCT MART
# Granularity: product
# Product-level performance metrics
# =============================================================================

@asset("product_mart")
def create_product_mart(spark: SparkSession) -> DataFrame:
    """
    Product Mart — product-level performance metrics.
    
    Granularity: product_id
    
    KPIs included:
    - category              = product category (English)
    - total_sales           = SUM(total_item_value)
    - total_quantity        = COUNT(order_item_id)
    - total_orders          = COUNT(DISTINCT order_id)
    - avg_price             = AVG(price)
    - avg_freight           = AVG(freight_value)
    - total_freight         = SUM(freight_value)
    - revenue_rank          = rank by total_sales DESC
    """
    fact_sales = _read_gold(spark, "fact_sales")
    dim_products = _read_gold(spark, "dim_products")
    
    # Product-level aggregation
    product_agg = (
        fact_sales
        .groupBy("product_id")
        .agg(
            spark_sum("total_item_value").alias("total_sales"),
            count("order_item_id").alias("total_quantity"),
            countDistinct("order_id").alias("total_orders"),
            spark_avg("price").alias("avg_price"),
            spark_avg("freight_value").alias("avg_freight"),
            spark_sum("freight_value").alias("total_freight"),
        )
        .withColumn("total_sales", spark_round(col("total_sales"), 2))
        .withColumn("avg_price", spark_round(col("avg_price"), 2))
        .withColumn("avg_freight", spark_round(col("avg_freight"), 2))
        .withColumn("total_freight", spark_round(col("total_freight"), 2))
    )
    
    # Join with dim_products for category info
    result = (
        product_agg
        .join(
            dim_products.select(
                col("product_id"),
                col("product_category_en").alias("category"),
                col("product_weight_g"),
                col("product_length_cm"),
                col("product_height_cm"),
                col("product_width_cm"),
            ),
            "product_id",
            "inner"
        )
        .select(
            col("product_id"),
            col("category"),
            col("total_sales"),
            col("total_quantity"),
            col("total_orders"),
            col("avg_price"),
            col("avg_freight"),
            col("total_freight"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm"),
        )
        .orderBy(col("total_sales").desc())
    )
    
    return result


# =============================================================================
# MART 4: FULFILLMENT MART
# Granularity: order
# Order-level delivery & review metrics
# =============================================================================

@asset("fulfillment_mart")
def create_fulfillment_mart(spark: SparkSession) -> DataFrame:
    """
    Fulfillment Mart — order-level delivery & customer experience metrics.
    
    Granularity: order_id
    
    KPIs included:
    - delivery_delay_days       = actual - estimated delivery
    - shipping_duration_days    = delivered - purchased
    - is_late_delivery          = 1 if delivery_delay_days > 0
    - review_score              = customer review score
    - is_positive_review        = 1 if review_score >= 4
    - order_status              = current order status
    - purchase_date_key         = date of purchase
    - estimated_delivery_date_key
    - actual_delivery_date_key
    """
    fact_fulfillment = _read_gold(spark, "fact_order_fulfillment")
    dim_customers = _read_gold(spark, "dim_customers")
    
    result = (
        fact_fulfillment
        .join(
            dim_customers.select(
                col("customer_id"),
                col("customer_city"),
                col("customer_state"),
            ),
            "customer_id",
            "inner"
        )
        .withColumn(
            "is_positive_review",
            when(col("review_score") >= 4, lit(1)).otherwise(lit(0))
        )
        .withColumn(
            "delivery_status",
            when(col("is_late_delivery") == 1, lit("LATE"))
            .when(col("delivery_delay_days").isNull(), lit("PENDING"))
            .otherwise(lit("ON_TIME"))
        )
        .select(
            col("order_id"),
            col("customer_id"),
            col("customer_city"),
            col("customer_state"),
            col("order_status"),
            col("purchase_date_key"),
            col("estimated_delivery_date_key"),
            col("actual_delivery_date_key"),
            col("delivery_delay_days"),
            col("shipping_duration_days"),
            col("is_late_delivery"),
            col("delivery_status"),
            col("review_id"),
            col("review_score"),
            col("is_positive_review"),
        )
        .orderBy("purchase_date_key")
    )
    
    return result


# =============================================================================
# MART 5: GEO SALES MART
# Granularity: location (state / city)
# Geographic revenue & order distribution
# =============================================================================

@asset("geo_sales_mart")
def create_geo_sales_mart(spark: SparkSession) -> DataFrame:
    """
    Geographic Sales Mart — location-level sales aggregation.
    
    Granularity: customer_state + customer_city
    
    KPIs included:
    - total_revenue      = SUM(total_item_value)
    - total_orders       = COUNT(DISTINCT order_id)
    - total_items        = COUNT(order_item_id)
    - total_customers    = COUNT(DISTINCT customer_id)
    - avg_order_value    = total_revenue / total_orders
    - avg_freight        = AVG(freight_value)
    """
    fact_sales = _read_gold(spark, "fact_sales")
    dim_customers = _read_gold(spark, "dim_customers")
    
    result = (
        fact_sales
        .join(
            dim_customers.select(
                col("customer_id"),
                col("customer_city"),
                col("customer_state"),
            ),
            "customer_id",
            "inner"
        )
        .groupBy("customer_state", "customer_city")
        .agg(
            spark_sum("total_item_value").alias("total_revenue"),
            countDistinct("order_id").alias("total_orders"),
            count("order_item_id").alias("total_items"),
            countDistinct("customer_id").alias("total_customers"),
            spark_avg("freight_value").alias("avg_freight"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("total_revenue") / col("total_orders"), 2)
        )
        .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
        .withColumn("avg_freight", spark_round(col("avg_freight"), 2))
        .select(
            col("customer_state").alias("state"),
            col("customer_city").alias("city"),
            col("total_revenue"),
            col("total_orders"),
            col("total_items"),
            col("total_customers"),
            col("avg_order_value"),
            col("avg_freight"),
        )
        .orderBy(col("total_revenue").desc())
    )
    
    return result


# =============================================================================
# BONUS: KPI SUMMARY TABLE
# Pre-computed global KPIs for dashboard header cards
# =============================================================================

@asset("kpi_summary")
def create_kpi_summary(spark: SparkSession) -> DataFrame:
    """
    KPI Summary — global pre-computed KPIs for BI dashboard cards.
    
    Single-row table with all headline metrics:
    
    Sales KPIs:
    - total_revenue, total_orders, avg_order_value, revenue_per_customer
    
    Fulfillment KPIs:
    - avg_delivery_time, late_delivery_rate_pct, avg_shipping_duration,
      on_time_delivery_rate_pct
    
    Customer Experience KPIs:
    - avg_review_score, positive_review_rate_pct, total_customers,
      repeat_customer_count
    """
    fact_sales = _read_gold(spark, "fact_sales")
    fact_fulfillment = _read_gold(spark, "fact_order_fulfillment")
    
    # --- Sales KPIs ---
    sales_kpis = fact_sales.agg(
        spark_round(spark_sum("total_item_value"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        count("order_item_id").alias("total_items"),
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("seller_id").alias("total_sellers"),
        countDistinct("product_id").alias("total_products"),
    ).withColumn(
        "avg_order_value",
        spark_round(col("total_revenue") / col("total_orders"), 2)
    ).withColumn(
        "revenue_per_customer",
        spark_round(col("total_revenue") / col("total_customers"), 2)
    )
    
    # --- Fulfillment KPIs ---
    # Filter to delivered orders only for meaningful delivery metrics
    delivered = fact_fulfillment.filter(
        col("shipping_duration_days").isNotNull()
    )
    
    fulfillment_kpis = delivered.agg(
        spark_round(spark_avg("shipping_duration_days"), 2).alias("avg_delivery_time_days"),
        spark_round(spark_avg("delivery_delay_days"), 2).alias("avg_delivery_delay_days"),
        spark_round(
            (spark_sum(col("is_late_delivery")) / count("*")) * 100, 2
        ).alias("late_delivery_rate_pct"),
    ).withColumn(
        "on_time_delivery_rate_pct",
        spark_round(lit(100) - col("late_delivery_rate_pct"), 2)
    )
    
    # --- Customer Experience KPIs ---
    reviews = fact_fulfillment.filter(col("review_score").isNotNull())
    
    review_kpis = reviews.agg(
        spark_round(spark_avg("review_score"), 2).alias("avg_review_score"),
        spark_round(
            (spark_sum(when(col("review_score") >= 4, 1).otherwise(0)) / count("*")) * 100, 2
        ).alias("positive_review_rate_pct"),
        count("*").alias("total_reviews"),
    )
    
    # --- Repeat Customer Count ---
    repeat_customers = (
        fact_sales
        .groupBy("customer_id")
        .agg(countDistinct("order_id").alias("order_count"))
        .filter(col("order_count") > 1)
        .agg(count("*").alias("repeat_customer_count"))
    )
    
    # Cross join all single-row KPIs into one flat row
    result = (
        sales_kpis
        .crossJoin(fulfillment_kpis)
        .crossJoin(review_kpis)
        .crossJoin(repeat_customers)
        .withColumn(
            "repeat_customer_rate_pct",
            spark_round(
                (col("repeat_customer_count") / col("total_customers")) * 100, 2
            )
        )
    )
    
    return result


# =============================================================================
# BONUS: SELLER PERFORMANCE MART
# Granularity: seller
# =============================================================================

@asset("seller_performance_mart")
def create_seller_performance_mart(spark: SparkSession) -> DataFrame:
    """
    Seller Performance Mart — seller-level performance metrics.
    
    Granularity: seller_id
    
    KPIs included:
    - total_orders, total_revenue, total_items
    - avg_order_value
    - avg_review_score (from fulfillment/reviews)
    """
    fact_sales = _read_gold(spark, "fact_sales")
    fact_fulfillment = _read_gold(spark, "fact_order_fulfillment")
    dim_sellers = _read_gold(spark, "dim_sellers")
    
    # Seller sales metrics
    seller_sales = (
        fact_sales
        .groupBy("seller_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("total_item_value").alias("total_revenue"),
            count("order_item_id").alias("total_items"),
            spark_avg("price").alias("avg_item_price"),
            spark_sum("freight_value").alias("total_freight"),
            countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
        .withColumn("avg_item_price", spark_round(col("avg_item_price"), 2))
        .withColumn("total_freight", spark_round(col("total_freight"), 2))
        .withColumn(
            "avg_order_value",
            spark_round(col("total_revenue") / col("total_orders"), 2)
        )
    )
    
    # Seller review metrics (via order_id linkage)
    seller_reviews = (
        fact_sales
        .select("seller_id", "order_id")
        .distinct()
        .join(
            fact_fulfillment.select("order_id", "review_score")
                .filter(col("review_score").isNotNull()),
            "order_id",
            "inner"
        )
        .groupBy("seller_id")
        .agg(
            spark_round(spark_avg("review_score"), 2).alias("avg_review_score"),
            count("*").alias("total_reviews"),
            spark_round(
                (spark_sum(when(col("review_score") >= 4, 1).otherwise(0)) / count("*")) * 100, 2
            ).alias("positive_review_rate_pct"),
        )
    )
    
    # Join with dim_sellers for geography
    result = (
        seller_sales
        .join(seller_reviews, "seller_id", "left")
        .join(
            dim_sellers.select(
                col("seller_id"),
                col("seller_city"),
                col("seller_state"),
                col("seller_zip_code_prefix"),
            ),
            "seller_id",
            "inner"
        )
        .select(
            col("seller_id"),
            col("seller_city"),
            col("seller_state"),
            col("seller_zip_code_prefix"),
            col("total_orders"),
            col("total_revenue"),
            col("total_items"),
            col("avg_order_value"),
            col("avg_item_price"),
            col("total_freight"),
            col("unique_customers"),
            col("avg_review_score"),
            col("total_reviews"),
            col("positive_review_rate_pct"),
        )
        .orderBy(col("total_revenue").desc())
    )
    
    return result


# =============================================================================
# ORCHESTRATION
# =============================================================================

# Registry of all platinum assets
PLATINUM_ASSETS = {
    "sales_summary_mart":       create_sales_summary_mart,
    "customer_mart":            create_customer_mart,
    "product_mart":             create_product_mart,
    "fulfillment_mart":         create_fulfillment_mart,
    "geo_sales_mart":           create_geo_sales_mart,
    "kpi_summary":              create_kpi_summary,
    "seller_performance_mart":  create_seller_performance_mart,
}


def run_single_mart(spark: SparkSession, mart_name: str) -> None:
    """Run a single Platinum mart by name."""
    if mart_name not in PLATINUM_ASSETS:
        raise ValueError(f"Unknown mart: {mart_name}. Available: {list(PLATINUM_ASSETS.keys())}")
    
    print(f"\n{'='*60}")
    print(f"  PLATINUM LAYER — Running: {mart_name}")
    print(f"{'='*60}")
    
    PLATINUM_ASSETS[mart_name](spark)
    
    print(f"\n✓ {mart_name} created successfully!")


def run_all_marts(spark: SparkSession) -> None:
    """Run all Platinum layer marts sequentially."""
    print(f"\n{'='*60}")
    print(f"  PLATINUM LAYER — FULL TRANSFORMATION")
    print(f"  Total marts to build: {len(PLATINUM_ASSETS)}")
    print(f"{'='*60}")
    
    for i, (name, func) in enumerate(PLATINUM_ASSETS.items(), 1):
        print(f"\n[{i}/{len(PLATINUM_ASSETS)}] Building {name}...")
        func(spark)
        print(f"✓ [{i}/{len(PLATINUM_ASSETS)}] {name} — done")
    
    # Clear cache after all marts are built
    _clear_cache()
    
    print(f"\n{'='*60}")
    print(f"  PLATINUM LAYER TRANSFORMATION COMPLETED!")
    print(f"  {len(PLATINUM_ASSETS)} marts built successfully")
    print(f"{'='*60}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    all_choices = list(PLATINUM_ASSETS.keys()) + ["all"]
    
    parser = argparse.ArgumentParser(
        description="Platinum Layer (BI Layer) — Data Mart Transformations"
    )
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        choices=all_choices,
        help=f"Mart to build. Options: {', '.join(all_choices)}"
    )
    args = parser.parse_args()

    # Initialize Spark
    spark = get_spark_session(app_name=f"PlatinumLayer-{args.table}")

    try:
        if args.table == "all":
            run_all_marts(spark)
        else:
            run_single_mart(spark, args.table)
    finally:
        _clear_cache()
        spark.stop()
        print("\nSpark session stopped.")
