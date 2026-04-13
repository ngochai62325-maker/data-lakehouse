"""
Platinum Layer (BI Layer) — PySpark Transformations
====================================================

Transforms Gold layer data into optimized data marts for BI tools
(Power BI, Athena). Part of the Medallion Architecture:

    Bronze → Silver → Gold → **Platinum**

Data Marts:
    1. sales_summary_mart  — Daily sales KPIs (time-series)
    2. customer_mart        — Customer-level behavior metrics
    3. product_mart         — Product-level performance
    4. kpi_summary          — Global headline KPIs (single row)

Usage:
    python -m etl_pipeline.processing.gold_to_platinum --table all
    python -m etl_pipeline.processing.gold_to_platinum --table sales_summary_mart
"""

import sys
import os
import argparse

# --- PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
# ----------------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, countDistinct, count, lit, round as spark_round, when,
    max as spark_max, sum as spark_sum, avg as spark_avg, lag
)
from pyspark.sql.window import Window

from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table


# =============================================================================
# SCHEMA NORMALIZATION — Tránh lỗi HIVE_BAD_DATA khi query bằng Athena
# =============================================================================
#
# Vấn đề: Spark có thể ghi Parquet với type INT32, nhưng Glue Catalog
#          định nghĩa DOUBLE → Athena reject file → lỗi toàn bộ mart.
#
# Giải pháp: Cast toàn bộ column theo quy tắc nhất quán trước khi write.
# =============================================================================

# Mapping: tên cột → Spark type (khớp 100% với Glue Catalog)
SCHEMA_OVERRIDES = {
    # ---------- Sales Summary Mart ----------
    "purchase_date_key":          "int",
    "full_date":                  "date",
    "year":                       "int",
    "month":                      "int",
    "quarter":                    "int",
    "day_of_week":                "int",
    "day_name":                   "string",
    "is_weekend":                 "boolean",
    "total_revenue":              "double",
    "total_payment":              "double",
    "total_orders":               "long",      # Spark long = Athena bigint
    "total_items":                "long",
    "avg_order_value":            "double",
    "avg_item_price":             "double",
    "total_freight":              "double",
    "revenue_day_growth_pct":     "double",

    # ---------- Customer Mart ----------
    "customer_id":                "string",
    "customer_unique_id":         "string",
    "customer_city":              "string",
    "customer_state":             "string",
    "customer_zip_code_prefix":   "string",
    "total_spent":                "double",
    "total_items_purchased":      "long",
    "total_freight_paid":         "double",
    "last_purchase_date_key":     "int",
    "last_purchase_date":         "date",
    "is_repeat_customer":         "int",

    # ---------- Product Mart ----------
    "product_id":                 "string",
    "category":                   "string",
    "total_sales":                "double",
    "total_quantity":             "long",
    "avg_price":                  "double",
    "avg_freight":                "double",
    "product_weight_g":           "double",
    "product_length_cm":          "double",
    "product_height_cm":          "double",
    "product_width_cm":           "double",

    # ---------- KPI Summary ----------
    "total_customers":            "long",
    "total_sellers":              "long",
    "total_products":             "long",
    "revenue_per_customer":       "double",
    "avg_delivery_time_days":     "double",
    "avg_delivery_delay_days":    "double",
    "late_delivery_rate_pct":     "double",
    "on_time_delivery_rate_pct":  "double",
    "avg_review_score":           "double",
    "positive_review_rate_pct":   "double",
    "total_reviews":              "long",
    "repeat_customer_count":      "long",
    "repeat_customer_rate_pct":   "double",
}


def normalize_schema(df: DataFrame) -> DataFrame:
    """
    Cast toàn bộ column của DataFrame theo SCHEMA_OVERRIDES.
    Column nào không có trong map → giữ nguyên type gốc.
    
    Đảm bảo Parquet output khớp 100% với Glue Catalog schema,
    tránh lỗi HIVE_BAD_DATA khi query bằng Athena / Power BI.
    """
    cast_cols = []
    for field in df.schema.fields:
        target_type = SCHEMA_OVERRIDES.get(field.name)
        if target_type and str(field.dataType).lower().replace("type", "") != target_type:
            cast_cols.append(col(field.name).cast(target_type).alias(field.name))
        else:
            cast_cols.append(col(field.name))
    return df.select(*cast_cols)


# =============================================================================
# MART 1: SALES SUMMARY — Daily sales KPIs
# Granularity: purchase_date_key (ngày)
# =============================================================================

def create_sales_summary_mart(spark: SparkSession) -> DataFrame:
    """KPIs: total_revenue, total_orders, avg_order_value, MoM growth..."""
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    dim_date = read_delta_table(spark, "gold", "dim_date")

    # Aggregate theo ngày
    daily = (
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
        .withColumn("avg_order_value", spark_round(col("total_revenue") / col("total_orders"), 2))
        .withColumn("total_revenue",   spark_round(col("total_revenue"), 2))
        .withColumn("total_payment",   spark_round(col("total_payment"), 2))
        .withColumn("avg_item_price",  spark_round(col("avg_item_price"), 2))
        .withColumn("total_freight",   spark_round(col("total_freight"), 2))
    )

    # Join dim_date → lấy calendar attributes
    result = daily.join(
        dim_date.select("date_key", "full_date", "year", "month",
                        "quarter", "day_of_week", "day_name", "is_weekend"),
        daily["purchase_date_key"] == dim_date["date_key"], "inner"
    ).drop("date_key")

    # Revenue day-over-day growth %
    w = Window.orderBy("purchase_date_key")
    result = (
        result
        .withColumn("_prev", lag("total_revenue", 1).over(w))
        .withColumn(
            "revenue_day_growth_pct",
            spark_round(
                when((col("_prev").isNotNull()) & (col("_prev") != 0),
                     ((col("total_revenue") - col("_prev")) / col("_prev")) * 100
                ).otherwise(lit(None)), 2
            )
        )
        .drop("_prev")
        .orderBy("purchase_date_key")
    )

    return normalize_schema(result)


# =============================================================================
# MART 2: CUSTOMER — Customer-level behavior metrics
# Granularity: customer_id
# =============================================================================

def create_customer_mart(spark: SparkSession) -> DataFrame:
    """KPIs: total_spent, total_orders, avg_order_value, is_repeat..."""
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    dim_customers = read_delta_table(spark, "gold", "dim_customers")
    dim_date = read_delta_table(spark, "gold", "dim_date")

    cust = (
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
        .withColumn("avg_order_value",   spark_round(col("total_spent") / col("total_orders"), 2))
        .withColumn("total_spent",       spark_round(col("total_spent"), 2))
        .withColumn("total_freight_paid", spark_round(col("total_freight_paid"), 2))
        .withColumn("avg_item_price",    spark_round(col("avg_item_price"), 2))
        .withColumn("is_repeat_customer", when(col("total_orders") > 1, lit(1)).otherwise(lit(0)))
    )

    result = (
        cust
        .join(dim_customers.select("customer_id", "customer_unique_id",
              "customer_city", "customer_state", "customer_zip_code_prefix"),
              "customer_id", "inner")
        .join(dim_date.select(col("date_key").alias("_dk"), col("full_date").alias("last_purchase_date")),
              col("last_purchase_date_key") == col("_dk"), "left")
        .drop("_dk")
        .select(
            "customer_id", "customer_unique_id", "customer_city",
            "customer_state", "customer_zip_code_prefix",
            "total_orders", "total_spent", "avg_order_value",
            "total_items_purchased", "total_freight_paid", "avg_item_price",
            "last_purchase_date_key", "last_purchase_date", "is_repeat_customer",
        )
        .orderBy(col("total_spent").desc())
    )

    return normalize_schema(result)


# =============================================================================
# MART 3: PRODUCT — Product-level performance
# Granularity: product_id
# =============================================================================

def create_product_mart(spark: SparkSession) -> DataFrame:
    """KPIs: total_sales, total_quantity, avg_price, category..."""
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    dim_products = read_delta_table(spark, "gold", "dim_products")

    prod = (
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
        .withColumn("total_sales",   spark_round(col("total_sales"), 2))
        .withColumn("avg_price",     spark_round(col("avg_price"), 2))
        .withColumn("avg_freight",   spark_round(col("avg_freight"), 2))
        .withColumn("total_freight", spark_round(col("total_freight"), 2))
    )

    result = (
        prod
        .join(dim_products.select(
            "product_id",
            col("product_category_en").alias("category"),
            "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm"),
            "product_id", "inner")
        .select(
            "product_id", "category", "total_sales", "total_quantity",
            "total_orders", "avg_price", "avg_freight", "total_freight",
            "product_weight_g", "product_length_cm",
            "product_height_cm", "product_width_cm",
        )
        .orderBy(col("total_sales").desc())
    )

    return normalize_schema(result)


# =============================================================================
# MART 4: KPI SUMMARY — Global headline KPIs (single row)
# =============================================================================

def create_kpi_summary(spark: SparkSession) -> DataFrame:
    """Pre-computed KPIs for BI dashboard header cards."""
    fact_sales = read_delta_table(spark, "gold", "fact_sales")
    fact_ff = read_delta_table(spark, "gold", "fact_order_fulfillment")

    # --- Sales ---
    sales = fact_sales.agg(
        spark_round(spark_sum("total_item_value"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        count("order_item_id").alias("total_items"),
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("seller_id").alias("total_sellers"),
        countDistinct("product_id").alias("total_products"),
    ).withColumn("avg_order_value", spark_round(col("total_revenue") / col("total_orders"), 2)) \
     .withColumn("revenue_per_customer", spark_round(col("total_revenue") / col("total_customers"), 2))

    # --- Fulfillment (chỉ đơn đã giao) ---
    delivered = fact_ff.filter(col("shipping_duration_days").isNotNull())
    ff_kpis = delivered.agg(
        spark_round(spark_avg("shipping_duration_days"), 2).alias("avg_delivery_time_days"),
        spark_round(spark_avg("delivery_delay_days"), 2).alias("avg_delivery_delay_days"),
        spark_round((spark_sum(col("is_late_delivery")) / count("*")) * 100, 2).alias("late_delivery_rate_pct"),
    ).withColumn("on_time_delivery_rate_pct", spark_round(lit(100) - col("late_delivery_rate_pct"), 2))

    # --- Reviews ---
    reviews = fact_ff.filter(col("review_score").isNotNull())
    rv_kpis = reviews.agg(
        spark_round(spark_avg("review_score"), 2).alias("avg_review_score"),
        spark_round((spark_sum(when(col("review_score") >= 4, 1).otherwise(0)) / count("*")) * 100, 2).alias("positive_review_rate_pct"),
        count("*").alias("total_reviews"),
    )

    # --- Repeat customers ---
    repeat = (
        fact_sales.groupBy("customer_id")
        .agg(countDistinct("order_id").alias("cnt"))
        .filter(col("cnt") > 1)
        .agg(count("*").alias("repeat_customer_count"))
    )

    # Cross join → 1 row with all KPIs
    result = (
        sales.crossJoin(ff_kpis).crossJoin(rv_kpis).crossJoin(repeat)
        .withColumn("repeat_customer_rate_pct",
                     spark_round((col("repeat_customer_count") / col("total_customers")) * 100, 2))
    )

    return normalize_schema(result)


# =============================================================================
# MAIN
# =============================================================================

MARTS = {
    "sales_summary_mart": create_sales_summary_mart,
    "customer_mart":      create_customer_mart,
    "product_mart":       create_product_mart,
    "kpi_summary":        create_kpi_summary,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Platinum Layer — Build BI Data Marts")
    parser.add_argument("--table", required=True,
                        choices=list(MARTS.keys()) + ["all"],
                        help="Mart to build, or 'all'")
    args = parser.parse_args()

    spark = get_spark_session(app_name=f"PlatinumLayer-{args.table}")

    try:
        targets = MARTS if args.table == "all" else {args.table: MARTS[args.table]}

        for name, func in targets.items():
            print(f"\n{'='*50}")
            print(f"  Building: {name}")
            print(f"{'='*50}")

            df = func(spark)
            write_delta_table(df, "platinum", name, mode="overwrite")

            print(f"  ✓ {name} — {df.count():,} rows, {len(df.columns)} cols")
    finally:
        spark.stop()
        print("\nDone. Spark stopped.")
