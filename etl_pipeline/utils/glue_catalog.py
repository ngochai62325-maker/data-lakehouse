"""
AWS Glue Data Catalog - Table Creation Script
==============================================

This module creates Glue tables for the Platinum layer to enable
querying via Amazon Athena and connecting to Power BI.

Platinum tables (4 marts):
  - sales_summary_mart  : Daily sales KPIs
  - customer_mart       : Customer-level behavior metrics
  - product_mart        : Product-level performance
  - kpi_summary         : Global pre-computed KPIs (single row)

Usage:
    python -m etl_pipeline.utils.glue_catalog --action create --table all
    python -m etl_pipeline.utils.glue_catalog --action create --table sales_summary_mart
    python -m etl_pipeline.utils.glue_catalog --action list
    python -m etl_pipeline.utils.glue_catalog --action delete --table all
"""

import sys
import os
import argparse
import boto3
from typing import Dict, List, Any

# --- PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
# ----------------

from config.settings import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_REGION, 
    S3_BUCKET
)


# =============================================================================
# CONFIGURATION
# =============================================================================

GLUE_DATABASE = "olist_lakehouse"
S3_PLATINUM_PATH = f"s3://{S3_BUCKET}/platinum"

# =============================================================================
# SCHEMA DEFINITIONS — Khớp với output của gold_to_platinum.py
# =============================================================================

PLATINUM_SCHEMAS: Dict[str, List[Dict[str, str]]] = {

    # ─── MART 1: Sales Summary ───────────────────────────────────────────────
    # Granularity: purchase_date_key (daily)
    # Source: fact_sales + dim_date
    "sales_summary_mart": [
        {"Name": "purchase_date_key", "Type": "int"},
        {"Name": "full_date", "Type": "date"},
        {"Name": "year", "Type": "int"},
        {"Name": "month", "Type": "int"},
        {"Name": "quarter", "Type": "int"},
        {"Name": "day_of_week", "Type": "int"},
        {"Name": "day_name", "Type": "string"},
        {"Name": "is_weekend", "Type": "boolean"},
        {"Name": "total_revenue", "Type": "double"},
        {"Name": "total_payment", "Type": "double"},
        {"Name": "total_orders", "Type": "bigint"},
        {"Name": "total_items", "Type": "bigint"},
        {"Name": "avg_order_value", "Type": "double"},
        {"Name": "avg_item_price", "Type": "double"},
        {"Name": "total_freight", "Type": "double"},
        {"Name": "revenue_day_growth_pct", "Type": "double"},
    ],

    # ─── MART 2: Customer ────────────────────────────────────────────────────
    # Granularity: customer_id
    # Source: fact_sales + dim_customers + dim_date
    "customer_mart": [
        {"Name": "customer_id", "Type": "string"},
        {"Name": "customer_unique_id", "Type": "string"},
        {"Name": "customer_city", "Type": "string"},
        {"Name": "customer_state", "Type": "string"},
        {"Name": "customer_zip_code_prefix", "Type": "string"},
        {"Name": "total_orders", "Type": "bigint"},
        {"Name": "total_spent", "Type": "double"},
        {"Name": "avg_order_value", "Type": "double"},
        {"Name": "total_items_purchased", "Type": "bigint"},
        {"Name": "total_freight_paid", "Type": "double"},
        {"Name": "avg_item_price", "Type": "double"},
        {"Name": "last_purchase_date_key", "Type": "int"},
        {"Name": "last_purchase_date", "Type": "date"},
        {"Name": "is_repeat_customer", "Type": "int"},
    ],

    # ─── MART 3: Product ─────────────────────────────────────────────────────
    # Granularity: product_id
    # Source: fact_sales + dim_products
    "product_mart": [
        {"Name": "product_id", "Type": "string"},
        {"Name": "category", "Type": "string"},
        {"Name": "total_sales", "Type": "double"},
        {"Name": "total_quantity", "Type": "bigint"},
        {"Name": "total_orders", "Type": "bigint"},
        {"Name": "avg_price", "Type": "double"},
        {"Name": "avg_freight", "Type": "double"},
        {"Name": "total_freight", "Type": "double"},
        {"Name": "product_weight_g", "Type": "double"},
        {"Name": "product_length_cm", "Type": "double"},
        {"Name": "product_height_cm", "Type": "double"},
        {"Name": "product_width_cm", "Type": "double"},
    ],

    # ─── MART 4: KPI Summary ────────────────────────────────────────────────
    # Granularity: single row (global KPIs)
    # Source: fact_sales + fact_order_fulfillment
    "kpi_summary": [
        {"Name": "total_revenue", "Type": "double"},
        {"Name": "total_orders", "Type": "bigint"},
        {"Name": "total_items", "Type": "bigint"},
        {"Name": "total_customers", "Type": "bigint"},
        {"Name": "total_sellers", "Type": "bigint"},
        {"Name": "total_products", "Type": "bigint"},
        {"Name": "avg_order_value", "Type": "double"},
        {"Name": "revenue_per_customer", "Type": "double"},
        {"Name": "avg_delivery_time_days", "Type": "double"},
        {"Name": "avg_delivery_delay_days", "Type": "double"},
        {"Name": "late_delivery_rate_pct", "Type": "double"},
        {"Name": "on_time_delivery_rate_pct", "Type": "double"},
        {"Name": "avg_review_score", "Type": "double"},
        {"Name": "positive_review_rate_pct", "Type": "double"},
        {"Name": "total_reviews", "Type": "bigint"},
        {"Name": "repeat_customer_count", "Type": "bigint"},
        {"Name": "repeat_customer_rate_pct", "Type": "double"},
    ],
}



# =============================================================================
# GLUE CLIENT
# =============================================================================

def get_glue_client():
    """Create Glue client with credentials."""
    return boto3.client(
        'glue',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def create_database(glue_client) -> bool:
    """Create Glue database if not exists."""
    try:
        glue_client.get_database(Name=GLUE_DATABASE)
        print(f"Database '{GLUE_DATABASE}' already exists.")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        pass
    
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': GLUE_DATABASE,
                'Description': 'Olist Lakehouse - Platinum Data Marts for BI/Analytics',
                'LocationUri': S3_PLATINUM_PATH,
            }
        )
        print(f"[OK] Database '{GLUE_DATABASE}' created successfully.")
        return True
    except Exception as e:
        print(f"[ERROR] Error creating database: {e}")
        return False


def delete_database(glue_client) -> bool:
    """Delete Glue database and all tables."""
    try:
        # Delete all tables first
        tables = glue_client.get_tables(DatabaseName=GLUE_DATABASE)
        for table in tables.get('TableList', []):
            glue_client.delete_table(
                DatabaseName=GLUE_DATABASE,
                Name=table['Name']
            )
            print(f"  Deleted table: {table['Name']}")
        
        # Delete database
        glue_client.delete_database(Name=GLUE_DATABASE)
        print(f"[OK] Database '{GLUE_DATABASE}' deleted successfully.")
        return True
    except Exception as e:
        print(f"[ERROR] Error deleting database: {e}")
        return False


# =============================================================================
# TABLE OPERATIONS
# =============================================================================

def _build_table_input(table_name: str, schema: List[Dict], location: str) -> Dict[str, Any]:
    """Build Glue TableInput dict for create/update operations."""
    return {
        'Name': table_name,
        'Description': f'Platinum layer - {table_name}',
        'StorageDescriptor': {
            'Columns': schema,
            'Location': location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            },
            'Compressed': True,
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'parquet',
            'compressionType': 'snappy',
            'typeOfData': 'file',
        },
    }


def create_table(glue_client, table_name: str) -> bool:
    """Create a single Glue table (or update if already exists)."""
    if table_name not in PLATINUM_SCHEMAS:
        print(f"[ERROR] Unknown table: {table_name}")
        print(f"  Available: {', '.join(PLATINUM_SCHEMAS.keys())}")
        return False
    
    schema = PLATINUM_SCHEMAS[table_name]
    location = f"{S3_PLATINUM_PATH}/{table_name}/"
    table_input = _build_table_input(table_name, schema, location)
    
    # Check if table exists → update instead
    try:
        glue_client.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        print(f"Table '{table_name}' already exists. Updating...")
        return update_table(glue_client, table_name, table_input)
    except glue_client.exceptions.EntityNotFoundException:
        pass
    
    try:
        glue_client.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input
        )
        print(f"[OK] Table '{table_name}' created successfully.")
        print(f"  Location : {location}")
        print(f"  Columns  : {len(schema)}")
        return True
    except Exception as e:
        print(f"[ERROR] Error creating table '{table_name}': {e}")
        return False


def update_table(glue_client, table_name: str, table_input: Dict) -> bool:
    """Update existing Glue table."""
    try:
        glue_client.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input
        )
        print(f"[OK] Table '{table_name}' updated successfully.")
        return True
    except Exception as e:
        print(f"[ERROR] Error updating table '{table_name}': {e}")
        return False


def delete_table(glue_client, table_name: str) -> bool:
    """Delete a single Glue table."""
    try:
        glue_client.delete_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        print(f"[OK] Table '{table_name}' deleted successfully.")
        return True
    except Exception as e:
        print(f"[ERROR] Error deleting table '{table_name}': {e}")
        return False


def create_all_tables(glue_client) -> None:
    """Create all Platinum layer tables."""
    print("\n" + "="*60)
    print("  CREATING ALL PLATINUM TABLES IN GLUE")
    print(f"  Database: {GLUE_DATABASE}")
    print(f"  Tables  : {len(PLATINUM_SCHEMAS)}")
    print("="*60 + "\n")
    
    # Ensure database exists
    create_database(glue_client)
    
    # Create each table
    success_count = 0
    for i, table_name in enumerate(PLATINUM_SCHEMAS.keys(), 1):
        print(f"\n[{i}/{len(PLATINUM_SCHEMAS)}] {table_name}")
        if create_table(glue_client, table_name):
            success_count += 1
    
    print("\n" + "="*60)
    print(f"  COMPLETED: {success_count}/{len(PLATINUM_SCHEMAS)} tables created")
    print("="*60)


def list_tables(glue_client) -> None:
    """List all tables in the Glue database."""
    try:
        response = glue_client.get_tables(DatabaseName=GLUE_DATABASE)
        tables = response.get('TableList', [])
        
        print("\n" + "="*60)
        print(f"  TABLES IN DATABASE: {GLUE_DATABASE}")
        print("="*60)
        
        if not tables:
            print("\n  No tables found.")
            return
        
        for table in tables:
            cols = table['StorageDescriptor']['Columns']
            print(f"\n  * {table['Name']}")
            print(f"    Location : {table['StorageDescriptor']['Location']}")
            print(f"    Columns  : {len(cols)}")
            print(f"    Cols list: {', '.join(c['Name'] for c in cols)}")
            
    except glue_client.exceptions.EntityNotFoundException:
        print(f"\n  Database '{GLUE_DATABASE}' does not exist.")
    except Exception as e:
        print(f"Error listing tables: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    all_choices = list(PLATINUM_SCHEMAS.keys()) + ["all"]
    
    parser = argparse.ArgumentParser(
        description="AWS Glue Data Catalog Manager - Platinum Layer"
    )
    parser.add_argument(
        "--action",
        type=str,
        default="create",
        choices=["create", "delete", "list"],
        help="Action to perform (default: create)"
    )
    parser.add_argument(
        "--table",
        type=str,
        default="all",
        choices=all_choices,
        help=f"Table name or 'all' (default: all)"
    )
    args = parser.parse_args()
    
    # Initialize Glue client
    glue_client = get_glue_client()
    
    if args.action == "list":
        list_tables(glue_client)
    elif args.action == "delete":
        if args.table == "all":
            delete_database(glue_client)
        else:
            delete_table(glue_client, args.table)
    elif args.action == "create":
        if args.table == "all":
            create_all_tables(glue_client)
        else:
            create_database(glue_client)
            create_table(glue_client, args.table)
