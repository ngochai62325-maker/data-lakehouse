"""
AWS Glue Data Catalog - Table Creation Script
==============================================

This module creates Glue tables for the Platinum layer to enable
querying via Amazon Athena and connecting to Power BI.

Usage:
    python -m etl_pipeline.utils.glue_catalog --table all
    python -m etl_pipeline.utils.glue_catalog --table mart_sales
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

# Schema definitions for Platinum tables
PLATINUM_SCHEMAS: Dict[str, List[Dict[str, str]]] = {
    "mart_sales": [
        {"Name": "order_id", "Type": "string"},
        {"Name": "customer_id", "Type": "string"},
        {"Name": "customer_city", "Type": "string"},
        {"Name": "customer_state", "Type": "string"},
        {"Name": "product_id", "Type": "string"},
        {"Name": "product_category_name_english", "Type": "string"},
        {"Name": "seller_id", "Type": "string"},
        {"Name": "seller_state", "Type": "string"},
        {"Name": "date_key", "Type": "int"},
        {"Name": "year", "Type": "int"},
        {"Name": "month", "Type": "int"},
        {"Name": "order_status", "Type": "string"},
        {"Name": "payment_type", "Type": "string"},
        {"Name": "price", "Type": "double"},
        {"Name": "freight_value", "Type": "double"},
        {"Name": "payment_value", "Type": "double"},
    ],
    "mart_kpi_daily": [
        {"Name": "date_key", "Type": "int"},
        {"Name": "year", "Type": "int"},
        {"Name": "month", "Type": "int"},
        {"Name": "total_orders", "Type": "bigint"},
        {"Name": "total_revenue", "Type": "double"},
        {"Name": "total_items", "Type": "bigint"},
        {"Name": "avg_order_value", "Type": "double"},
    ],
    "mart_top_products": [
        {"Name": "product_id", "Type": "string"},
        {"Name": "product_category_name_english", "Type": "string"},
        {"Name": "total_sales", "Type": "double"},
        {"Name": "total_items", "Type": "bigint"},
    ],
    "mart_customer_analysis": [
        {"Name": "customer_id", "Type": "string"},
        {"Name": "customer_state", "Type": "string"},
        {"Name": "total_orders", "Type": "bigint"},
        {"Name": "total_spent", "Type": "double"},
        {"Name": "avg_order_value", "Type": "double"},
    ],
    "mart_seller_performance": [
        {"Name": "seller_id", "Type": "string"},
        {"Name": "seller_state", "Type": "string"},
        {"Name": "total_orders", "Type": "bigint"},
        {"Name": "total_revenue", "Type": "double"},
        {"Name": "avg_review_score", "Type": "double"},
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
                'Description': 'Olist Lakehouse - Data Mart for BI/Analytics',
                'LocationUri': S3_PLATINUM_PATH,
            }
        )
        print(f"✓ Database '{GLUE_DATABASE}' created successfully.")
        return True
    except Exception as e:
        print(f"✗ Error creating database: {e}")
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
        print(f"✓ Database '{GLUE_DATABASE}' deleted successfully.")
        return True
    except Exception as e:
        print(f"✗ Error deleting database: {e}")
        return False


# =============================================================================
# TABLE OPERATIONS
# =============================================================================

def create_table(glue_client, table_name: str) -> bool:
    """Create a single Glue table."""
    if table_name not in PLATINUM_SCHEMAS:
        print(f"✗ Unknown table: {table_name}")
        return False
    
    schema = PLATINUM_SCHEMAS[table_name]
    location = f"{S3_PLATINUM_PATH}/{table_name}/"
    
    # Check if table exists
    try:
        glue_client.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        print(f"Table '{table_name}' already exists. Updating...")
        return update_table(glue_client, table_name, schema, location)
    except glue_client.exceptions.EntityNotFoundException:
        pass
    
    try:
        glue_client.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput={
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
        )
        print(f"✓ Table '{table_name}' created successfully.")
        print(f"  Location: {location}")
        return True
    except Exception as e:
        print(f"✗ Error creating table '{table_name}': {e}")
        return False


def update_table(glue_client, table_name: str, schema: List[Dict], location: str) -> bool:
    """Update existing Glue table."""
    try:
        glue_client.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput={
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
        )
        print(f"✓ Table '{table_name}' updated successfully.")
        return True
    except Exception as e:
        print(f"✗ Error updating table '{table_name}': {e}")
        return False


def delete_table(glue_client, table_name: str) -> bool:
    """Delete a single Glue table."""
    try:
        glue_client.delete_table(DatabaseName=GLUE_DATABASE, Name=table_name)
        print(f"✓ Table '{table_name}' deleted successfully.")
        return True
    except Exception as e:
        print(f"✗ Error deleting table '{table_name}': {e}")
        return False


def create_all_tables(glue_client) -> None:
    """Create all Platinum layer tables."""
    print("\n" + "="*60)
    print("CREATING ALL PLATINUM TABLES IN GLUE")
    print("="*60 + "\n")
    
    # Ensure database exists
    create_database(glue_client)
    
    # Create each table
    success_count = 0
    for table_name in PLATINUM_SCHEMAS.keys():
        if create_table(glue_client, table_name):
            success_count += 1
    
    print("\n" + "="*60)
    print(f"COMPLETED: {success_count}/{len(PLATINUM_SCHEMAS)} tables created")
    print("="*60)


def list_tables(glue_client) -> None:
    """List all tables in the database."""
    try:
        response = glue_client.get_tables(DatabaseName=GLUE_DATABASE)
        tables = response.get('TableList', [])
        
        print("\n" + "="*60)
        print(f"TABLES IN DATABASE: {GLUE_DATABASE}")
        print("="*60)
        
        if not tables:
            print("No tables found.")
            return
        
        for table in tables:
            print(f"\n• {table['Name']}")
            print(f"  Location: {table['StorageDescriptor']['Location']}")
            print(f"  Columns: {len(table['StorageDescriptor']['Columns'])}")
            
    except Exception as e:
        print(f"Error listing tables: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AWS Glue Data Catalog Manager")
    parser.add_argument(
        "--action",
        type=str,
        default="create",
        choices=["create", "delete", "list"],
        help="Action to perform"
    )
    parser.add_argument(
        "--table",
        type=str,
        default="all",
        choices=list(PLATINUM_SCHEMAS.keys()) + ["all"],
        help="Table name or 'all'"
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
