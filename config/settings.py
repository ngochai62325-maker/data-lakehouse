import os
from dotenv import load_dotenv

load_dotenv()

# AWS
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET_NAME")

# S3 paths
S3_BASE   = f"s3a://{S3_BUCKET}"
S3_BRONZE = f"{S3_BASE}/bronze"
S3_SILVER = f"{S3_BASE}/silver"
S3_GOLD   = f"{S3_BASE}/gold"
S3_PLATINUM = f"{S3_BASE}/platinum"
S3_ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results"

# Danh sách bảng Olist
OLIST_TABLES = [
    "olist_orders_dataset",
    "olist_customers_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_geolocation_dataset",           
    "product_category_name_translation",
]