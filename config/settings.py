import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# Lakehouse paths
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver"
GOLD_PATH = f"s3a://{S3_BUCKET}/gold"
