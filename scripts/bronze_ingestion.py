import argparse
import os
import sys
import requests
import pandas as pd
from pyspark.sql import SparkSession
from delta.tables import DeltaTable


# ============================================================
# CẤU HÌNH
# ============================================================
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "olist-lakehouse-2026")

S3_BRONZE_PATH = f"s3a://{S3_BUCKET_NAME}/bronze/"
LOCAL_DATA_DIR = "/opt/spark/dataset/"
INCREMENTAL_DATA_DIR = "/opt/spark/dataset/incremental/"

# Danh sách các bảng Olist
OLIST_TABLES = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation"
]

# Khóa chính cho mỗi bảng (dùng cho incremental load)
TABLE_KEYS = {
    "olist_customers_dataset": "customer_id",
    "olist_orders_dataset": "order_id",
    "olist_order_items_dataset": ["order_id", "order_item_id"],
    "olist_order_payments_dataset": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset": "review_id",
    "olist_products_dataset": "product_id",
    "olist_sellers_dataset": "seller_id",
    "olist_geolocation_dataset": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
    "product_category_name_translation": "product_category_name",
    "holidays_dataset": "date"
}


def create_spark_session():
    """Tạo Spark Session với cấu hình Delta Lake và S3"""
    spark = SparkSession.builder \
        .appName("Bronze_Layer_Ingestion") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print("=" * 60)
    print("SPARK SESSION INITIALIZED")
    print(f"S3 Bronze Path: {S3_BRONZE_PATH}")
    print("=" * 60)
    
    return spark


def full_load(spark):
    """
    Nạp toàn bộ dữ liệu lần đầu (Full Load)
    Ghi đè toàn bộ dữ liệu hiện có trên Bronze Layer
    """
    print("\n" + "=" * 60)
    print("FULL LOAD - Nạp toàn bộ dữ liệu Olist")
    print("=" * 60)
    
    results = {}
    success_count = 0
    error_count = 0
    
    for table_name in OLIST_TABLES:
        csv_path = f"{LOCAL_DATA_DIR}{table_name}.csv"
        s3_path = f"{S3_BRONZE_PATH}{table_name}"
        
        try:
            print(f"\n[{table_name}]")
            print(f"Đọc từ: {csv_path}")
            
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            row_count = df.count()
            
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(s3_path)
            
            results[table_name] = row_count
            success_count += 1
            print(f"Đã ghi {row_count:,} dòng lên: {s3_path}")
            
        except Exception as e:
            error_count += 1
            print(f"Lỗi: {e}")
    
    print("\n" + "=" * 60)
    print(f"KẾT QUẢ: {success_count} thành công, {error_count} lỗi")
    print("=" * 60)
    
    return results


def load_api_holidays(spark):
    """
    Kéo dữ liệu ngày lễ Brazil từ API Nager.Date
    """
    print("\n" + "=" * 60)
    print("API HOLIDAYS - Kéo dữ liệu ngày lễ Brazil")
    print("=" * 60)
    
    years_to_fetch = [2016, 2017, 2018]
    country_code = "BR"
    all_holidays = []
    
    for year in years_to_fetch:
        api_url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
        print(f"Đang gọi API năm {year}...")
        
        try:
            response = requests.get(api_url, timeout=30)
            if response.status_code == 200:
                all_holidays.extend(response.json())
                print(f"Thành công năm {year}")
            else:
                print(f"Lỗi HTTP {response.status_code}")
        except Exception as e:
            print(f"Lỗi: {e}")
    
    if not all_holidays:
        print("Không có dữ liệu để ghi!")
        return 0
    
    # Chuyển đổi sang Spark DataFrame
    pdf = pd.DataFrame(all_holidays).astype(str)
    df_holidays = spark.createDataFrame(pdf)
    
    # Ghi lên S3
    s3_path = f"{S3_BRONZE_PATH}holidays_dataset"
    df_holidays.write \
        .format("delta") \
        .mode("overwrite") \
        .save(s3_path)
    
    count = df_holidays.count()
    print(f"\nĐã ghi {count} ngày lễ lên: {s3_path}")
    
    return count


def incremental_load(spark):
    """
    Nạp dữ liệu gia tăng (Incremental Load) với MERGE
    Đọc file mới từ thư mục incremental và merge vào Delta Table
    """
    print("\n" + "=" * 60)
    print("INCREMENTAL LOAD - Nạp dữ liệu gia tăng")
    print("=" * 60)
    
    results = []
    
    for table_name, merge_key in TABLE_KEYS.items():
        new_data_file = f"{INCREMENTAL_DATA_DIR}{table_name}_new.csv"
        bronze_path = f"{S3_BRONZE_PATH}{table_name}"
        
        try:
            print(f"\n[{table_name}]")
            print(f"Kiểm tra: {new_data_file}")
            
            # Đọc dữ liệu mới
            df_new = spark.read.csv(new_data_file, header=True, inferSchema=True)
            new_count = df_new.count()
            
            if new_count == 0:
                print(f"Không có dữ liệu mới")
                continue
            
            print(f"Tìm thấy {new_count:,} bản ghi mới")
            
            # MERGE vào Delta Table
            if DeltaTable.isDeltaTable(spark, bronze_path):
                delta_table = DeltaTable.forPath(spark, bronze_path)
                
                # Xây dựng điều kiện merge
                if isinstance(merge_key, list):
                    condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_key])
                else:
                    condition = f"target.{merge_key} = source.{merge_key}"
                
                delta_table.alias("target").merge(
                    df_new.alias("source"),
                    condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                
                print(f"MERGE thành công")
            else:
                # Tạo mới nếu chưa tồn tại
                df_new.write.format("delta").mode("overwrite").save(bronze_path)
                print(f"Tạo mới Delta Table")
            
            # Đếm tổng
            total = spark.read.format("delta").load(bronze_path).count()
            results.append((table_name, new_count, total))
            print(f"Tổng số bản ghi: {total:,}")
            
        except Exception as e:
            if "Path does not exist" in str(e) or "Unable to infer schema" in str(e):
                print(f"Không tìm thấy file mới")
            else:
                print(f"Lỗi: {e}")
    
    # Tổng kết
    print("\n" + "=" * 60)
    print("KẾT QUẢ NẠP GIA TĂNG:")
    print("=" * 60)
    if results:
        for table, new_cnt, total in results:
            print(f"   {table}: +{new_cnt:,} → Tổng: {total:,}")
    else:
        print("Không có dữ liệu mới để nạp")
    
    return results


def validate_data(spark, full_load_counts=None):
    """
    Kiểm định dữ liệu đã nạp trên Bronze Layer
    """
    print("\n" + "=" * 60)
    print("DATA VALIDATION - Kiểm định dữ liệu Bronze Layer")
    print("=" * 60)
    
    all_tables = OLIST_TABLES + ["holidays_dataset"]
    validation_passed = True
    results = []
    
    for table_name in all_tables:
        s3_path = f"{S3_BRONZE_PATH}{table_name}"
        
        try:
            df = spark.read.format("delta").load(s3_path)
            count = df.count()
            
            # So sánh với source nếu có
            if full_load_counts and table_name in full_load_counts:
                source_count = full_load_counts[table_name]
                if count == source_count:
                    status = "PASS"
                else:
                    status = "FAIL"
                    validation_passed = False
            else:
                source_count = "N/A"
                status = "EXISTS"
            
            results.append({
                "table": table_name,
                "source": source_count,
                "bronze": count,
                "status": status
            })
            
        except Exception as e:
            results.append({
                "table": table_name,
                "source": "N/A",
                "bronze": "ERROR",
                "status": f"{str(e)[:30]}"
            })
            validation_passed = False
    
    # In kết quả
    print(f"\n{'Bảng':<40} {'Source':<12} {'Bronze':<12} {'Status'}")
    print("-" * 80)
    for r in results:
        print(f"{r['table']:<40} {str(r['source']):<12} {str(r['bronze']):<12} {r['status']}")
    print("-" * 80)
    
    if validation_passed:
        print("\nVALIDATION PASSED")
    else:
        print("\nVALIDATION FAILED")
    
    return validation_passed


def main():
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion Script")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["full_load", "incremental", "api_holidays", "validate", "all"],
        help="Chế độ chạy: full_load, incremental, api_holidays, validate, all"
    )
    
    args = parser.parse_args()
    
    # Tạo Spark Session
    spark = create_spark_session()
    
    try:
        if args.mode == "full_load":
            full_load(spark)
            
        elif args.mode == "incremental":
            incremental_load(spark)
            
        elif args.mode == "api_holidays":
            load_api_holidays(spark)
            
        elif args.mode == "validate":
            validate_data(spark)
            
        elif args.mode == "all":
            # Chạy full pipeline
            print("\n" + "=" * 60)
            print("RUNNING FULL PIPELINE")
            print("=" * 60)
            
            counts = full_load(spark)
            holidays_count = load_api_holidays(spark)
            counts["holidays_dataset"] = holidays_count
            validate_data(spark, counts)
            
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()
