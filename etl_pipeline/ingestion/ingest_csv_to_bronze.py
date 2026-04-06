import argparse
import sys

sys.path.insert(0, "/opt/spark/etl_pipeline")
sys.path.insert(0, "/opt/spark")

from config.settings import S3_BRONZE, OLIST_TABLES
from etl_pipeline.utils.spark_session import get_spark_session
from delta.tables import DeltaTable


LOCAL_DATA_DIR = "/opt/spark/dataset/"
INCREMENTAL_DATA_DIR = "/opt/spark/dataset/incremental/"

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


def full_load(spark):
    """
    Nạp toàn bộ dữ liệu lần đầu (Full Load)
    Ghi đè toàn bộ dữ liệu hiện có trên Bronze Layer
    """
    print("\n" + "=" * 60)
    print("FULL LOAD - Nạp toàn bộ dữ liệu Olist CSV")
    print("=" * 60)
    
    results = {}
    success_count = 0
    error_count = 0
    
    for table_name in OLIST_TABLES:
        csv_path = f"{LOCAL_DATA_DIR}{table_name}.csv"
        s3_path = f"{S3_BRONZE}/{table_name}"
        
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


def incremental_load(spark):
    """
    Nạp dữ liệu gia tăng (Incremental Load) với MERGE
    Đọc file mới từ thư mục incremental và merge vào Delta Table
    """
    print("\n" + "=" * 60)
    print("INCREMENTAL LOAD")
    print("=" * 60)
    
    results = []
    
    for table_name, merge_key in TABLE_KEYS.items():
        new_data_file = f"{INCREMENTAL_DATA_DIR}{table_name}_new.csv"
        bronze_path = f"{S3_BRONZE}/{table_name}"
        
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


def main():
    parser = argparse.ArgumentParser(description="Bronze Layer CSV Ingestion")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["full_load", "incremental"],
        help="Chế độ chạy: full_load hoặc incremental"
    )
    
    args = parser.parse_args()
    
    # Tạo Spark Session từ utils
    spark = get_spark_session("Bronze_CSV_Ingestion")
    
    print("=" * 60)
    print("SPARK SESSION INITIALIZED")
    print(f"S3 Bronze Path: {S3_BRONZE}")
    print("=" * 60)
    
    try:
        if args.mode == "full_load":
            full_load(spark)
        elif args.mode == "incremental":
            incremental_load(spark)
            
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()
