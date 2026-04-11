import sys

sys.path.insert(0, "/opt/spark/etl_pipeline")
sys.path.insert(0, "/opt/spark")

from config.settings import S3_BRONZE, OLIST_TABLES
from etl_pipeline.utils.spark_session import get_spark_session


# Danh sách tất cả các bảng Olist cần kiểm tra
ALL_TABLES = OLIST_TABLES


def validate_bronze(spark, expected_counts=None):
    """
    Kiểm định dữ liệu đã nạp trên Bronze Layer
    
    Args:
        spark: SparkSession
        expected_counts: Dict với số dòng mong đợi cho mỗi bảng (optional)
    
    Returns:
        bool: True nếu validation passed
    """
    print("\n" + "=" * 70)
    print("DATA VALIDATION - Kiểm định dữ liệu Bronze Layer")
    print("=" * 70)
    
    validation_passed = True
    results = []
    
    for table_name in ALL_TABLES:
        s3_path = f"{S3_BRONZE}/{table_name}"
        
        try:
            df = spark.read.format("delta").load(s3_path)
            count = df.count()
            
            # So sánh với expected nếu có
            if expected_counts and table_name in expected_counts:
                expected = expected_counts[table_name]
                if count == expected:
                    status = "PASS"
                else:
                    status = "FAIL"
                    validation_passed = False
            else:
                expected = "N/A"
                status = "EXISTS"
            
            results.append({
                "table": table_name,
                "expected": expected,
                "actual": count,
                "status": status
            })
            
        except Exception as e:
            results.append({
                "table": table_name,
                "expected": "N/A",
                "actual": "ERROR",
                "status": f"{str(e)[:30]}"
            })
            validation_passed = False
    
    # In kết quả dạng bảng
    print(f"\n{'Bảng':<40} {'Expected':<12} {'Actual':<12} {'Status'}")
    print("-" * 80)
    for r in results:
        print(f"{r['table']:<40} {str(r['expected']):<12} {str(r['actual']):<12} {r['status']}")
    print("-" * 80)
    
    if validation_passed:
        print("\nVALIDATION PASSED - Tất cả dữ liệu đã được nạp thành công!")
    else:
        print("\nVALIDATION FAILED - Có lỗi, vui lòng kiểm tra!")
    
    return validation_passed


def main():
    # Tạo Spark Session từ utils
    spark = get_spark_session("Bronze_Validation")
    
    print("=" * 60)
    print("SPARK SESSION INITIALIZED")
    print(f"S3 Bronze Path: {S3_BRONZE}")
    print("=" * 60)
    
    try:
        passed = validate_bronze(spark)
        sys.exit(0 if passed else 1)
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()
