import sys
import requests
import pandas as pd

sys.path.insert(0, "/opt/spark/etl_pipeline")
sys.path.insert(0, "/opt/spark")

from config.settings import S3_BRONZE
from etl_pipeline.utils.spark_session import get_spark_session


# CẤU HÌNH API
API_BASE_URL = "https://date.nager.at/api/v3/PublicHolidays"
COUNTRY_CODE = "BR"  # Brazil
YEARS_TO_FETCH = [2016, 2017, 2018]


def load_api_holidays(spark):
    """
    Kéo dữ liệu ngày lễ Brazil từ API Nager.Date
    """
    print("\n" + "=" * 60)
    print("API HOLIDAYS - Kéo dữ liệu ngày lễ Brazil")
    print("=" * 60)
    
    all_holidays = []
    
    for year in YEARS_TO_FETCH:
        api_url = f"{API_BASE_URL}/{year}/{COUNTRY_CODE}"
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
    s3_path = f"{S3_BRONZE}/holidays_dataset"
    df_holidays.write \
        .format("delta") \
        .mode("overwrite") \
        .save(s3_path)
    
    count = df_holidays.count()
    print(f"\nĐã ghi {count} ngày lễ lên: {s3_path}")
    
    return count


def main():
    # Tạo Spark Session từ utils
    spark = get_spark_session("Bronze_API_Ingestion")
    
    print("=" * 60)
    print("SPARK SESSION INITIALIZED")
    print(f"S3 Bronze Path: {S3_BRONZE}")
    print("=" * 60)
    
    try:
        load_api_holidays(spark)
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()
