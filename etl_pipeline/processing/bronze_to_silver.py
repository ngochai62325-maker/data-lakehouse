import sys
import os
import argparse

# nạp các mô-đun thiết lập
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))

if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# Nạp các hàm hỗ trợ cơ bản từ thư viện PySpark
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace, lpad, lower, trim

# Nạp các hàm tiện ích nội bộ để trích xuất và cấp phát dữ liệu S3
from etl_pipeline.utils.spark_session import get_spark_session
from etl_pipeline.utils.s3_reader import read_delta_table
from etl_pipeline.utils.s3_writer import write_delta_table


def transform_orders(spark):
    """
    Chuẩn hóa định dạng bảng dữ liệu Orders.

    Args:
        spark (SparkSession): Hệ thống phiên làm việc Spark hiện hành.
    
    Action:
        Tiếp nhận dữ liệu, thực hiện ép kiểu thời gian cho các tiêu chí giao hàng. Các bản ghi chưa hoàn thành sẽ duy trì trạng thái Null.
        Ghi đè kết quả cấu trúc xuống tầng Silver.
    """
    print("Transforming Orders...")
    df = read_delta_table(spark, "bronze", "olist_orders_dataset")
    
    # Chuyển đổi các trường mốc sự kiện sang định dạng Timestamp chuẩn
    # không loại bỏ giá trị Null nhằm bảo tồn dữ liệu của các đơn hàng chưa khép kín quy trình (chưa giao hàng, bị hủy).
    timestamp_cols = [
        "order_purchase_timestamp", "order_approved_at", 
        "order_delivered_carrier_date", "order_delivered_customer_date", 
        "order_estimated_delivery_date"
    ]
    df_cleaned = df
    for c in timestamp_cols:
        df_cleaned = df_cleaned.withColumn(c, to_timestamp(col(c)))
        
    write_delta_table(df_cleaned, "silver", "silver_olist_orders_dataset", mode="overwrite")


def transform_order_items(spark):
    """
    Chuẩn hóa cấu trúc bảng dữ liệu Order Items.

    Args:
        spark (SparkSession): Hệ thống phiên làm việc Spark hiện hành.
    
    Action:
        Ép kiểu logic số học và thời gian, sau đó ghi đè xuống tầng Silver.
    """
    print("Transforming Order Items...")
    df = read_delta_table(spark, "bronze", "olist_order_items_dataset")
    
    # Thiết lập kiểu số thực (Double) cho các trường tài chính
    # Chuẩn hóa trường shipping_limit_date sang định dạng Timestamp.
    df_cleaned = df.withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))) \
                   .withColumn("price", col("price").cast("double")) \
                   .withColumn("freight_value", col("freight_value").cast("double"))
                   
    write_delta_table(df_cleaned, "silver", "silver_olist_order_items_dataset", mode="overwrite")


def transform_customers(spark):
    """
    Chuẩn hóa bộ dữ liệu Customers.
    Dữ liệu được làm sạch các trường địa lý, sau đó ghi đè kết cấu chuẩn xuống tầng Silver.
    """
    print("Transforming Customers...")
    df = read_delta_table(spark, "bronze", "olist_customers_dataset")
    
    # Bổ sung các số không (0) ở đầu cho mã bưu điện nhằm duy trì tính thống nhất 5 ký tự lưu trữ toàn cục.
    # Cắt bỏ khoảng trắng thừa và chuẩn hóa văn bản chữ thường
    df_cleaned = df.withColumn("customer_zip_code_prefix", lpad(col("customer_zip_code_prefix").cast("string"), 5, "0")) \
                   .withColumn("customer_city", lower(trim(col("customer_city")))) \
                   .withColumn("customer_state", lower(trim(col("customer_state"))))
                   
    write_delta_table(df_cleaned, "silver", "silver_olist_customers_dataset", mode="overwrite")


def transform_geolocation(spark):
    """
    Chuẩn hóa danh mục địa lý Geolocation.
    Khởi tạo hệ thống dữ liệu thành một bảng tra cứu chuẩn (Lookup table) bằng cơ chế loại trừ trùng lặp khối lượng lớn.
    """
    print("Transforming Geolocation...")
    df = read_delta_table(spark, "bronze", "olist_geolocation_dataset")
    
    # Bổ sung số 0 cho mã bưu điện.
    # Chuẩn hóa văn bản thành chữ thường và loại bỏ khoảng trắng thừa.
    df_cleaned = df.dropDuplicates(["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])
    df_cleaned = df_cleaned.withColumn(
        "geolocation_zip_code_prefix", 
        lpad(col("geolocation_zip_code_prefix").cast("string"), 5, "0")
    )
    df_cleaned = (
        df_cleaned
        .withColumn("geolocation_city",  trim(lower(col("geolocation_city"))))
        .withColumn("geolocation_state", trim(lower(col("geolocation_state"))))
    )
    
    # Thiết lập nền tảng bảng tra cứu bằng việc ép buộc tính độc bản của mã bưu điện.
    # Xoá bỏ các bản ghi trùng lặp dựa trên mã bưu điện, vĩ độ và kinh độ
    df_cleaned = df_cleaned.dropDuplicates(["geolocation_zip_code_prefix"])

    write_delta_table(df_cleaned, "silver", "silver_olist_geolocation_dataset", mode="overwrite")


def transform_order_payments(spark):
    """
    Chuẩn hóa bộ dữ liệu Order Payments.
    Dữ liệu được lọc lỗi cấu trúc tham chiếu và chuẩn hóa phân loại, ghi đè xuống tầng Silver.
    """
    print("Transforming Order Payments...")
    df = read_delta_table(spark, "bronze", "olist_order_payments_dataset")

    # Quy chuẩn hóa khái niệm hạng mục thanh toán để thống nhất chuẩn ngữ nghĩa nội bộ.
    payment_mapping = {
        "credit_card": "credit card",
        "debit_card": "debit card",
        "not_defined": "not defined"
    }
    df_cleaned = df.replace(payment_mapping, subset=["payment_type"])

    # Chỉ duy trì các trạng thái thanh toán hiện diện trong cơ sở dữ liệu hóa đơn hợp lệ.
    # Mô hình Left Semi Join xử lý hiện tượng hóa đơn lưu vong (Orphaned records), bảo lưu cấu trúc toàn vẹn tham chiếu.
    df_orders = read_delta_table(spark, "silver", "silver_olist_orders_dataset").select("order_id")
    df_cleaned = df_cleaned.join(df_orders, on="order_id", how="left_semi")

    write_delta_table(df_cleaned, "silver", "silver_olist_order_payments_dataset", mode="overwrite")


def transform_order_reviews(spark):
    """
    Chuẩn hóa cấu trúc kho dữ liệu Order Reviews.
    Dữ liệu được khắc phục khuyết thiếu nội tại, định dạng toàn vẹn dữ liệu định danh và tiến hành ghi nền tảng xuống.
    """
    print("Transforming Order Reviews...")
    df = read_delta_table(spark, "bronze", "olist_order_reviews_dataset")

    df_cleaned = (
        df
        # Loại bỏ các hồ sơ đánh giá tổn thất định danh lõi, đảm bảo quy chuẩn bắt buộc của siêu liên kết nối.
        .dropna(subset=["review_id", "order_id"])
        # Kiểm tra và loại bỏ các bản ghi trùng lặp để kiến tạo mã khóa chính (Primary Key) độc bản tuyệt đối.
        .dropDuplicates(["review_id"])
        # Chuyển sang dữ liệu thời gian về Timestamp.
        .withColumn("review_creation_date", to_timestamp(col("review_creation_date")))
        .withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))
        .withColumn("review_score", col("review_score").cast("int"))
        # Tiến hành phân loại ngoại lệ (Outlier Filter)
        .filter(col("review_score").isNull() | col("review_score").between(1, 5))
        # Áp đặt giá trị mặc định cho các khoảng trống nội dung nhằm chặn đứng các rủi ro sập luồng (NullPointerException)
        .fillna({"review_comment_title": "No Title", "review_comment_message": "No Message"})
    )

    # Tối ưu hóa vùng tài nguyên cấp phát (Shuffle) bằng cơ chế giới hạn Select để lấy chính trường khóa order_id.
    df_orders = read_delta_table(spark, "silver", "silver_olist_orders_dataset").select("order_id")
    df_cleaned = df_cleaned.join(df_orders, on="order_id", how="left_semi")

    write_delta_table(df_cleaned, "silver", "silver_olist_order_reviews_dataset", mode="overwrite")


def transform_products(spark):
    """
    Tiêu chuẩn hóa dữ kiện phân cấp mặt hàng Products.
    Tăng cường cấu trúc dữ liệu theo hình thức nối chéo thông tin từ điển để đẩy lên Silver.
    """
    print("Transforming Products...")
    df = read_delta_table(spark, "bronze", "olist_products_dataset")

    # Điền 'unknown' for missing product category names.
    # Khôi phục bố cục khoảng trắng cho siêu cấu trúc phân loại.
    df_cleaned = (
        df.fillna({"product_category_name": "unknown"})
        .withColumn("product_category_name", regexp_replace(col("product_category_name"), "_", " "))
    )

    # Khai thác trực tiếp kho từ vựng đã được làm sạch tại cấu trúc Silver trước đó
    df_translation = read_delta_table(spark, "silver", "silver_product_category_name_translation")

    # Giảm thiểu gánh nặng vật lý cho tiến trình BI tại các tầng cao hơn
    df_cleaned = df_cleaned.join(df_translation, on="product_category_name", how="left")
    df_cleaned = df_cleaned.fillna({"product_category_name_english": "unknown"})

    write_delta_table(df_cleaned, "silver", "silver_olist_products_dataset", mode="overwrite")


def transform_sellers(spark):
    """
    Chuẩn hóa bộ dữ liệu phân bổ định danh Sellers.
    Dữ liệu được nhất quán hóa định dạng text và quy đúc chiều dài mã zip, sau đó ghi đè tầng Silver.
    """
    print("Transforming Sellers...")
    df = read_delta_table(spark, "bronze", "olist_sellers_dataset")
    
    # Bổ sung số 0 (zero-padding) cho mã bưu điện.
    # Chuẩn hóa văn bản thành chữ thường và loại bỏ khoảng trắng thừa.
    df_cleaned = df.withColumn("seller_zip_code_prefix", lpad(col("seller_zip_code_prefix").cast("string"), 5, "0")) \
                   .withColumn("seller_city", lower(trim(col("seller_city")))) \
                   .withColumn("seller_state", lower(trim(col("seller_state"))))
                   
    write_delta_table(df_cleaned, "silver", "silver_olist_sellers_dataset", mode="overwrite")


def transform_category_translation(spark):
    """
    Tinh giản kho từ vựng Product Category Name Translation.
    Dữ liệu tự điển được thay thế ký tự lỗi để cấp quyền lưu trữ tham chiếu.
    """
    print("Transforming Category Translations...")
    df = read_delta_table(spark, "bronze", "product_category_name_translation")
    
    # Tiến hành thanh lọc toàn bộ ký tự gạch dưới thành khoảng trắng nhằm thiết lập ngôn ngữ chuẩn ở các chiều phân tích hạ tầng hệ thống.
    columns_to_clean = ["product_category_name", "product_category_name_english"]
    df_cleaned = df
    for c in columns_to_clean:
        df_cleaned = df_cleaned.withColumn(c, regexp_replace(col(c), "_", " "))
        
    write_delta_table(df_cleaned, "silver", "silver_product_category_name_translation", mode="overwrite")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Silver Layer Transformations")
    parser.add_argument("--table", type=str, required=True, 
                        help="Name of the table to transform (e.g., 'orders', 'products', 'all')")
    args = parser.parse_args()

    spark = get_spark_session(app_name=f"SilverLayer-{args.table.capitalize()}")

    if args.table == "orders":
        transform_orders(spark)
    elif args.table == "order_items":
        transform_order_items(spark)
    elif args.table == "customers":
        transform_customers(spark)
    elif args.table == "geolocation":
        transform_geolocation(spark)
    elif args.table == "order_payments":
        transform_order_payments(spark)
    elif args.table == "order_reviews":
        transform_order_reviews(spark)
    elif args.table == "products":
        transform_products(spark)
    elif args.table == "sellers":
        transform_sellers(spark)
    elif args.table == "translation":
        transform_category_translation(spark)
    elif args.table == "all":
        # Khởi chạy quy trình theo tuần tự
        transform_orders(spark)
        transform_order_items(spark)
        transform_customers(spark)
        transform_geolocation(spark)
        transform_order_payments(spark)
        transform_order_reviews(spark)
        transform_category_translation(spark)
        transform_products(spark)
        transform_sellers(spark)
    else:
        print(f"Unknown table parameter: {args.table}")

    spark.stop()