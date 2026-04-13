import os
from datetime import datetime, timedelta


#  1. DEFAULT ARGS — Tham số mặc định cho mọi DAG
# Template cơ sở — dùng hàm get_default_args() để tuỳ chỉnh cho từng DAG
_BASE_DEFAULT_ARGS = {
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_default_args(owner: str, **overrides) -> dict:
    """
    Trả về default_args cho DAG, kế thừa từ _BASE_DEFAULT_ARGS.

    Params:
        owner    : Tên / mã thành viên phụ trách (vd: "thanhvien1")
        overrides: Các tham số muốn ghi đè hoặc bổ sung
                   (vd: retries=3, retry_delay=timedelta(minutes=10))
    """
    args = {**_BASE_DEFAULT_ARGS, "owner": owner}
    args.update(overrides)
    return args


#  2. SPARK — Cấu hình Spark Submit chung
# Connection ID của Spark trong Airflow
SPARK_CONN_ID = "spark_default"

# Maven packages cho Delta Lake & S3 — tải tự động khi spark-submit chạy
SPARK_PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.3.2",
    "io.delta:delta-storage:3.3.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

# Spark configs cho Delta Lake + S3
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

# Biến môi trường cho Spark workers
SPARK_ENV_VARS = {
    "PYTHONPATH": "/opt/airflow:/opt/airflow/etl_pipeline",
}


#  3. ETL SCRIPTS — Đường dẫn các script ETL (trong container Airflow)
ETL_BASE_PATH = "/opt/airflow/etl_pipeline"

ETL_SCRIPTS = {
    # ── Bronze Layer (TV1) ──
    "ingest_csv_to_bronze":  f"{ETL_BASE_PATH}/ingestion/ingest_csv_to_bronze.py",
    "validate_bronze":       f"{ETL_BASE_PATH}/ingestion/validate_bronze.py",

    # ── Silver Layer (TV2) ── Thêm đường dẫn khi có script
    # "transform_silver":    f"{ETL_BASE_PATH}/transformation/transform_silver.py",
    # "validate_silver":     f"{ETL_BASE_PATH}/transformation/validate_silver.py",

    # ── Gold Layer (TV3) ── Thêm đường dẫn khi có script
    "aggregate_gold":      f"{ETL_BASE_PATH}/processing/silver_to_gold.py",
    #"validate_gold":       f"{ETL_BASE_PATH}/aggregation/validate_gold.py",

    # ── Platinum Layer (TV4) ──
    "gold_to_platinum":     f"{ETL_BASE_PATH}/processing/gold_to_platinum.py",
}


#  4. DAG METADATA — Thông tin & tags cho từng layer
# Tags gợi ý cho từng layer — dùng trong tham số `tags` của DAG
DAG_TAGS = {
    "bronze":   ["bronze", "ingestion", "thanhvien1"],
    "silver":   ["silver", "transformation", "thanhvien2"],
    "gold":     ["gold", "aggregation", "thanhvien3"],
    "platinum": ["platinum", "bi", "thanhvien4"],
    "master":   ["master", "orchestration", "pipeline"],
}

# DAG IDs — dùng trong master DAG để trigger các DAG con
DAG_IDS = {
    "bronze":   "bronze_ingestion",
    "silver":   "silver_transformation",
    "gold":     "gold_aggregation",
    "platinum": "platinum_bi",
    "master":   "master_pipeline",
}

# Lịch chạy mặc định cho từng layer (Giờ UTC - Giờ VN trừ đi 7 tiếng)
DAG_SCHEDULES = {
    "bronze":   None,           # Thay vì chạy theo lịch, sẽ chờ Master gọi
    "silver":   None,           # Thay vì chạy theo lịch, sẽ chờ Master gọi
    "gold":     None,           # Thay vì chạy theo lịch, sẽ chờ Master gọi
    "platinum": None,           # Thay vì chạy theo lịch, sẽ chờ Master gọi
    "master":   "0 17 * * *",   # 17:00 UTC (00:00 Giờ VN) hàng ngày (trigger luồng pipeline chính)
}


#  5. HELPER FUNCTIONS — Hàm tiện ích dùng chung
def create_spark_submit_kwargs(
    application: str,
    app_name: str,
    application_args: list = None,
    extra_conf: dict = None,
    extra_env_vars: dict = None,
    **overrides,
) -> dict:
    """
    Tạo dict kwargs cho SparkSubmitOperator, kế thừa cấu hình chung.
    """
    conf = {**SPARK_CONF}
    if extra_conf:
        conf.update(extra_conf)
    
    # Isolate Ivy caches by app_name to prevent race conditions during parallel execution
    conf["spark.jars.ivy"] = f"/tmp/.ivy_{app_name}"

    env_vars = {**SPARK_ENV_VARS}
    if extra_env_vars:
        env_vars.update(extra_env_vars)

    kwargs = {
        "conn_id": SPARK_CONN_ID,
        "application": application,
        "name": app_name,
        "packages": SPARK_PACKAGES,
        "conf": conf,
        "env_vars": env_vars,
        "verbose": True,
    }

    if application_args:
        kwargs["application_args"] = application_args

    kwargs.update(overrides)
    return kwargs
