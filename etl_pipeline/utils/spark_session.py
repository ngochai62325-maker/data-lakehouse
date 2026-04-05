from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from etl_pipeline.config.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY


def get_spark_session(app_name="LakehouseApp"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()