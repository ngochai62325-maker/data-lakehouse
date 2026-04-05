from etl_pipeline.config.settings import BRONZE_PATH, SILVER_PATH


def read_delta_table(spark, layer, table_name):
    if layer == "bronze":
        path = f"{BRONZE_PATH}/{table_name}"
    elif layer == "silver":
        path = f"{SILVER_PATH}/{table_name}"
    else:
        raise ValueError("Layer must be 'bronze' or 'silver'")

    return spark.read.format("delta").load(path)