from config.settings import S3_BRONZE, S3_SILVER, S3_GOLD

def read_delta_table(spark, layer, table_name):
    if layer == "bronze":
        path = f"{S3_BRONZE}/{table_name}"
    elif layer == "silver":
        path = f"{S3_SILVER}/{table_name}"
    elif layer == "gold":
        path = f"{S3_GOLD}/{table_name}"
    else:
        raise ValueError("Layer must be 'bronze', 'silver', or 'gold'")

    return spark.read.format("delta").load(path)