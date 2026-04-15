from config.settings import S3_BRONZE, S3_SILVER, S3_GOLD, S3_PLATINUM
import boto3

def write_delta_table(df, layer, table_name, mode="overwrite"):
    """
    Write DataFrame to S3 as Parquet (NOT Delta to avoid version accumulation).

    Pure Parquet ensures:
    - Each write completely overwrites S3
    - Athena reads fresh data only (no Delta version log)
    - No data accumulation on repeated runs
    """
    if layer == "bronze":
        path = f"{S3_BRONZE}/{table_name}"
    elif layer == "silver":
        path = f"{S3_SILVER}/{table_name}"
    elif layer == "gold":
        path = f"{S3_GOLD}/{table_name}"
    elif layer == "platinum":
        path = f"{S3_PLATINUM}/{table_name}"
    else:
        raise ValueError("Layer must be 'bronze', 'silver', 'gold', or 'platinum'")

    # STEP 1: Delete ALL S3 files (complete purge)
    if mode == "overwrite":
        try:
            parts = path.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key_prefix = parts[1] if len(parts) > 1 else ""

            print(f"[DELETE-START] Purging all files at {path}...")

            s3_client = boto3.client('s3')
            s3_resource = boto3.resource('s3')

            # Delete everything under this prefix
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=key_prefix)

            deleted_count = 0
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_resource.Object(bucket, obj['Key']).delete()
                        deleted_count += 1

            print(f"[DELETE-OK] Deleted {deleted_count} objects (all files + logs)")
        except Exception as e:
            print(f"[DELETE-WARN] Failed to purge: {e}")

    # STEP 2: Write fresh Parquet (NOT Delta - avoids version log)
    try:
        df.coalesce(1).write.format("parquet").mode(mode).save(path)
        print(f"[WRITE-OK] Saved {df.count():,} rows as pure Parquet")
    except Exception as e:
        print(f"[WRITE-ERROR] Failed to write: {e}")
        raise
