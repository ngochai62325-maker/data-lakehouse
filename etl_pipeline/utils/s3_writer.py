from config.settings import S3_BRONZE, S3_SILVER, S3_GOLD, S3_PLATINUM
from delta import DeltaTable
import boto3

def write_delta_table(df, layer, table_name, mode="overwrite"):
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

    # If mode is overwrite, delete S3 path first to ensure clean slate
    if mode == "overwrite":
        try:
            # Parse S3 path: s3://bucket/key/path
            parts = path.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key_prefix = parts[1] if len(parts) > 1 else ""

            # Delete all objects under this prefix
            s3 = boto3.resource('s3')
            s3_bucket = s3.Bucket(bucket)
            s3_bucket.objects.filter(Prefix=key_prefix).delete()
            print(f"[CLEAN] Deleted S3 path: {path}")
        except Exception as e:
            print(f"[WARN] Failed to clean S3 {path}: {e}")

    # Initialize the writer
    writer = df.write.format("delta").mode(mode)

    # If mode is overwrite, force Delta to overwrite the schema too
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")

    # Execute the save
    writer.save(path)

    print(f"[OK] Saved successfully to {path}")

    # Extra cleanup: VACUUM for any remaining orphaned files
    if mode == "overwrite":
        try:
            delta_table = DeltaTable.forPath(df.sparkSession, path)
            delta_table.vacuum(0)
            print(f"[VACUUM] Cleaned up orphaned files from {path}")
        except Exception as e:
            print(f"[VACUUM-WARN] VACUUM failed (non-critical): {e}")