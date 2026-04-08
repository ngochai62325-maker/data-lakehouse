from config.settings import S3_BRONZE, S3_SILVER, S3_GOLD, S3_PLATINUM

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

    # Initialize the writer
    writer = df.write.format("delta").mode(mode)
    
    # If mode is overwrite, force Delta to overwrite the schema too
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
        
    # Execute the save
    writer.save(path)

    print(f"Saved successfully to {path}")