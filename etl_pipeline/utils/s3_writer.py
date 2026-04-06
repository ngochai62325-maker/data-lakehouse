from etl_pipeline.config.settings import SILVER_PATH, GOLD_PATH

def write_delta_table(df, layer, table_name, mode="overwrite"):
    if layer == "silver":
        path = f"{SILVER_PATH}/{table_name}"
    elif layer == "gold":
        path = f"{GOLD_PATH}/{table_name}"
    else:
        raise ValueError("Layer must be 'silver' or 'gold'")

    # Initialize the writer
    writer = df.write.format("delta").mode(mode)
    
    # If mode is overwrite, force Delta to overwrite the schema too
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
        
    # Execute the save
    writer.save(path)

    print(f"Saved successfully to {path}")