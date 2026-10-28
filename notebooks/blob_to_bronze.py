# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Blob to Bronze: Transaction Data Ingestion
# MAGIC
# MAGIC This notebook uses Auto Loader to read CSV files from the blob source volume and writes them into the `retail_s.blob_bronze.transactions` Delta table.

# COMMAND ----------

# DBTITLE 1,Auto Loader: CSV to Bronze Delta Table
from pyspark.sql.functions import col

# Source and target configuration
source_path = "/Volumes/retail_s/volume/blob_source/transaction_source/"
schema_location = "/Volumes/retail_s/volume/blob_source/transaction_source/_schema"
checkpoint_location = "/Volumes/retail_s/volume/blob_source/transaction_source/_checkpoints/transactions"
target_table = "retail_s.blob_bronze.transactions"

# Read CSV files using Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(source_path)
    .withColumn("source_file", col("_metadata.file_name"))
)

# Write to Delta table using AvailableNow trigger for batch-like processing
query = (df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
print(f"Data loaded successfully into {target_table}")

# COMMAND ----------

# DBTITLE 1,Verify Loaded Data
# Verify the loaded data
print(f"Row count in {target_table}:")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {target_table}").show()

print("\nSample rows:")
display(spark.sql(f"SELECT * FROM {target_table} LIMIT 5"))