from pyspark.sql.functions import input_file_name

@dlt.table(
  name="bronze_raw",
  comment="Raw ingested data from source"
)
def bronze_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")  # or csv/parquet
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/raw-data/")
        .withColumn("source_file", input_file_name())
    )
