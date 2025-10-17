@dlt.table(
  name="silver_cleaned",
  comment="Cleaned and validated data"
)
@dlt.expect("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
def silver_cleaned():
    df = dlt.read("bronze_raw")
    return df.dropDuplicates(["id"])
