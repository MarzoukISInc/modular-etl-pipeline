@dlt.table(
  name="gold_summary",
  comment="Aggregated metrics for reporting"
)
def gold_summary():
    df = dlt.read("silver_cleaned")
    return df.groupBy("category").agg({"amount": "sum"})
