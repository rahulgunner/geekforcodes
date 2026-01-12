import dlt
from pyspark.sql.functions import *


@dlt.table(name="api_silver")
def api_silver():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/practice/api/api_data")
        .withColumn("ingest_ts", current_timestamp())
    )

dlt.create_streaming_table(name="api_gold")
dlt.apply_changes(
    target="api_gold",
    source="api_silver",
    keys=["id"],
    sequence_by="ingest_ts",
    stored_as_scd_type=2
)