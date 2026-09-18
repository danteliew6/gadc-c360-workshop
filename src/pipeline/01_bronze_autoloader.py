# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1 · Auto Loader ingestion from S3 → Bronze
# MAGIC
# MAGIC Ingests the four raw JSON feeds landed on the **S3 landing volume** into
# MAGIC Bronze Delta tables using **Auto Loader** (`cloudFiles`). This is a
# MAGIC standalone Structured Streaming job (deliberately *outside* the SDP pipeline)
# MAGIC so the Auto Loader mechanics are visible on their own:
# MAGIC
# MAGIC * **Incremental file discovery** — only new files since the last run are read
# MAGIC   (tracked in the checkpoint). Re-running after landing an `incremental`
# MAGIC   batch processes *only* the new files.
# MAGIC * **Schema inference + evolution** — `schemaLocation` persists the inferred
# MAGIC   schema; `schemaEvolutionMode=addNewColumns` adapts when new fields appear.
# MAGIC * **Rescued data** — malformed / unexpected fields are captured in
# MAGIC   `_rescued_data` instead of being silently dropped.
# MAGIC * **`Trigger.AvailableNow`** — process all available files then stop, which
# MAGIC   is the right trigger for scheduled batch-style ingestion (vs a always-on
# MAGIC   stream). Swap to `processingTime` for continuous ingestion.
# MAGIC
# MAGIC > The landing volume is an **S3 external location** — `/Volumes/.../landing`
# MAGIC > resolves to `s3://…/mcdo_ph/landing`. To point Auto Loader at the raw S3
# MAGIC > URI directly, pass `s3://…` in place of the volume path; the external
# MAGIC > location's storage credential brokers access (no keys in code).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "dante_classic_stable_catalog")
dbutils.widgets.text("bronze_schema", "mcdo_ph_bronze")

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
vol = f"/Volumes/{catalog}/{bronze}/landing"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{bronze}`")
print(f"landing volume = {vol}")

# COMMAND ----------

# MAGIC %md ### Reusable Auto Loader → Bronze helper

# COMMAND ----------


def ingest(source_dir: str, table: str, cluster_by: str | None = None):
    """Stream new JSON files from `vol/source_dir` into Bronze table `table`."""
    schema_loc = f"{vol}/_schemas/{table}"
    checkpoint = f"{vol}/_checkpoints/{table}"

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Directory listing mode is used by default; for high-volume buckets
        # enable file notifications: .option("cloudFiles.useNotifications","true")
        .option("cloudFiles.maxFilesPerTrigger", "200")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{vol}/{source_dir}")
    )

    enriched = (
        reader
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_batch", F.lit(source_dir))
    )

    writer = (
        enriched.writeStream
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(table)
    )
    writer.awaitTermination()

    cnt = spark.table(table).count()
    print(f"✓ {table}: {cnt:,} rows")
    if cluster_by:
        # Liquid clustering keeps analytical reads fast as the table grows.
        spark.sql(f"ALTER TABLE {table} CLUSTER BY ({cluster_by})")


# COMMAND ----------

# MAGIC %md ### Ingest the four raw feeds

# COMMAND ----------

ingest("customers", "customers_raw", cluster_by="region")
ingest("orders", "orders_raw", cluster_by="order_ts")
ingest("order_items", "order_items_raw")
ingest("app_events", "app_events_raw", cluster_by="event_ts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect ingestion metadata & rescued data
# MAGIC Anything Auto Loader couldn't fit the inferred schema lands in
# MAGIC `_rescued_data` — a running audit of source drift.

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT 'customers_raw' tbl, count(*) rows,
               count(_rescued_data) rescued, max(_ingest_ts) last_ingest
        FROM customers_raw
        UNION ALL SELECT 'orders_raw', count(*), count(_rescued_data), max(_ingest_ts) FROM orders_raw
        UNION ALL SELECT 'order_items_raw', count(*), count(_rescued_data), max(_ingest_ts) FROM order_items_raw
        UNION ALL SELECT 'app_events_raw', count(*), count(_rescued_data), max(_ingest_ts) FROM app_events_raw
        """
    )
)

# COMMAND ----------

dbutils.notebook.exit("Bronze ingestion complete")
