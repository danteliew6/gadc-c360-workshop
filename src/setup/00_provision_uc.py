# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Provision Unity Catalog for the C360 Workshop
# MAGIC
# MAGIC White-labeled for **McDonald's Philippines (GADC)**.
# MAGIC
# MAGIC Creates the medallion schemas and the **S3-backed external volume** that
# MAGIC acts as the Auto Loader landing zone. Everything is idempotent — safe to
# MAGIC re-run.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `{catalog}.{bronze}` | Raw ingested tables (Module 1 — Auto Loader) |
# MAGIC | `{catalog}.{silver}` | Cleaned + quality-checked (Module 2 — SDP) |
# MAGIC | `{catalog}.{gold}`   | Customer 360 marts (Module 2 — SDP) |
# MAGIC | `{bronze}.landing` (EXTERNAL VOLUME) | S3 bucket where raw files land |

# COMMAND ----------

dbutils.widgets.text("catalog", "dante_classic_stable_catalog")
dbutils.widgets.text("bronze_schema", "mcdo_ph_bronze")
dbutils.widgets.text("silver_schema", "mcdo_ph_silver")
dbutils.widgets.text("gold_schema", "mcdo_ph_gold")
dbutils.widgets.text(
    "landing_root",
    "s3://dante-classic-stable-ext-s3-049629455384-g7v92y/mcdo_ph/landing",
)

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
silver = dbutils.widgets.get("silver_schema")
gold = dbutils.widgets.get("gold_schema")
landing_root = dbutils.widgets.get("landing_root").rstrip("/")

print(f"catalog      = {catalog}")
print(f"bronze/silver/gold = {bronze} / {silver} / {gold}")
print(f"landing_root = {landing_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

# COMMAND ----------

for schema in (bronze, silver, gold):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"✓ schema {catalog}.{schema}")

spark.sql(
    f"ALTER SCHEMA `{catalog}`.`{bronze}` SET DBPROPERTIES "
    "('workshop'='gadc-c360','layer'='bronze')"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## S3 landing zone — external volume
# MAGIC
# MAGIC The landing zone is a **UC external volume** mapped onto an S3 external
# MAGIC location. Auto Loader ingests directly from `/Volumes/.../landing/...`,
# MAGIC which resolves to the underlying `s3://` path — this is the governed
# MAGIC "Auto Loader from S3 with Unity Catalog" pattern. Access is brokered by
# MAGIC the external location's storage credential (no keys in code).

# COMMAND ----------

spark.sql(
    f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS `{catalog}`.`{bronze}`.landing
    LOCATION '{landing_root}'
    COMMENT 'Raw source files (JSON) landed for Auto Loader ingestion.'
    """
)

volume_path = f"/Volumes/{catalog}/{bronze}/landing"
print(f"✓ external volume -> {volume_path}  (=> {landing_root})")

# Auto Loader keeps schema + checkpoint state on the same governed storage.
for sub in ("_schemas", "_checkpoints"):
    dbutils.fs.mkdirs(f"{volume_path}/{sub}")

# COMMAND ----------

dbutils.notebook.exit(
    f"Provisioned {catalog}: {bronze}/{silver}/{gold} + landing volume at {volume_path}"
)
