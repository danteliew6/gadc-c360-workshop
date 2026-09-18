# Module 1 · Auto Loader ingestion from S3

**Goal:** incrementally ingest four raw JSON feeds (`customers`, `orders`,
`order_items`, `app_events`) that land in an **S3 bucket** into Bronze Delta
tables, with schema inference, schema evolution, and rescued-data capture.

Code: [`src/pipeline/01_bronze_autoloader.py`](../src/pipeline/01_bronze_autoloader.py)
Landing zone: [`src/setup/00_provision_uc.py`](../src/setup/00_provision_uc.py)
Data generator: [`src/data_generation/generate_and_land.py`](../src/data_generation/generate_and_land.py)

---

## 1. Why Auto Loader

Auto Loader (`cloudFiles`) is Databricks' engine for **incremental file
ingestion** from cloud object storage. Versus a naive `spark.read.json(path)` on
a schedule, it gives you:

- **Exactly-once, incremental discovery** — it tracks which files it has already
  processed in a checkpoint, so re-runs only read *new* files. No manual
  high-water-marking, no reprocessing.
- **Scalable file discovery** — *directory listing* (default) for modest volumes,
  or *file notification* mode (SNS/SQS on AWS) that scales to millions of
  files/day without listing the bucket.
- **Schema inference & evolution** — infers types on first run, persists the
  schema, and adapts as new columns appear.
- **Rescued data** — values that don't fit the inferred schema are preserved in a
  `_rescued_data` column instead of being dropped or failing the job.

## 2. The S3 landing zone (Unity Catalog governed)

The landing zone is a **UC external volume** mapped onto an S3 external location:

```sql
CREATE EXTERNAL VOLUME IF NOT EXISTS
  dante_classic_stable_catalog.mcdo_ph_bronze.landing
LOCATION 's3://dante-classic-stable-ext-s3-049629455384-g7v92y/mcdo_ph/landing';
```

`/Volumes/dante_classic_stable_catalog/mcdo_ph_bronze/landing` resolves to that
`s3://…` path. Access is brokered by the external location's **storage
credential** (IAM role) — no access keys ever appear in code, and every read is
governed and audited by Unity Catalog.

> To point Auto Loader at a raw `s3://` URI directly instead of the volume, just
> pass the `s3://…` string to `.load()`. The external location's credential still
> brokers access. The volume form is preferred because it's governed and portable.

## 3. The ingestion pattern

The reusable helper in `01_bronze_autoloader.py`:

```python
reader = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{vol}/_schemas/{table}")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxFilesPerTrigger", "200")
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{vol}/{source_dir}"))

(reader
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream
    .option("checkpointLocation", f"{vol}/_checkpoints/{table}")
    .trigger(availableNow=True)      # process all new files, then stop
    .toTable(table))
```

Key decisions:

| Option | Choice | Why |
|--------|--------|-----|
| `schemaLocation` | per-table dir under the volume | persists inferred schema + tracks evolution |
| `inferColumnTypes` | `true` | infer `int`/`double`/`timestamp`, not all strings |
| `schemaEvolutionMode` | `addNewColumns` | stream restarts and picks up new fields automatically |
| `rescuedDataColumn` | `_rescued_data` | never silently lose malformed/extra data |
| `trigger` | `availableNow=True` | batch-style: drain all files then stop (ideal for scheduled jobs). Swap for `.trigger(processingTime="30 seconds")` for a continuously-running stream |
| `_metadata.file_path` | added column | lineage — which source file each row came from |

We also apply **Liquid Clustering** (`ALTER TABLE … CLUSTER BY (…)`) so Bronze
reads stay fast as tables grow — `region` for customers, `order_ts`/`event_ts`
for the time-series feeds.

## 4. Demonstrating incrementality

1. First run: the generator lands the full history; Auto Loader ingests every
   file and records them in the checkpoint.
2. Land an **incremental** batch (new recent files only):

   ```bash
   databricks bundle run mcdo_ph_c360_workshop -t dev \
     --python-params # or run the generate task with mode=incremental, batch_id=1
   ```

   Re-run `01_bronze_autoloader` — it processes **only the new files**; existing
   rows are untouched. Confirm with the ingestion-metadata query at the bottom of
   the notebook (`max(_ingest_ts)` advances, row counts grow by exactly the new
   batch).

## 5. Inspecting rescued data & lineage

```sql
SELECT _source_file, _ingest_ts, _rescued_data
FROM   dante_classic_stable_catalog.mcdo_ph_bronze.orders_raw
WHERE  _rescued_data IS NOT NULL
LIMIT 20;
```

Anything the inferred schema couldn't place lands here — a running audit of
source drift. In this dataset the generator injects a few malformed records
(bad emails, null IDs, future signup dates); those are caught downstream by the
**Module 2 expectations**, not here — Auto Loader's job is to *land everything
losslessly*, and let the declarative pipeline enforce quality.

**Next:** [Module 2 — SDP pipeline + data-quality expectations](02-sdp-pipeline-quality.md)
