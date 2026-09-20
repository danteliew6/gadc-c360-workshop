# Module 0 · Deploy your own copy (customer setup)

This repo is a **reference implementation** wired for the Field Engineering demo
workspace. To stand it up in **your own Databricks workspace**, fork the repo,
provision a few prerequisites, and change the handful of values that are pinned
to the demo environment. Everything is packaged as a **Databricks Asset Bundle
(DAB)**, so most of the config lives in `databricks.yml` `variables` you can
override at deploy time.

> **The data is synthetic.** No real customer data is used; the generator
> fabricates a Philippines-localized MyMcDonald's Rewards dataset.

---

## 1. Choose a deployment model

| Model | Who deploys | Good for |
|-------|-------------|----------|
| **A — Hosted** | One admin deploys once; everyone else opens the live dashboard + app | Large rooms, exec demos |
| **B — Self-serve Modules 1–3** | Each participant deploys Bronze→Silver→Gold + dashboard into their **own schemas** | Hands-on data-engineering labs |
| **C — Full copy incl. Module 4** | Admin (per environment) — needs Lakebase + a git-backed app | A complete standalone environment |

Modules 1–3 are portable via DAB variable overrides. **Module 4 (the Lakebase
app) is the heavy part** — it needs a Lakebase project and, because this
workspace enforces git-backed apps, a fork of this repo. Plan Module 4 as an
admin task, not a per-participant one.

## 2. Prerequisites

- A **Unity Catalog–enabled** Databricks workspace (the demo is on **AWS**; the
  Auto Loader landing zone is an S3 external location — on Azure/GCP use an ADLS/GCS
  external location instead).
- **Databricks CLI** (recent), authenticated: `databricks auth login -p <profile>`.
- A **UC catalog** you can create schemas in (bronze/silver/gold).
- A **SQL warehouse** (used by the dashboard and the reverse-ETL loader).
- An **external location + volume** on your own cloud storage for the raw landing
  files (Auto Loader source).
- **Module 4 only:** a **Lakebase** (Autoscaling Postgres) project + branch +
  endpoint; a **fork** of this repo on your GitHub org (the app deploys from git);
  and permission to create Databricks Apps. Node 20+/npm are used by the app, but
  the git-backed app **builds itself on deploy** — you don't build locally.

## 3. Values pinned to the demo — change these

| File | Setting | Demo value → change to |
|------|---------|------------------------|
| `databricks.yml` | `targets.dev/prod → workspace.host` | `fevm-dante-classic-stable…` → **your workspace URL** |
| `databricks.yml` | var `catalog` | `dante_classic_stable_catalog` → **your catalog** |
| `databricks.yml` | var `landing_root` | demo S3 URI → **your external-location URI** |
| `databricks.yml` | var `warehouse_id` | `751d12396d67eb55` → **your SQL warehouse id** |
| `databricks.yml` | vars `lakebase_project` / `_branch` / `_database` | `mcdo-ph-c360…` → **your Lakebase paths** (M4) |
| `src/dashboards/mcdo_ph_c360.lvdash.json` | every `FROM dante_classic_stable_catalog.mcdo_ph_gold.…` | **your `catalog.gold_schema`** (search-and-replace; the dashboard JSON isn't variable-substituted) |
| `resources/c360_app.app.yml` | `git_repository.url` | this repo → **your fork's URL** (M4) |
| `app/app.yaml` | `LAKEBASE_ENDPOINT`, `PGHOST` | demo endpoint/host → **your Lakebase endpoint + host** (M4) |
| `scripts/load_serving_lakebase.py` | `PROFILE`, `WS_HOST`, `WAREHOUSE_HTTP`, `LB_HOST`, `ENDPOINT`, `PG_USER`, `APP_SP` | demo values → **yours** (M4 reverse-ETL) |

The `catalog` / `*_schema` / `landing_root` / `warehouse_id` values can be passed
at deploy time with `--var` (see below) instead of editing the file — except the
**dashboard JSON**, which needs a literal search-and-replace of the
`catalog.schema` prefix because DAB uploads it as-is.

## 4. Deploy Modules 1–3 (data + dashboard)

```bash
# Auth to your workspace
databricks auth login -p myco

# Fork/clone the repo, then deploy with your environment's values
databricks bundle deploy -t dev -p myco \
  --var="catalog=myco_catalog" \
  --var="bronze_schema=mcdo_bronze" \
  --var="silver_schema=mcdo_silver" \
  --var="gold_schema=mcdo_gold" \
  --var="landing_root=s3://<your-external-location>/mcdo_ph/landing" \
  --var="warehouse_id=<your_wh_id>"

# Run the end-to-end job: setup → generate synthetic data → Auto Loader →
# SDP (Silver+Gold) → metric view
databricks bundle run mcdo_ph_c360_workshop -t dev -p myco
```

Before this works, **search-and-replace** the catalog/schema in
`src/dashboards/mcdo_ph_c360.lvdash.json` (e.g. `dante_classic_stable_catalog.mcdo_ph_gold`
→ `myco_catalog.mcdo_gold`) and re-run `databricks bundle deploy`, then publish:

```bash
databricks lakeview publish <DASHBOARD_ID> --warehouse-id <your_wh_id> -p myco
```

`<DASHBOARD_ID>` comes from `databricks bundle summary -t dev -p myco`.

## 5. Deploy Module 4 (Lakebase app) — admin

1. **Create a Lakebase project** (Autoscaling, PG 17) and note its endpoint path
   (`projects/<p>/branches/<b>/endpoints/<e>`) and host.
2. Set the `lakebase_*` vars in `databricks.yml`, and the endpoint/host in
   `app/app.yaml` (and, if you run the loader, in `scripts/load_serving_lakebase.py`).
3. Point `resources/c360_app.app.yml → git_repository.url` at **your fork**, and
   push your changes to its `main`.
4. Load the serving tables the app reads (reverse-ETL Gold → `serving.*`, grants
   the app SP `SELECT`):
   ```bash
   uv venv /tmp/mcdo_venv && /tmp/mcdo_venv/bin/pip install "psycopg[binary]" databricks-sql-connector
   /tmp/mcdo_venv/bin/python scripts/load_serving_lakebase.py
   ```
   > The **preferred** path is managed Lakebase **synced tables**
   > (`scripts/provision_lakebase.sh`), but that needs `CREATE CATALOG` on the
   > metastore; the reverse-ETL is the privilege-light equivalent.
5. Deploy the app. Because it's git-backed, the deploy request carries only the
   git **ref** + source path (the repo url lives on the app resource):
   ```bash
   databricks bundle deploy -t dev -p myco
   databricks apps deploy mcdo-ph-c360 -p myco \
     --json '{"git_source": {"branch": "main", "source_code_path": "app"}, "mode": "SNAPSHOT"}'
   ```
   The app SP creates and owns the `app.*` write-back schema on first startup.

## 6. Per-participant isolated copies (Model B)

DAB **dev mode** prefixes resource names with `[dev <user>]` and pauses
schedules, so many learners can deploy into **one** workspace safely — each just
points at their own schemas:

```bash
databricks bundle deploy -t dev -p <their-profile> \
  --var="catalog=<shared_or_own_catalog>" \
  --var="bronze_schema=<user>_bronze" \
  --var="silver_schema=<user>_silver" \
  --var="gold_schema=<user>_gold" \
  --var="landing_root=s3://<ext-loc>/<user>/landing" \
  --var="warehouse_id=<wh>"
```

(Each learner still needs to search-replace their catalog/schema in the dashboard
JSON, or skip the dashboard and use the shared hosted one.)

## 7. Teardown

```bash
databricks bundle destroy -t dev -p <profile>
```

Then drop the schemas / Lakebase project you created if they're no longer needed.

---

**Next:** [Module 1 — Auto Loader from S3](01-autoloader-from-s3.md)
