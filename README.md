# McDonald's PH — Customer 360 Lakehouse Workshop

A hands-on, end-to-end **Databricks Data Intelligence Platform** workshop built for
**GADC (Golden Arches Development Corporation)** — the McDonald's franchise operator
in the Philippines. It takes raw loyalty-app and point-of-sale data all the way to a
live, operational **Customer 360** experience, using the modern Databricks stack.

> The dataset is **synthetic** and Philippines-localized (NCR/regional stores,
> GCash/Maya payments, McDelivery, localized menu). No real customer data is used.

## What you build

| # | Module | Product | Outcome |
|---|--------|---------|---------|
| 1 | **Ingest** | Auto Loader (Structured Streaming) from **S3** | Incremental, schema-evolving ingestion of 4 raw feeds into Bronze |
| 2 | **Transform + govern** | **Lakeflow SDP** pipeline w/ **data-quality expectations** + SCD2 | Cleaned Silver + curated **Customer 360** Gold marts |
| 3 | **Analyze** | **AI/BI (Lakeview)** dashboard + governed **metric view** | Executive Customer 360 analytics + Genie-ready KPIs |
| 4 | **Activate** | **Databricks App** on **Lakebase** (Postgres) | Customer-care console: millisecond reads + operational write-back |

## Architecture

```
   loyalty app / POS                 Unity Catalog (dante_classic_stable_catalog)
   ─────────────────      ┌───────────────────────────────────────────────────────┐
   customers.json  ──┐    │  BRONZE (mcdo_ph_bronze)   SILVER (mcdo_ph_silver)      │
   orders.json     ──┤    │  customers_raw       ┌──►  orders_clean  (expectations) │
   order_items.json──┼─►  │  orders_raw          │     order_items_clean            │
   app_events.json ──┘    │  app_events_raw ─────┘     app_events_clean             │
        │                 │  dim_store / dim_product   dim_customer (SCD2, Auto CDC)│
        ▼ (land files)    │        ▲                            │                   │
   S3 external volume     │        │ Auto Loader                ▼                   │
   s3://…/mcdo_ph/landing │  [Module 1] cloudFiles     GOLD (mcdo_ph_gold)          │
                          │                            customer_360 · store_perf ·  │
                          │  [Module 2] SDP + DQ ─────► daily_sales · category_mix   │
                          └────────────┬─────────────────────────┬──────────────────┘
                                       │                          │
                        [Module 3] AI/BI dashboard   [Module 4] Lakebase synced tables
                        + c360_metrics metric view   → Databricks App (reads + write-back)
```

## Repo layout

```
gadc-c360-workshop/
├── databricks.yml                     # DAB: bundles all resources + targets
├── resources/                         # DAB resource definitions
│   ├── c360_pipeline.pipeline.yml     #   Module 2 — SDP pipeline
│   ├── c360_workshop.job.yml          #   end-to-end orchestration job
│   ├── c360_dashboard.dashboard.yml   #   Module 3 — AI/BI dashboard
│   └── c360_app.app.yml               #   Module 4 — Databricks App
├── src/
│   ├── setup/00_provision_uc.py       # schemas + S3 landing volume
│   ├── data_generation/generate_and_land.py   # synthetic data → S3
│   ├── pipeline/01_bronze_autoloader.py        # Module 1 — Auto Loader
│   ├── pipeline/02_silver_quality.py           # Module 2 — Silver + expectations
│   ├── pipeline/03_gold_customer360.py         # Module 2 — Gold marts
│   └── pipeline/04_metric_view.py              # Module 2 — metric view
├── app/                               # Module 4 — React/AppKit + FastAPI app
├── scripts/provision_lakebase.sh      # one-time Lakebase synced tables + grants
└── docs/                              # step-by-step module guides
    ├── architecture.md
    ├── 01-autoloader-from-s3.md
    ├── 02-sdp-pipeline-quality.md
    ├── 03-aibi-dashboard-c360.md
    └── 04-databricks-app-lakebase.md
```

## Quickstart

```bash
# 0. Auth (Field Engineering workspace)
databricks auth login -p fevm-dante-classic-stable

# 1. Deploy all resources
databricks bundle deploy -t dev -p fevm-dante-classic-stable

# 2. Run the whole pipeline end-to-end (setup → generate → Auto Loader → SDP → metrics)
databricks bundle run mcdo_ph_c360_workshop -t dev -p fevm-dante-classic-stable

# 3. Provision Lakebase serving tables + deploy the app (Module 4)
bash scripts/provision_lakebase.sh
```

Then open the **AI/BI dashboard** and the **Databricks App** from the workspace.
Each `docs/0X-*.md` walks through the concepts and the code for that module.

## Target environment

- **Workspace:** `fevm-dante-classic-stable` (AWS `us-east-1`)
- **Catalog:** `dante_classic_stable_catalog` · schemas `mcdo_ph_bronze` / `_silver` / `_gold`
- **SQL warehouse:** `dante-wh` · **Lakebase project:** `mcdo-ph-c360`

All names are parameterized as DAB `variables` in `databricks.yml` — override per learner
or per target to run isolated copies.
