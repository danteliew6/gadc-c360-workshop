# McDonald's PH — MyMcDonald's Rewards Membership Analytics Workshop

A hands-on, end-to-end **Databricks Data Intelligence Platform** workshop built for
**GADC (Golden Arches Development Corporation)** — the McDonald's franchise operator
in the Philippines. It takes raw **MyMcDonald's Rewards** loyalty-app and point-of-sale
data all the way to a live, operational **membership analytics** experience — the
**points economy**, status tiers, and rewards catalog — on the modern Databricks stack.

> The dataset is **synthetic** and Philippines-localized (NCR/regional stores,
> GCash/Maya payments, McDelivery, localized menu). The loyalty mechanics are modeled
> on **MyMcDonald's Rewards** (earn 1 pt/₱1 × status-tier multiplier; redeem across the
> 1500/3000/4500/6000-point reward tiers). **No real customer data is used.**

## What you build

| # | Module | Product | Outcome |
|---|--------|---------|---------|
| 1 | **Ingest** | Auto Loader (Structured Streaming) from **S3** | Incremental, schema-evolving ingestion of 5 raw feeds (incl. the **points ledger**) into Bronze |
| 2 | **Transform + govern** | **Lakeflow SDP** pipeline w/ **data-quality expectations** + SCD2 | Cleaned Silver + curated **Member 360** + points-economy Gold marts |
| 3 | **Analyze** | **AI/BI (Lakeview)** dashboard + governed **metric view** | Membership & rewards analytics (points earned/redeemed, liability, tier health) + Genie-ready KPIs |
| 4 | **Activate** | **Databricks App** on **Lakebase** (Postgres) | Membership-care console: millisecond reads + points/reward write-back |

### MyMcDonald's Rewards model (synthetic)

- **Earn** 1 point per ₱1 net spend on app-linked orders, times a **status-tier
  multiplier** — Member ×1.0 · Silver ×1.1 · Gold ×1.25 · Platinum ×1.5.
- **Redeem** points against a rewards catalog priced in the four canonical tiers
  **1500 / 3000 / 4500 / 6000** points.
- Points **expire** after 12 months (program *breakage*); **bonus** promos add points.
  The `points_ledger` feed is the single source of truth for the points economy.
- **Status tier** is qualified on rolling-12-month earned points; the gap between the
  tier a member *holds* and the tier they *qualify* for drives up/downgrade actions.

## Architecture

```
   loyalty app / POS                 Unity Catalog (dante_classic_stable_catalog)
   ─────────────────      ┌───────────────────────────────────────────────────────┐
   customers.json  ──┐    │  BRONZE (mcdo_ph_bronze)   SILVER (mcdo_ph_silver)      │
   orders.json     ──┤    │  customers_raw       ┌──►  orders_clean  (expectations) │
   order_items.json──┤    │  orders_raw          │     order_items_clean            │
   app_events.json ──┤►   │  app_events_raw      │     app_events_clean             │
   points_ledger.json─┘   │  points_ledger_raw ──┘     points_ledger_clean (+quar.) │
        │                 │  dim_store / dim_product   dim_customer (SCD2, Auto CDC)│
        ▼ (land files)    │  dim_reward / dim_tier              │                   │
   S3 external volume     │        ▲ Auto Loader                ▼                   │
   s3://…/mcdo_ph/landing │  [Module 1] cloudFiles     GOLD (mcdo_ph_gold)          │
                          │                            customer_360 (Member 360) ·  │
                          │  [Module 2] SDP + DQ ─────► points_economy · reward_perf │
                          │                            · tier_migration · store_perf │
                          └────────────┬─────────────────────────┬──────────────────┘
                                       │                          │
                        [Module 3] AI/BI dashboard   [Module 4] Lakebase serving tables
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
│   ├── data_generation/generate_and_land.py   # synthetic data (+ points ledger) → S3
│   ├── pipeline/01_bronze_autoloader.py        # Module 1 — Auto Loader (5 feeds)
│   ├── pipeline/02_silver_quality.py           # Module 2 — Silver + expectations (incl. points ledger)
│   ├── pipeline/03_gold_customer360.py         # Module 2 — Member 360 + points-economy marts
│   └── pipeline/04_metric_view.py              # Module 2 — rewards metric view
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

# 3. Load the Lakebase serving tables the app reads (Module 4)
/tmp/mcdo_venv/bin/python scripts/load_serving_lakebase.py   # reverse-ETL Gold -> Lakebase
#   (managed synced tables — scripts/provision_lakebase.sh — are the preferred
#    path but need CREATE CATALOG on the metastore; the reverse-ETL is the
#    privilege-light equivalent used here.)
```

## Live deployment (fevm-dante-classic-stable)

| Asset | Link / id |
|-------|-----------|
| AI/BI dashboard | `dashboardsv3/01f1b337a37b1f7b9978024dbaee5432/published` |
| Databricks App | https://mcdo-ph-c360-7474647641788932.aws.databricksapps.com |
| SDP pipeline | `[dev] McDonald's PH — C360 SDP (Silver + Gold)` |
| Lakebase project | `mcdo-ph-c360` (Autoscaling, PG 17) |

Then open the **AI/BI dashboard** and the **Databricks App** from the workspace.
Each `docs/0X-*.md` walks through the concepts and the code for that module.

## Target environment

- **Workspace:** `fevm-dante-classic-stable` (AWS `us-east-1`)
- **Catalog:** `dante_classic_stable_catalog` · schemas `mcdo_ph_bronze` / `_silver` / `_gold`
- **SQL warehouse:** `dante-wh` · **Lakebase project:** `mcdo-ph-c360`

All names are parameterized as DAB `variables` in `databricks.yml` — override per learner
or per target to run isolated copies.
