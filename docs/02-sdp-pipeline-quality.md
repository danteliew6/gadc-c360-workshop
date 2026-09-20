# Module 2 · Lakeflow SDP pipeline + data-quality expectations

**Goal:** turn raw Bronze into governed, trustworthy Silver and a curated
**Member 360 + MyMcDonald's Rewards points economy** Gold layer using a
**Lakeflow Spark Declarative Pipeline (SDP)** — with data-quality
**expectations**, an **SCD Type 2** customer dimension via **Auto CDC**, and a
governed **metric view**.

Code:
[`02_silver_quality.py`](../src/pipeline/02_silver_quality.py) ·
[`03_gold_customer360.py`](../src/pipeline/03_gold_customer360.py) ·
[`04_metric_view.py`](../src/pipeline/04_metric_view.py) ·
pipeline def [`c360_pipeline.pipeline.yml`](../resources/c360_pipeline.pipeline.yml)

> **Naming:** SDP = Lakeflow Declarative Pipelines = (formerly) DLT. The modern
> API is `from pyspark import pipelines as dp` — no more `import dlt`.

---

## 1. Why declarative pipelines

You declare **what** each dataset is (a query + quality rules); Lakeflow figures
out the **DAG**, incrementalization, checkpointing, retries, and observability.
You get, for free: a lineage graph, per-expectation quality metrics in the event
log, automatic recompute of only what changed, and serverless autoscaling.

The pipeline (`c360_pipeline.pipeline.yml`) is **serverless**, publishes Silver
to `mcdo_ph_silver` (the pipeline default schema) and Gold to `mcdo_ph_gold`
(via fully-qualified dataset names). Parameters flow in via `configuration:` and
are read with `spark.conf.get("workshop.*")`.

## 2. Dataset types used here

| Dataset | Type | Rationale |
|---------|------|-----------|
| `orders_clean`, `order_items_clean`, `app_events_clean`, `points_ledger_clean` | **Materialized View** | batch reads of Bronze; recomputed each run |
| `orders_quarantine`, `points_ledger_quarantine` | Materialized View | audit copies of rows that failed critical rules |
| `customers_clean` | **Temporary View** (streaming) | private staging that feeds the SCD2 flow |
| `dim_customer` | **Streaming Table** + **Auto CDC** | SCD Type 2 profile/tier history |
| `customer_360`, `points_economy`, `reward_performance`, `tier_migration`, `store_performance`, `daily_sales`, `category_mix` | Materialized View (Gold) | aggregates over the full dataset |

Rule of thumb from the pipeline decision tree: **streaming source → Streaming
Table; aggregation over full dataset → Materialized View.** Gold is all MVs
because it aggregates; the SCD2 dimension is a Streaming Table because Auto CDC
consumes a change stream.

## 3. Data-quality expectations — all three modes

Expectations are boolean SQL constraints attached to a dataset. Violations are
counted in the event log (visible in the pipeline UI's **Data quality** tab). The
enforcement action is what differs:

```python
# WARN — keep the row, just record the violation
@dp.expect_all(ORDER_RULES_WARN)
# DROP — discard rows that fail a critical rule
@dp.expect_all_or_drop(ORDER_RULES_CRITICAL)
@dp.materialized_view(...)
def orders_clean(): ...
```

Critical rules (drop): non-null `order_id`/`customer_id`, `net_amount > 0`, known
`channel`. Soft rules (warn): `discount <= gross`, known `order_status`,
delivery orders have a partner.

A third mode — **`expect_all_or_fail`** — aborts the update on any violation
(use for hard contracts). We don't fail the pipeline here; instead we pair
*drop* with an explicit **quarantine table** (`orders_quarantine`) that captures
the dropped rows and *why* (`_failed_rules`), so nothing is lost silently and the
data steward can triage:

```sql
SELECT _failed_rules, count(*)
FROM   dante_classic_stable_catalog.mcdo_ph_silver.orders_quarantine
GROUP BY _failed_rules;
```

The customer feed carries the injected dirty records — a warn on email format,
drops on null id / future signup date:

```python
@dp.expect_all({"valid_email_format": "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'"})
@dp.expect_all_or_drop({"valid_customer_id": "customer_id IS NOT NULL",
                        "signup_not_in_future": "signup_date <= current_date()"})
```

The **points ledger** gets the same treatment (`points_ledger_clean` +
`points_ledger_quarantine`): critical rules **drop** malformed rows — non-null
`ledger_id`/`customer_id`, a known `txn_type` in
`('EARN','REDEEM','EXPIRE','BONUS','ADJUST')`, non-null `points`; while soft
rules **warn** on the sign convention (earn/bonus add points, redeem/expire
remove them) and that a `REDEEM` carries a `reward_id`. This keeps the points
economy in Module 2's Gold marts trustworthy.

## 4. SCD Type 2 customer dimension (Auto CDC)

`dim_customer` keeps **full history** of profile / loyalty-tier changes. A cleaned
streaming view feeds `create_auto_cdc_flow`:

```python
dp.create_streaming_table(name="dim_customer")
dp.create_auto_cdc_flow(
    target="dim_customer", source="customers_clean",
    keys=["customer_id"], sequence_by="_ingest_ts",
    stored_as_scd_type=2,
    except_column_list=["_source_file", "_ingest_batch"])
```

Query current state with `WHERE __END_AT IS NULL`; history is the full table.
(Lakeflow uses the double-underscore `__START_AT` / `__END_AT` columns.) Gold's
`customer_360` reads only current rows.

## 5. Gold — the Member 360 mart + points economy

`customer_360` is one row per member combining the profile, RFM/CLV/churn, and
the full **MyMcDonald's Rewards points economy**:

- **Profile** (current SCD2 row): region, signup channel, language, consent.
- **Points economy** (from `points_ledger_clean`): `points_balance`,
  `lifetime_points_earned`, `lifetime_points_redeemed`, `points_expired`,
  `points_earned_12mo`, `redemptions_count`, `redemption_rate`, and an estimated
  peso `points_liability_php` (balance × ₱0.04/pt).
- **Status tiers**: `current_tier` (the tier the member *holds*) vs.
  `qualified_tier` (what their rolling-12-month earned points justify), and
  `tier_status` — **Upgrade eligible / On track / Downgrade risk** — plus
  `points_to_next_tier`. (This replaces the old raw `loyalty_tier` column.)
- **RFM**: `recency_days`, `total_orders` (frequency), `total_spend` (monetary),
  scored into quintiles (`ntile(5)`) → `rfm_score` and a labeled `rfm_segment`.
- **CLV estimate**, **churn risk** (recency-based), and **favorites**
  (`favorite_store`, `favorite_category`, `preferred_channel`).

Three marts model the rewards program itself:

- **`points_economy`** — daily points earned / redeemed / expired / bonus, net
  flow, and a running **outstanding balance** + peso liability (the growing
  liability line on the dashboard).
- **`reward_performance`** — per catalog reward: redemptions, points spent,
  unique members, and peso value delivered.
- **`tier_migration`** — the held-tier × qualified-tier transition matrix.

Supporting marts stay: `store_performance`, `daily_sales`, `category_mix`.
**Gold preserves the dimensions the dashboard filters on** — region, status
tier, segment, date.

## 6. Governed metric view

`04_metric_view.py` creates `gold.c360_metrics` — a UC **metric view** (YAML) that
defines the rewards KPIs *once*: Members, Active Members, **Points Earned /
Redeemed / Balance**, **Outstanding Liability PHP**, **Redemption Rate**,
**Members Redeeming**, **Upgrade Eligible / Downgrade Risk Members**, Est. CLV —
with dimensions Region, **Status Tier**, **Qualified Tier**, **Tier Status**, RFM
Segment. BI, Genie, and ad-hoc SQL all get the **same** definitions, so
"redemption rate" never means three different things in three reports.

## 7. Run it

```bash
databricks bundle deploy -t dev -p fevm-dante-classic-stable
databricks bundle run   mcdo_ph_c360_workshop -t dev -p fevm-dante-classic-stable
```

Watch the pipeline's lineage graph and **Data quality** tab in the UI. Then:

```sql
SELECT current_tier, tier_status, count(*) members,
       round(sum(points_balance),0) points, round(sum(points_liability_php),0) liability_php
FROM   dante_classic_stable_catalog.mcdo_ph_gold.customer_360
GROUP BY 1,2 ORDER BY 1,2;
```

**Next:** [Module 3 — AI/BI dashboard](03-aibi-dashboard-c360.md)
