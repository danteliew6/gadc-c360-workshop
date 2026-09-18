# Module 2 · Lakeflow SDP pipeline + data-quality expectations

**Goal:** turn raw Bronze into governed, trustworthy Silver and a curated
**Customer 360** Gold layer using a **Lakeflow Spark Declarative Pipeline (SDP)**
— with data-quality **expectations**, an **SCD Type 2** customer dimension via
**Auto CDC**, and a governed **metric view**.

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
| `orders_clean`, `order_items_clean`, `app_events_clean` | **Materialized View** | batch reads of Bronze; recomputed each run |
| `orders_quarantine` | Materialized View | audit copy of rows that failed critical rules |
| `customers_clean` | **Temporary View** (streaming) | private staging that feeds the SCD2 flow |
| `dim_customer` | **Streaming Table** + **Auto CDC** | SCD Type 2 profile/tier history |
| `customer_360`, `store_performance`, `daily_sales`, `category_mix` | Materialized View (Gold) | aggregates over the full dataset |

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

## 5. Gold — the Customer 360 mart

`customer_360` is one row per member combining:

- **Profile** (current SCD2 row): tier, region, signup channel, language, consent.
- **RFM**: `recency_days`, `total_orders` (frequency), `total_spend` (monetary),
  scored into quintiles (`ntile(5)`) → `rfm_score` and a labeled `rfm_segment`
  (Champion / Promising / At Risk / Hibernating / Prospect).
- **CLV estimate**: annualized projection from realized spend over tenure.
- **Churn risk**: rule-based on recency (High > 60d, Medium > 30d, Low ≤ 30d).
- **Favorites**: `favorite_store`, `favorite_category`, `preferred_channel`
  computed with per-customer `row_number()` argmax windows.
- **Engagement**: active days, rewards redeemed, order count → `engagement_score`.

Supporting marts: `store_performance` (per-store revenue, AOV, drive-thru share),
`daily_sales` (date × region × channel for trends), `category_mix` (menu category
revenue, localized-item split). **Gold preserves the dimensions the dashboard
filters on** — region, tier, channel, segment, date.

## 6. Governed metric view

`04_metric_view.py` creates `gold.c360_metrics` — a UC **metric view** (YAML) that
defines KPIs *once* (Active Members, Repeat Rate, AOV, High-Churn-Risk Members,
Est. CLV) with dimensions (Region, Tier, RFM Segment, Channel). BI, Genie, and
ad-hoc SQL all get the **same** definitions. This is what keeps "repeat rate" from
meaning three different things in three different reports.

## 7. Run it

```bash
databricks bundle deploy -t dev -p fevm-dante-classic-stable
databricks bundle run   mcdo_ph_c360_workshop -t dev -p fevm-dante-classic-stable
```

Watch the pipeline's lineage graph and **Data quality** tab in the UI. Then:

```sql
SELECT rfm_segment, churn_risk, count(*) members, round(sum(total_spend),0) revenue
FROM   dante_classic_stable_catalog.mcdo_ph_gold.customer_360
GROUP BY 1,2 ORDER BY 1,2;
```

**Next:** [Module 3 — AI/BI dashboard](03-aibi-dashboard-c360.md)
