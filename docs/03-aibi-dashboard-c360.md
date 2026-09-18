# Module 3 · AI/BI Customer 360 dashboard

**Goal:** a governed, executive-ready **AI/BI (Lakeview)** dashboard over the Gold
marts — loyalty KPIs, revenue trends, RFM/churn segmentation, network and menu
performance — plus a governed **metric view** and optional **Genie** link.

Dashboard JSON: [`src/dashboards/mcdo_ph_c360.lvdash.json`](../src/dashboards/mcdo_ph_c360.lvdash.json)
Resource: [`c360_dashboard.dashboard.yml`](../resources/c360_dashboard.dashboard.yml)

---

## 1. Structure

Three pages, backed by four datasets over the Gold layer:

| Dataset | Source (Gold) | Powers |
|---------|---------------|--------|
| `ds_members` | `customer_360` | KPIs, tier/RFM/churn/region breakdowns, top-member table |
| `ds_daily` | `daily_sales` | revenue trend, channel mix |
| `ds_stores` | `store_performance` | store map, store table, network KPIs |
| `ds_category` | `category_mix` | menu-category revenue |

- **Page 1 · Customer 360** — KPI row (Active Members, Lifetime Revenue, AOV,
  Repeat Rate), daily revenue trend, tier pie, RFM-segment bar, churn-risk bar
  (semantic colors: High = red, Medium = gold, Low = green), revenue-by-region
  bar, channel-mix pie, and a top-members-by-CLV table.
- **Page 2 · Stores & Menu** — network KPIs, a **symbol map** of stores colored by
  revenue, category-revenue bar, and a store-performance table.
- **Page 3 · Filters** — a global-filters page: **Region** (cascades to all four
  datasets), **Loyalty tier**, and an **Order-date** range.

`ds_members` backs most Page-1 widgets, so clicking a segment/tier/region bar
**cross-filters** the whole page — no explicit filter needed.

## 2. Governed KPIs — measures once, everywhere

`ds_members` declares dataset-level `columns` measures (Active Members, Total
Revenue, Avg Order Value, Repeat Rate, High Churn Members, Est CLV) referenced
in widgets via `MEASURE(\`...\`)`. The same definitions also live in the UC
**metric view** `gold.c360_metrics` (Module 2), so Genie and ad-hoc SQL compute
identical numbers.

## 3. Build discipline (tested before deploy)

Per the AI/BI workflow, every dataset query was validated against the warehouse
before deploying — e.g.:

```bash
DATABRICKS_WAREHOUSE_ID=<wh> databricks experimental aitools tools query \
  -p fevm-dante-classic-stable --output json \
  "SELECT rfm_segment, count(*) FROM dante_classic_stable_catalog.mcdo_ph_gold.customer_360 GROUP BY 1"
```

Inside the dashboard JSON the `FROM` clause uses **bare table names**
(`FROM customer_360`); the catalog/schema are supplied by the
`--dataset-catalog` / `--dataset-schema` flags at create time, keeping the
dashboard portable across environments.

## 4. Deploy

Via the CLI (bundle-managed resource, or directly):

```bash
databricks lakeview create \
  --display-name "McDonald's PH — Customer 360" \
  --warehouse-id 751d12396d67eb55 \
  --dataset-catalog dante_classic_stable_catalog \
  --dataset-schema mcdo_ph_gold \
  --serialized-dashboard "$(cat src/dashboards/mcdo_ph_c360.lvdash.json)" \
  --json '{"parent_path": "/Workspace/Users/dante.liew@databricks.com/gadc-c360"}'
# then: databricks lakeview publish <DASHBOARD_ID> --warehouse-id 751d12396d67eb55
```

## 5. Theme & Genie

The dashboard uses a McDonald's-branded palette (red `#DA291C`, gold `#FFC72C`)
with semantic churn colors pinned as literal hex. To add an **Ask Genie** button,
set `uiSettings.genieSpace.overrideId` to a Genie space built on the same Gold
tables / metric view — natural-language Q&A over the exact governed definitions.

**Next:** [Module 4 — Databricks App on Lakebase](04-databricks-app-lakebase.md)
