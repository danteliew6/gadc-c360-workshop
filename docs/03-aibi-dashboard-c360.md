# Module 3 · AI/BI MyMcDonald's Rewards dashboard

**Goal:** a governed, executive-ready **AI/BI (Lakeview)** dashboard over the Gold
marts — the loyalty **points economy**: earn/redeem/liability KPIs, the growing
outstanding-liability trend, status-tier health, and reward-catalog performance —
plus a governed **metric view** and optional **Genie** link.

Dashboard JSON: [`src/dashboards/mcdo_ph_c360.lvdash.json`](../src/dashboards/mcdo_ph_c360.lvdash.json)
Resource: [`c360_dashboard.dashboard.yml`](../resources/c360_dashboard.dashboard.yml)

---

## 1. Structure

Three pages, backed by four datasets over the Gold layer:

| Dataset | Source (Gold) | Powers |
|---------|---------------|--------|
| `ds_members` | `customer_360` | KPIs, tier/status/region breakdowns, top-member-by-points table |
| `ds_points` | `points_economy` | liability trend, points-earned trend |
| `ds_rewards` | `reward_performance` | redemptions by reward, value by tier, catalog table |
| `ds_tiers` | `tier_migration` | held-vs-qualified tier bar |

- **Page 1 · Membership Overview** — KPI row (Active Members, Outstanding Points,
  Outstanding Liability ₱, Redemption Rate), the **outstanding points-liability**
  trend, members-by-status-tier pie, a **tier-status** bar (Downgrade risk = red,
  Upgrade eligible = gold, On track = green — pinned as literal hex), points
  liability by region, and a top-members-by-points-balance table.
- **Page 2 · Rewards & Points Economy** — reward KPIs (Total Redemptions, Points
  Redeemed, Value Delivered ₱, Members Redeeming), redemptions-by-reward and
  value-by-reward-tier bars, a monthly points-earned trend, the **held-tier ×
  qualified-tier** grouped bar, and the full rewards-catalog table.
- **Page 3 · Filters** — a global-filters page: **Region**, **Status tier**, and a
  **Transaction-date** range.

`ds_members` backs most Page-1 widgets, so clicking a tier/status/region bar
**cross-filters** the whole page — no explicit filter needed.

## 2. Governed KPIs — measures once, everywhere

`ds_members` declares dataset-level `columns` measures (Active Members, Outstanding
Points, Points Earned/Redeemed, Outstanding Liability, Redemption Rate, Members
Redeeming, Upgrade Eligible / Downgrade Risk Members) referenced in widgets via
`MEASURE(\`...\`)`. The same definitions also live in the UC **metric view**
`gold.c360_metrics` (Module 2), so Genie and ad-hoc SQL compute identical numbers.

## 3. Build discipline (tested before deploy)

Per the AI/BI workflow, every dataset query was validated against the warehouse
before deploying — e.g.:

```bash
DATABRICKS_WAREHOUSE_ID=<wh> databricks experimental aitools tools query \
  -p fevm-dante-classic-stable --output json \
  "SELECT current_tier, tier_status, count(*) FROM dante_classic_stable_catalog.mcdo_ph_gold.customer_360 GROUP BY 1,2"
```

This dashboard is **bundle-managed** (a DAB `dashboards` resource), so the dataset
`FROM` clauses are **fully qualified** (`FROM dante_classic_stable_catalog.mcdo_ph_gold.customer_360`)
— there is no `--dataset-catalog` flag in the DAB path.

## 4. Deploy

The dashboard ships as part of the bundle — deploying the bundle creates/updates
it, then publish:

```bash
databricks bundle deploy -t dev -p fevm-dante-classic-stable
databricks lakeview publish <DASHBOARD_ID> --warehouse-id 751d12396d67eb55 -p fevm-dante-classic-stable
```

> **Updating an existing dashboard keeps the same id + URL** — the bundle updates
> the draft in place; always `publish` after to roll the change to viewers.

## 5. Theme & Genie

The dashboard uses a McDonald's-branded palette (red `#DA291C`, gold `#FFC72C`)
with the semantic tier-status colors pinned as literal hex. To add an **Ask
Genie** button, set `uiSettings.genieSpace.overrideId` to a Genie space built on
the same Gold tables / metric view — natural-language Q&A over the exact governed
definitions.

**Next:** [Module 4 — Databricks App on Lakebase](04-databricks-app-lakebase.md)
