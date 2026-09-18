# McDonald's Philippines (GADC) — Customer 360

Customer Care & Loyalty Console for a Databricks workshop (white-label demo,
synthetic data). React + AppKit + Vite front end, `@databricks/appkit` Express
server, backed by an **Autoscaling Lakebase** (managed Postgres) project.

## What it does

- **Member Search** — fast lookup over `serving.customer_360` by name / ID /
  email, filtered by region and RFM segment.
- **Customer 360** — full profile: tier + churn/RFM chips, KPI row (lifetime
  spend, orders, AOV, CLV, recency, engagement), RFM/favorites/engagement/contact
  panels, and operational **write-back actions** (voucher, service ticket, tier
  change, note) with a recent-actions feed.
- **Store Overview** — KPIs + top-store bar chart + sortable table over
  `serving.store_performance`.

## Data model

- **Reads** (read-only): Lakebase synced tables in schema `serving`
  (`customer_360`, `store_performance`) — reverse-ETL'd from Unity Catalog.
- **Writes** (app-owned): schema `app` — `vouchers`, `service_tickets`,
  `tier_changes`, `notes`. Created with `CREATE SCHEMA/TABLE IF NOT EXISTS` on
  server startup. The app never writes to `serving`.

All queries are parameterized; schema/table identifiers are guarded
(`server/lakebase.ts`).

## Auth / connectivity

Autoscaling Lakebase mints endpoint-scoped OAuth tokens (endpoint
`projects/mcdo-ph-c360/branches/production/endpoints/primary`). `server/lakebase.ts`
uses `@databricks/lakebase`'s `createLakebasePool()`, which returns a standard
`pg.Pool` whose password callback refreshes the ~1h token automatically. Hosted:
runs as the app service principal (bound via the `postgres` app resource).
Local: uses the `DATABRICKS_CONFIG_PROFILE` CLI profile.

## Local dev

```bash
npm install
npm run build        # server (tsdown) + client (vite)
npm run typecheck    # tsc -b (server + client)
DATABRICKS_CONFIG_PROFILE=fevm-dante-classic-stable npm run dev
```

## Deploy

Git-backed (this workspace enforces it). Deployed via the parent bundle
(`../databricks.yml` + `../resources/c360_app.app.yml`), app name `mcdo-ph-c360`.
