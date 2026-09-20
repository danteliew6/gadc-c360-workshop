# Module 4 · Databricks App on Lakebase (fast reads + write-back)

**Goal:** an operational **MyMcDonald's Rewards — Membership 360** console for
McDonald's PH loyalty / care agents. It serves each member's points economy and
status tier at OLTP speed from **Lakebase** (managed Postgres) and writes
operational actions — **points adjustments**, **reward redemptions**, vouchers,
service tickets, tier changes, notes — **back** to Lakebase. This is the
"activation" layer that closes the loop from analytics to action.

App code: [`app/`](../app) · Provisioning: [`scripts/provision_lakebase.sh`](../scripts/provision_lakebase.sh)

---

## 1. Why Lakebase for an app

The Gold `customer_360` and `reward_performance` marts live in Delta — great for
analytics, wrong shape for a transactional app that does per-member lookups and
writes on every click. **Lakebase** is serverless Postgres integrated with the
lakehouse:

- **Fast reads:** the app queries small, indexed Postgres tables (single-digit-ms
  point lookups) instead of scanning Delta on each request.
- **Fast writes:** operational actions are true OLTP `INSERT`s — no Delta
  small-file problem, immediate read-after-write.
- **Lakehouse-integrated:** Gold Delta tables are kept in sync into Postgres via
  managed **synced tables**; write-back tables can flow *back* to Delta via
  Lakehouse Sync for downstream analytics.

## 2. Two data paths

```
 READS  (analytics → app)                WRITES (app → operations)
 gold.customer_360      ──reverse-ETL──►  serving.customer_360      (read-only, app SP: SELECT)
 gold.reward_performance ──reverse-ETL─►  serving.reward_performance
 gold.store_performance ──reverse-ETL──►  serving.store_performance
                                          app.points_adjustments ┐
                                          app.reward_grants      │
 app SP owns + writes ────────────────►   app.vouchers           ├─ OLTP write-back
                                          app.service_tickets    │   (app SP: OWNER)
                                          app.tier_changes        │
                                          app.notes              ┘
```

- **`serving.*`** — the Gold marts materialized into Lakebase for OLTP reads.
  The **preferred** path is managed Lakebase **synced tables**
  (`scripts/provision_lakebase.sh`, SNAPSHOT mode), but that registers the
  Postgres DB as a UC catalog and needs `CREATE CATALOG` on the metastore. Where
  that privilege isn't available (as in this FE workspace), use the
  **reverse-ETL** equivalent `scripts/load_serving_lakebase.py`, which reads Gold
  via a SQL warehouse and loads `serving.customer_360` /
  `serving.reward_performance` / `serving.store_performance` into Postgres, then
  grants the app SP `SELECT`. Either way the tables are read-only to the app.
- **`app.*`** — write-back tables **owned by the app's Service Principal**, created
  on first startup: `points_adjustments` (goodwill/clawback points),
  `reward_grants` (a reward redeemed on the member's behalf), plus vouchers,
  service tickets, tier changes, and notes.

> Synced tables are read-only in Postgres — never write to `serving.*`. All app
> writes go to the SP-owned `app.*` schema.

## 3. Tech stack (AppKit)

React + TypeScript + Vite front end, `@databricks/appkit` Express server, `pg`
connection pool — the same proven stack as our other Lakebase apps:

- **`server/lakebase.ts`** — builds the pool with `@databricks/lakebase`'s
  `createLakebasePool`, which handles the endpoint-scoped OAuth credential and its
  ~1h refresh for the **Autoscaling** Lakebase project (per-endpoint, not the
  legacy per-instance API). Parameterized queries only.
- **`server/server.ts`** — read routes over `serving.*` (member search with a
  **tier** filter, member 360, rewards catalog `/api/rewards`) + write routes into
  `app.*` (**adjust points**, **redeem reward**, voucher, ticket, tier change,
  note).
- **`client/src/pages/`** — Member Search (tier + points columns), Member 360
  (a **MyMcDonald's Rewards** panel: balance, held vs qualified tier, redemption
  rate, liability + points-adjust / redeem actions), and a **Rewards Catalog**
  page. The sidebar is white-labeled with the McDonald's arches logo.

## 4. Connection & auth

The `postgres` resource binding on the app injects `PGHOST` / `PGPORT` /
`PGDATABASE` / `PGUSER`; the pool mints & refreshes tokens itself via
`createLakebasePool` (Autoscaling endpoint credential):

```ts
import { createLakebasePool } from '@databricks/lakebase';
pool = createLakebasePool({
  endpoint: 'projects/mcdo-ph-c360/branches/production/endpoints/primary',
  host: PGHOST, database: PGDATABASE, user: process.env.PGUSER,
  ssl: { rejectUnauthorized: false }, maxLifetimeSeconds: 45 * 60,
}); // returns a standard pg.Pool whose password is an auto-refreshing token
```

## 5. Deploy

The app is a bundle resource ([`c360_app.app.yml`](../resources/c360_app.app.yml)).
This workspace **enforces git-backed app deploys**, so the app clones the GitHub
repo and builds itself (`npm install && build:server && build:client && start`):

```bash
# 1. Load the serving tables the app reads (reverse-ETL → serving.*, grants SP SELECT)
/tmp/mcdo_venv/bin/python scripts/load_serving_lakebase.py
# 2. Push app source to the repo's main branch (git-backed deploy needs it there)
git push origin main
# 3. Roll the running app to the new commit — the deploy request carries only the
#    git REF + source_code_path; the repo url/provider live at the app level.
databricks apps deploy mcdo-ph-c360 -p fevm-dante-classic-stable \
  --json '{"git_source": {"branch": "main", "source_code_path": "app"}, "mode": "SNAPSHOT"}'
```

The app SP creates and owns the `app.*` write-back schema on first startup
(`initAppSchema`).

## 6. What to show the audience

1. **Search** a member (filter by status tier) → instant profile load (Postgres
   point lookup).
2. The **Membership 360**: points balance, **held vs qualified tier** with a
   tier-status chip, redemption rate, points liability, lifetime earned/redeemed —
   all served from Lakebase.
3. **Take an action**: **adjust points** (goodwill credit / clawback) or **redeem
   a reward** on the member's behalf → the write lands in Postgres immediately
   (read-after-write) and shows in the member's recent-actions feed.
4. Open the **Rewards Catalog** page to see redemption volume and peso value
   delivered per reward.
5. Explain the round-trip: Delta Gold → reverse-ETL → `serving.*` app reads; app
   writes → `app.*` → (optional Lakehouse Sync) → back to Delta for analytics on
   agent actions.

**Back to:** [Workshop README](../README.md)
