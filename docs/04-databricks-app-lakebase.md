# Module 4 · Databricks App on Lakebase (fast reads + write-back)

**Goal:** an operational **Customer Care & Loyalty Console** for McDonald's PH
store-ops / CRM agents. It serves the **Customer 360** at OLTP speed from
**Lakebase** (managed Postgres) and writes operational actions — vouchers, service
tickets, tier changes, notes — **back** to Lakebase. This is the "activation"
layer that closes the loop from analytics to action.

App code: [`app/`](../app) · Provisioning: [`scripts/provision_lakebase.sh`](../scripts/provision_lakebase.sh)

---

## 1. Why Lakebase for an app

The Gold `customer_360` lives in Delta — great for analytics, wrong shape for a
transactional app that does per-customer lookups and writes on every click.
**Lakebase** is serverless Postgres integrated with the lakehouse:

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
 gold.customer_360  ──synced table──►     serving.customer_360   (read-only, app SP: SELECT)
 gold.store_performance ──synced──────►   serving.store_performance
                                          app.vouchers        ┐
 app SP owns + writes ────────────────►   app.service_tickets ├─ OLTP write-back
                                          app.tier_changes    │   (app SP: OWNER)
                                          app.notes           ┘
```

- **`serving.*`** — the Gold marts materialized into Lakebase for OLTP reads.
  The **preferred** path is managed Lakebase **synced tables**
  (`scripts/provision_lakebase.sh`, SNAPSHOT mode), but that registers the
  Postgres DB as a UC catalog and needs `CREATE CATALOG` on the metastore. Where
  that privilege isn't available (as in this FE workspace), use the
  **reverse-ETL** equivalent `scripts/load_serving_lakebase.py`, which reads Gold
  via a SQL warehouse and loads `serving.customer_360` / `serving.store_performance`
  into Postgres, then grants the app SP `SELECT`. Either way the tables are
  read-only to the app.
- **`app.*`** — write-back tables **owned by the app's Service Principal**, created
  on first startup. The console inserts here.

> Synced tables are read-only in Postgres — never write to `serving.*`. All app
> writes go to the SP-owned `app.*` schema.

## 3. Tech stack (AppKit)

React + TypeScript + Vite front end, `@databricks/appkit` Express server, `pg`
connection pool — the same proven stack as our other Lakebase apps:

- **`server/lakebase.ts`** — mints & **refreshes** the Lakebase OAuth token (they
  expire ~1h) with the Databricks SDK and hands `pg` a *password function*, so
  every new connection opens with a valid token. Parameterized queries only.
- **`server/server.ts`** — read routes over `serving.*` + write routes into
  `app.*` (search member, get 360 profile, list/create voucher, open ticket,
  change tier, add note).
- **`client/src/pages/`** — Member Search, Customer 360 profile (RFM, CLV, churn,
  favorites, order history), and the action panels.

## 4. Connection & auth

The `postgres` resource binding on the app injects `PGHOST` / `PGPORT` /
`PGDATABASE` / `PGUSER`; the app mints tokens itself:

```ts
const cred = await client().database.generateDatabaseCredential({
  request_id: randomUUID(), instance_names: [PROJECT] });
// pg pool: password is a function → fresh token per physical connection
pool = new Pool({ host, database, user, password: getToken,
                  ssl: { rejectUnauthorized: false }, maxLifetimeSeconds: 45*60 });
```

## 5. Deploy

The app is a bundle resource ([`c360_app.app.yml`](../resources/c360_app.app.yml)).
**Deploy before running locally** so the SP creates and owns the `app.*` schema:

```bash
# 1. Ensure Gold exists + provision serving synced tables
bash scripts/provision_lakebase.sh
# 2. Deploy + start the app
databricks bundle deploy -t dev -p fevm-dante-classic-stable
databricks apps deploy mcdo-ph-c360 -p fevm-dante-classic-stable
# 3. Grant the app SP SELECT on serving.*  (see script output)
```

## 6. What to show the audience

1. **Search** a member → instant profile load (Postgres point lookup).
2. The **Customer 360**: tier, RFM segment, CLV, churn risk, favorite
   store/category, order history — all served from Lakebase.
3. **Take an action**: issue a "PISOFRIES" voucher or open a service ticket →
   write lands in Postgres immediately (read-after-write).
4. Explain the round-trip: Delta → synced tables → app reads; app writes →
   (optional Lakehouse Sync) → back to Delta for analytics on agent actions.

**Back to:** [Workshop README](../README.md)
