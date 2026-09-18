/**
 * Lakebase (Autoscaling Postgres) connection for the McDonald's PH Customer 360
 * console.
 *
 * READS come from the `serving` schema — Lakebase synced tables reverse-ETL'd
 * from Unity Catalog (customer_360, store_performance). WRITES go to the `app`
 * schema, which the app service principal owns and creates at startup
 * (vouchers, service_tickets, tier_changes, notes). The app NEVER writes to
 * `serving`.
 *
 * Auth: this is an *Autoscaling* Lakebase project, so credentials are minted per
 * endpoint (projects/<p>/branches/<b>/endpoints/<e>) rather than per legacy
 * Database Instance. `@databricks/lakebase`'s `createLakebasePool` handles the
 * endpoint-based `generate-database-credential` call AND the ~1h token refresh
 * for us: it returns a standard `pg.Pool` whose password is a callback that
 * always yields a currently-valid OAuth token. In the hosted app the token is
 * minted as the app SP (bound via the `postgres` app resource); locally it uses
 * the named CLI profile.
 */
import type { Pool, QueryResultRow } from 'pg';
import { createLakebasePool } from '@databricks/lakebase';
import { WorkspaceClient } from '@databricks/sdk-experimental';

// Endpoint resource path for the Autoscaling credential call. Overridable via
// env so the same code runs against any branch/endpoint.
const ENDPOINT =
  process.env.LAKEBASE_ENDPOINT ?? 'projects/mcdo-ph-c360/branches/production/endpoints/primary';
const PGHOST =
  process.env.PGHOST ?? 'ep-divine-waterfall-d29cawrn.database.us-east-1.cloud.databricks.com';
const PGDATABASE = process.env.PGDATABASE ?? 'databricks_postgres';

// Read-only synced tables live here; the app's own write-back tables live in
// APP_SCHEMA (owned by the app SP).
const SERVING_SCHEMA = process.env.LAKEBASE_SERVING_SCHEMA ?? 'serving';
const APP_SCHEMA = process.env.LAKEBASE_APP_SCHEMA ?? 'app';

// Recycle connections well inside a token's ~1h lifetime.
const MAX_LIFETIME_SECONDS = 45 * 60;

// Strict identifier guard: schema/table names that reach a query string are
// validated first (defence against identifier injection).
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
function safeIdentifier(value: string, what: string): string {
  if (!IDENTIFIER_RE.test(value)) {
    throw new Error(`Invalid Lakebase ${what} ${JSON.stringify(value)}: must match ${IDENTIFIER_RE}`);
  }
  return value;
}

/** Quoted `"serving"."<name>"` identifier (read-only). */
export function servingTable(name: string): string {
  const schema = safeIdentifier(SERVING_SCHEMA, 'serving schema');
  const table = safeIdentifier(name, 'serving table name');
  return `"${schema}"."${table}"`;
}

/** Quoted `"app"."<name>"` identifier (write-back, app-owned). */
export function appTable(name: string): string {
  const schema = safeIdentifier(APP_SCHEMA, 'app schema');
  const table = safeIdentifier(name, 'app table name');
  return `"${schema}"."${table}"`;
}

function workspaceClient(): WorkspaceClient | undefined {
  // Hosted: the app SP's OAuth creds are injected via env (DATABRICKS_HOST +
  // client id/secret), so the default auth chain works — return undefined and
  // let the pool build its own client. Local: use the named CLI profile.
  const profile = process.env.DATABRICKS_CONFIG_PROFILE;
  return profile ? new WorkspaceClient({ profile }) : undefined;
}

let pool: Pool | null = null;
export function getPool(): Pool {
  if (pool) return pool;
  pool = createLakebasePool({
    endpoint: ENDPOINT,
    host: PGHOST,
    database: PGDATABASE,
    user: process.env.PGUSER,
    workspaceClient: workspaceClient(),
    ssl: { rejectUnauthorized: false },
    max: 8,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 15_000,
    maxLifetimeSeconds: MAX_LIFETIME_SECONDS,
  });
  pool.on('error', (err) => console.error('[lakebase] idle client error', err));
  return pool;
}

/** Run a parameterized query and return its rows. */
export async function query<T extends QueryResultRow = QueryResultRow>(
  sql: string,
  params: unknown[] = []
): Promise<T[]> {
  const result = await getPool().query<T>(sql, params);
  return result.rows;
}

/**
 * Create the app-owned write-back schema + tables if they don't exist. Called
 * once at startup. Guarded so a hosted app that can't yet reach Lakebase logs
 * and continues (reads/writes then surface a clear error per-request).
 */
export async function initAppSchema(): Promise<void> {
  const schema = safeIdentifier(APP_SCHEMA, 'app schema');
  await query(`CREATE SCHEMA IF NOT EXISTS "${schema}"`);
  await query(
    `CREATE TABLE IF NOT EXISTS ${appTable('vouchers')} (
       id serial PRIMARY KEY,
       customer_id text NOT NULL,
       code text NOT NULL,
       amount numeric,
       note text,
       created_by text,
       created_at timestamptz NOT NULL DEFAULT now()
     )`
  );
  await query(
    `CREATE TABLE IF NOT EXISTS ${appTable('service_tickets')} (
       id serial PRIMARY KEY,
       customer_id text NOT NULL,
       category text,
       priority text,
       subject text,
       status text NOT NULL DEFAULT 'OPEN',
       created_at timestamptz NOT NULL DEFAULT now()
     )`
  );
  await query(
    `CREATE TABLE IF NOT EXISTS ${appTable('tier_changes')} (
       id serial PRIMARY KEY,
       customer_id text NOT NULL,
       old_tier text,
       new_tier text,
       reason text,
       created_at timestamptz NOT NULL DEFAULT now()
     )`
  );
  await query(
    `CREATE TABLE IF NOT EXISTS ${appTable('notes')} (
       id serial PRIMARY KEY,
       customer_id text NOT NULL,
       body text NOT NULL,
       author text,
       created_at timestamptz NOT NULL DEFAULT now()
     )`
  );
}
