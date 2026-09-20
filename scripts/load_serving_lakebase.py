#!/usr/bin/env python3
"""Reverse-ETL: load Gold marts into Lakebase for the app's fast reads.

Managed Lakebase *synced tables* are the preferred path, but they require
CREATE CATALOG on the metastore to register the Postgres DB as a UC catalog.
Where that privilege isn't available, this script does the equivalent as a
one-time (re-runnable) load: read `gold.customer_360` / `gold.store_performance`
via a SQL warehouse and write them into the Lakebase `serving` schema, then
grant the app's Service Principal SELECT.

Run locally (uses the CLI profile for both the warehouse token and the Lakebase
OAuth credential):
    /tmp/mcdo_venv/bin/python scripts/load_serving_lakebase.py
"""
import json
import os
import subprocess
import psycopg
from databricks import sql as dbsql

PROFILE = os.environ.get("PROFILE", "fevm-dante-classic-stable")
WS_HOST = "fevm-dante-classic-stable.cloud.databricks.com"
WAREHOUSE_HTTP = "/sql/1.0/warehouses/751d12396d67eb55"
CATALOG = "dante_classic_stable_catalog"
GOLD = "mcdo_ph_gold"
LB_HOST = "ep-divine-waterfall-d29cawrn.database.us-east-1.cloud.databricks.com"
ENDPOINT = "projects/mcdo-ph-c360/branches/production/endpoints/primary"
PG_DB = "databricks_postgres"
PG_USER = os.environ.get("PG_USER", "dante.liew@databricks.com")
APP_SP = os.environ.get("APP_SP", "ed2a09a0-19f3-46db-a6c6-8ee30a0f2824")
SERVING = "serving"

TABLES = {"customer_360": "customer_id", "store_performance": "store_id",
          "reward_performance": "reward_id"}

# Spark/DBSQL type_code -> Postgres type
PG_TYPE = {
    "STRING": "text", "INT": "integer", "BIGINT": "bigint", "SMALLINT": "smallint",
    "DOUBLE": "double precision", "FLOAT": "real", "DECIMAL": "numeric",
    "BOOLEAN": "boolean", "DATE": "date", "TIMESTAMP": "timestamptz",
    "TIMESTAMP_NTZ": "timestamp",
}


def cli_json(args):
    return json.loads(subprocess.check_output(["databricks", *args, "-p", PROFILE, "-o", "json"]))


def resolve(host):
    return subprocess.check_output(["dig", "+short", host]).decode().strip().splitlines()[-1]


def pg_type(type_code: str) -> str:
    return PG_TYPE.get(str(type_code).upper().split("(")[0], "text")


def main():
    lb_token = cli_json(["postgres", "generate-database-credential", ENDPOINT])["token"]
    ws_token = cli_json(["auth", "token"])["access_token"]
    hostaddr = resolve(LB_HOST)
    print(f"Lakebase {LB_HOST} -> {hostaddr}")

    pg = psycopg.connect(host=LB_HOST, hostaddr=hostaddr, dbname=PG_DB,
                         user=PG_USER, password=lb_token, sslmode="require", autocommit=True)
    pg.execute(f'CREATE SCHEMA IF NOT EXISTS "{SERVING}"')

    with dbsql.connect(server_hostname=WS_HOST, http_path=WAREHOUSE_HTTP, access_token=ws_token) as wc:
        for table, pk in TABLES.items():
            cur = wc.cursor()
            cur.execute(f"SELECT * FROM {CATALOG}.{GOLD}.{table}")
            desc = cur.description  # [(name, type_code, ...), ...]
            cols = [d[0] for d in desc]
            coldefs = ", ".join(f'"{d[0]}" {pg_type(d[1])}' for d in desc)
            rows = cur.fetchall()

            pg.execute(f'DROP TABLE IF EXISTS "{SERVING}"."{table}"')
            pg.execute(f'CREATE TABLE "{SERVING}"."{table}" ({coldefs}, PRIMARY KEY ("{pk}"))')
            placeholders = ", ".join(["%s"] * len(cols))
            collist = ", ".join(f'"{c}"' for c in cols)
            with pg.cursor() as ins:
                ins.executemany(
                    f'INSERT INTO "{SERVING}"."{table}" ({collist}) VALUES ({placeholders})',
                    [tuple(r) for r in rows],
                )
            # Helpful secondary indexes for the app's filters/search.
            if table == "customer_360":
                pg.execute(f'CREATE INDEX IF NOT EXISTS ix_c360_region ON "{SERVING}"."{table}" (region)')
                pg.execute(f'CREATE INDEX IF NOT EXISTS ix_c360_segment ON "{SERVING}"."{table}" (rfm_segment)')
                pg.execute(f'CREATE INDEX IF NOT EXISTS ix_c360_name ON "{SERVING}"."{table}" (full_name)')
                pg.execute(f'CREATE INDEX IF NOT EXISTS ix_c360_tier ON "{SERVING}"."{table}" (current_tier)')
            print(f"✓ {SERVING}.{table}: {len(rows)} rows, {len(cols)} cols")

    # Grant the app Service Principal read access to the serving schema.
    pg.execute(f'GRANT USAGE ON SCHEMA "{SERVING}" TO "{APP_SP}"')
    pg.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{SERVING}" TO "{APP_SP}"')
    pg.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{SERVING}" GRANT SELECT ON TABLES TO "{APP_SP}"')
    print(f"✓ granted SELECT on {SERVING}.* to app SP {APP_SP}")
    pg.close()


if __name__ == "__main__":
    main()
