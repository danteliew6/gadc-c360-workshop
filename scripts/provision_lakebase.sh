#!/usr/bin/env bash
# ============================================================================
# Module 4 — provision the Lakebase serving layer (one-time).
#
# 1. Register the Lakebase Postgres database as a UC catalog.
# 2. Create Lakebase *synced tables* (Delta -> Postgres) for the app's fast
#    reads: gold.customer_360 and gold.store_performance (SNAPSHOT mode).
# 3. (After the app is deployed) grant the app's Service Principal SELECT on
#    the synced serving schema — see the printed GRANT block at the end.
#
# Run AFTER the data job has materialized the Gold tables.
#   bash scripts/provision_lakebase.sh
# ============================================================================
set -euo pipefail

PROFILE="${PROFILE:-fevm-dante-classic-stable}"
CATALOG="${CATALOG:-dante_classic_stable_catalog}"
GOLD="${GOLD:-mcdo_ph_gold}"
PROJECT="${LAKEBASE_PROJECT:-mcdo-ph-c360}"
BRANCH="projects/${PROJECT}/branches/production"
PG_DB="databricks_postgres"
LB_CATALOG="${LB_CATALOG:-mcdo_ph_lakebase}"   # UC catalog mapping to the Postgres DB
PG_SCHEMA="serving"                            # Postgres schema the app reads

echo "▶ Registering Lakebase DB as UC catalog: ${LB_CATALOG}"
databricks postgres get-catalog "${LB_CATALOG}" -p "$PROFILE" >/dev/null 2>&1 || \
databricks postgres create-catalog "${LB_CATALOG}" -p "$PROFILE" --json "{
  \"spec\": { \"postgres_database\": \"${PG_DB}\", \"branch\": \"${BRANCH}\" }
}"

create_sync() {
  local table="$1"; local pk="$2"
  echo "▶ Creating synced table ${LB_CATALOG}.${PG_SCHEMA}.${table} (SNAPSHOT)"
  databricks postgres get-synced-table "synced_tables/${LB_CATALOG}.${PG_SCHEMA}.${table}" -p "$PROFILE" >/dev/null 2>&1 && {
    echo "  already exists — refreshing"; return 0; }
  databricks postgres create-synced-table "${LB_CATALOG}.${PG_SCHEMA}.${table}" -p "$PROFILE" --json "{
    \"spec\": {
      \"source_table_full_name\": \"${CATALOG}.${GOLD}.${table}\",
      \"primary_key_columns\": ${pk},
      \"scheduling_policy\": \"SNAPSHOT\",
      \"branch\": \"${BRANCH}\",
      \"postgres_database\": \"${PG_DB}\",
      \"create_database_objects_if_missing\": true,
      \"new_pipeline_spec\": { \"storage_catalog\": \"${CATALOG}\", \"storage_schema\": \"${GOLD}\" }
    }
  }"
}

create_sync customer_360      '["customer_id"]'
create_sync store_performance '["store_id"]'

cat <<EOF

✅ Synced tables requested. Check status with:
   databricks postgres get-synced-table "synced_tables/${LB_CATALOG}.${PG_SCHEMA}.customer_360" -p ${PROFILE}

Once the app is deployed, grant its Service Principal read access. Get the SP:
   SP=\$(databricks apps get mcdo-ph-c360 -p ${PROFILE} -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['service_principal_client_id'])")
Then connect to Postgres (see the databricks-lakebase skill for the psql one-liner) and run:
   GRANT USAGE ON SCHEMA ${PG_SCHEMA} TO "\$SP";
   GRANT SELECT ON ALL TABLES IN SCHEMA ${PG_SCHEMA} TO "\$SP";
   ALTER DEFAULT PRIVILEGES IN SCHEMA ${PG_SCHEMA} GRANT SELECT ON TABLES TO "\$SP";
EOF
