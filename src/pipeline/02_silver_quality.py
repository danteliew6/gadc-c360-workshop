"""Module 2 · Silver — clean & conform with data-quality expectations.

Lakeflow Spark Declarative Pipeline (SDP). Reads the Bronze Delta tables that
Module 1's Auto Loader job produced and applies:

* **Expectations** in all three enforcement modes:
  - `expect_all`          → warn only (violations counted in the event log)
  - `expect_all_or_drop`  → drop offending rows
  - a **quarantine** streaming table that captures the dropped rows for audit
* **Auto CDC (SCD Type 2)** to build a slowly-changing `dim_customer` that keeps
  full history of loyalty-tier / profile changes (`__START_AT` / `__END_AT`).

Pipeline publishes to the Silver schema by default (set in the pipeline config).
Parameters come from the pipeline `configuration` block via `spark.conf`.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("workshop.catalog")
BRONZE = spark.conf.get("workshop.bronze_schema")


def bronze(tbl: str) -> str:
    return f"{CATALOG}.{BRONZE}.{tbl}"


# ---------------------------------------------------------------------------
# ORDERS  — critical rules drop the row; soft rules warn. Dropped rows are
# re-derived into a quarantine table so nothing is lost silently.
# ---------------------------------------------------------------------------
ORDER_RULES_CRITICAL = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "positive_net_amount": "net_amount > 0",
    "known_channel": "channel IN ('DriveThru','CounterDineIn','KioskDineIn','McDelivery','InAppPickup')",
}
ORDER_RULES_WARN = {
    "discount_not_exceeding_gross": "discount_amount <= gross_amount",
    "known_status": "order_status IN ('COMPLETED','CANCELLED')",
    "delivery_has_partner": "channel <> 'McDelivery' OR delivery_partner IS NOT NULL",
}


@dp.materialized_view(
    comment="Cleaned, typed orders. Critical rules drop; soft rules warn.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["order_date", "region_hint"],
)
@dp.expect_all(ORDER_RULES_WARN)
@dp.expect_all_or_drop(ORDER_RULES_CRITICAL)
def orders_clean():
    return (
        spark.read.table(bronze("orders_raw"))
        # Auto Loader is at-least-once; dedupe on the natural key so silver is
        # idempotent regardless of duplicate source files.
        .dropDuplicates(["order_id"])
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("gross_amount", F.col("gross_amount").cast("decimal(12,2)"))
        .withColumn("discount_amount", F.coalesce(F.col("discount_amount").cast("decimal(12,2)"), F.lit(0)))
        .withColumn("net_amount", F.col("net_amount").cast("decimal(12,2)"))
        .withColumn("region_hint", F.substring("store_id", 1, 4))
        .drop("_rescued_data", "_ingest_batch")
    )


@dp.materialized_view(
    comment="Quarantine: orders that failed a CRITICAL rule, kept for audit.",
    table_properties={"quality": "quarantine"},
)
def orders_quarantine():
    fail_expr = " OR ".join(f"NOT ({c})" for c in ORDER_RULES_CRITICAL.values())
    return (
        spark.read.table(bronze("orders_raw"))
        .withColumn("_failed_rules", F.expr(
            "concat_ws(',', " + ",".join(
                f"CASE WHEN NOT ({c}) THEN '{name}' END" for name, c in ORDER_RULES_CRITICAL.items()
            ) + ")"))
        .filter(F.expr(fail_expr))
    )


# ---------------------------------------------------------------------------
# ORDER ITEMS
# ---------------------------------------------------------------------------
@dp.materialized_view(
    comment="Cleaned order line items.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product": "product_id IS NOT NULL",
    "positive_quantity": "quantity > 0",
    "non_negative_amount": "line_amount >= 0",
})
def order_items_clean():
    return (
        spark.read.table(bronze("order_items_raw"))
        .dropDuplicates(["order_id", "line_no"])
        .withColumn("line_amount", F.col("line_amount").cast("decimal(12,2)"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
        .drop("_rescued_data", "_ingest_batch")
    )


# ---------------------------------------------------------------------------
# APP EVENTS
# ---------------------------------------------------------------------------
@dp.materialized_view(
    comment="Cleaned app engagement events.",
    table_properties={"quality": "silver"},
    cluster_by=["event_date"],
)
@dp.expect_all_or_drop({
    "valid_customer": "customer_id IS NOT NULL",
    "known_event_type": "event_type IN ('app_open','menu_view','add_to_cart','checkout',"
                        "'reward_redeem','push_received','push_opened','coupon_view')",
})
def app_events_clean():
    return (
        spark.read.table(bronze("app_events_raw"))
        .dropDuplicates(["event_id"])
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .drop("_rescued_data", "_ingest_batch")
    )


# ---------------------------------------------------------------------------
# POINTS LEDGER  — the MyMcDonald's Rewards points economy. Critical rules drop
# (and quarantine) malformed rows; a soft rule warns on the sign convention.
# ---------------------------------------------------------------------------
LEDGER_RULES_CRITICAL = {
    "valid_ledger_id": "ledger_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "known_txn_type": "txn_type IN ('EARN','REDEEM','EXPIRE','BONUS','ADJUST')",
    "points_present": "points IS NOT NULL",
}
LEDGER_RULES_WARN = {
    # Earn/bonus add points; redeem/expire remove them — flag sign violations.
    "earn_is_positive": "txn_type NOT IN ('EARN','BONUS') OR points > 0",
    "redeem_is_negative": "txn_type NOT IN ('REDEEM','EXPIRE') OR points < 0",
    "redeem_has_reward": "txn_type <> 'REDEEM' OR reward_id IS NOT NULL",
}


@dp.materialized_view(
    comment="Cleaned points ledger (earn/redeem/expire/bonus). Critical rules drop; sign rules warn.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    cluster_by=["txn_date", "txn_type"],
)
@dp.expect_all(LEDGER_RULES_WARN)
@dp.expect_all_or_drop(LEDGER_RULES_CRITICAL)
def points_ledger_clean():
    return (
        spark.read.table(bronze("points_ledger_raw"))
        .dropDuplicates(["ledger_id"])
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .withColumn("txn_date", F.to_date("txn_ts"))
        .withColumn("points", F.col("points").cast("int"))
        .drop("_rescued_data", "_ingest_batch")
    )


@dp.materialized_view(
    comment="Quarantine: ledger rows that failed a CRITICAL rule, kept for audit.",
    table_properties={"quality": "quarantine"},
)
def points_ledger_quarantine():
    fail_expr = " OR ".join(f"NOT ({c})" for c in LEDGER_RULES_CRITICAL.values())
    return (
        spark.read.table(bronze("points_ledger_raw"))
        .withColumn("_failed_rules", F.expr(
            "concat_ws(',', " + ",".join(
                f"CASE WHEN NOT ({c}) THEN '{name}' END" for name, c in LEDGER_RULES_CRITICAL.items()
            ) + ")"))
        .filter(F.expr(fail_expr))
    )


# ---------------------------------------------------------------------------
# CUSTOMER dimension — SCD Type 2 via Auto CDC.
# A cleaned streaming view feeds an SCD2 target so loyalty-tier / profile
# changes are tracked with __START_AT / __END_AT history.
# ---------------------------------------------------------------------------
@dp.temporary_view(comment="Cleaned customer records feeding the SCD2 flow.")
@dp.expect_all({"valid_email_format": "email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'"})
@dp.expect_all_or_drop({
    "valid_customer_id": "customer_id IS NOT NULL",
    "signup_not_in_future": "signup_date <= current_date()",
})
def customers_clean():
    # Keep _source_file / _ingest_batch here — the SCD2 flow's
    # except_column_list excludes them from dim_customer downstream.
    return (
        spark.readStream.table(bronze("customers_raw"))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("birth_date", F.to_date("birth_date"))
        .drop("_rescued_data")
    )


dp.create_streaming_table(
    name="dim_customer",
    comment="SCD Type 2 customer dimension (full profile/tier history).",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="dim_customer",
    source="customers_clean",
    keys=["customer_id"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
    except_column_list=["_source_file", "_ingest_batch"],
)
