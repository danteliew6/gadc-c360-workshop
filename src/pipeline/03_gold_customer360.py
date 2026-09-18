"""Module 2 · Gold — curated Customer 360 + business marts.

Materialized views published to the Gold schema (fully-qualified names). The
centerpiece is `customer_360`: one row per member enriched with RFM scores,
CLV estimate, churn risk, favorite store / category / channel, and app
engagement. Supporting marts power the AI/BI dashboard (Module 3) and the
Lakebase-backed app (Module 4).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window as W

CATALOG = spark.conf.get("workshop.catalog")
BRONZE = spark.conf.get("workshop.bronze_schema")
GOLD = spark.conf.get("workshop.gold_schema")


def gold(tbl: str) -> str:
    return f"{CATALOG}.{GOLD}.{tbl}"


def bronze(tbl: str) -> str:
    return f"{CATALOG}.{BRONZE}.{tbl}"


# ---------------------------------------------------------------------------
# Intermediate (pipeline-private) rollups
# ---------------------------------------------------------------------------
@dp.temporary_view
def _orders_enriched():
    """Completed orders joined to store + current customer region."""
    stores = spark.read.table(bronze("dim_store")).select(
        "store_id", "store_name", "region", "province", "city", "store_format")
    return (
        spark.read.table("orders_clean")
        .filter(F.col("order_status") == "COMPLETED")
        .join(stores, "store_id", "left")
    )


@dp.temporary_view
def _cust_order_stats():
    return (
        spark.read.table("_orders_enriched").groupBy("customer_id").agg(
            F.count("*").alias("total_orders"),
            F.sum("net_amount").alias("total_spend"),
            F.avg("net_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("store_id").alias("distinct_stores"),
            F.sum(F.when(F.col("channel") == "McDelivery", 1).otherwise(0)).alias("delivery_orders"),
            F.sum(F.when(F.col("promo_code").isNotNull(), 1).otherwise(0)).alias("promo_orders"),
        )
    )


def _top_by(group_col, value_col, alias):
    """argmax helper — favorite `group_col` per customer by summed `value_col`."""
    ranked = (
        spark.read.table("_orders_enriched")
        .groupBy("customer_id", group_col).agg(F.sum(value_col).alias("v"))
        .withColumn("rn", F.row_number().over(
            W.partitionBy("customer_id").orderBy(F.desc("v"))))
        .filter(F.col("rn") == 1)
        .select("customer_id", F.col(group_col).alias(alias))
    )
    return ranked


@dp.temporary_view
def _cust_fav_store():
    return _top_by("store_name", "net_amount", "favorite_store")


@dp.temporary_view
def _cust_fav_channel():
    return _top_by("channel", "net_amount", "preferred_channel")


@dp.temporary_view
def _cust_fav_category():
    items = spark.read.table("order_items_clean")
    orders = spark.read.table("orders_clean").select("order_id", "customer_id", "order_status")
    prod = spark.read.table(bronze("dim_product")).select("product_id", "category")
    joined = (
        items.join(orders, "order_id").filter(F.col("order_status") == "COMPLETED")
        .join(prod, "product_id", "left")
        .groupBy("customer_id", "category").agg(F.sum("line_amount").alias("v"))
        .withColumn("rn", F.row_number().over(
            W.partitionBy("customer_id").orderBy(F.desc("v"))))
        .filter(F.col("rn") == 1)
        .select("customer_id", F.col("category").alias("favorite_category"))
    )
    return joined


@dp.temporary_view
def _cust_engagement():
    return (
        spark.read.table("app_events_clean").groupBy("customer_id").agg(
            F.count("*").alias("total_app_events"),
            F.sum(F.when(F.col("event_type") == "reward_redeem", 1).otherwise(0)).alias("rewards_redeemed"),
            F.countDistinct("event_date").alias("active_days"),
            F.max("event_date").alias("last_active_date"),
        )
    )


# ---------------------------------------------------------------------------
# GOLD: customer_360
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("customer_360"),
    comment="One row per loyalty member: profile + RFM + CLV + churn + favorites + engagement.",
    table_properties={"quality": "gold"},
    # Cluster on early columns — Delta collects stats on the first 32 columns,
    # and the derived rfm_segment sits beyond that window.
    cluster_by=["region", "loyalty_tier"],
)
def customer_360():
    cust = (
        spark.read.table("dim_customer").filter(F.col("__END_AT").isNull())
        .select("customer_id", "first_name", "last_name", "email", "mobile", "gender",
                "birth_date", "region", "province", "city", "signup_date", "signup_channel",
                "loyalty_tier", "preferred_language", "marketing_consent")
    )
    o = spark.read.table("_cust_order_stats")
    df = (
        cust
        .join(o, "customer_id", "left")
        .join(spark.read.table("_cust_fav_store"), "customer_id", "left")
        .join(spark.read.table("_cust_fav_channel"), "customer_id", "left")
        .join(spark.read.table("_cust_fav_category"), "customer_id", "left")
        .join(spark.read.table("_cust_engagement"), "customer_id", "left")
        .withColumn("total_orders", F.coalesce("total_orders", F.lit(0)))
        .withColumn("total_spend", F.coalesce("total_spend", F.lit(0).cast("decimal(14,2)")))
        .withColumn("recency_days", F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn("tenure_days", F.datediff(F.current_date(), F.col("signup_date")))
        .withColumn("delivery_share",
                    F.round(F.col("delivery_orders") / F.nullif(F.col("total_orders"), F.lit(0)), 3))
    )

    # RFM quintiles (5 = best). Recency is inverted (fewer days = better).
    df = (
        df
        .withColumn("r_score", F.when(F.col("recency_days").isNull(), F.lit(1))
                    .otherwise(F.lit(6) - F.ntile(5).over(W.orderBy(F.asc_nulls_last("recency_days")))))
        .withColumn("f_score", F.ntile(5).over(W.orderBy(F.asc("total_orders"))))
        .withColumn("m_score", F.ntile(5).over(W.orderBy(F.asc("total_spend"))))
    )
    df = df.withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))

    df = (
        df
        .withColumn("rfm_segment", F.when(F.col("total_orders") == 0, "Prospect")
                    .when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "Champion")
                    .when((F.col("r_score") >= 4) & (F.col("f_score") < 4), "Promising")
                    .when((F.col("r_score") == 3), "Needs Attention")
                    .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4), "At Risk")
                    .otherwise("Hibernating"))
        .withColumn("churn_risk", F.when(F.col("total_orders") == 0, F.lit("N/A — Prospect"))
                    .when(F.col("recency_days") > 60, F.lit("High"))
                    .when(F.col("recency_days") > 30, F.lit("Medium"))
                    .otherwise(F.lit("Low")))
        # Simple annualized CLV projection from realized spend over tenure.
        .withColumn("clv_estimate", F.round(
            F.when(F.col("tenure_days") > 30,
                   F.col("total_spend") / F.col("tenure_days") * 365 * 2)
            .otherwise(F.col("total_spend")), 2))
        .withColumn("engagement_score", F.round(
            F.coalesce(F.col("active_days"), F.lit(0)) * 2
            + F.coalesce(F.col("rewards_redeemed"), F.lit(0)) * 5
            + F.coalesce(F.col("total_orders"), F.lit(0)) * 3, 0))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("age", (F.datediff(F.current_date(), F.col("birth_date")) / 365).cast("int"))
        .withColumn("_refreshed_at", F.current_timestamp())
    )
    return df


# ---------------------------------------------------------------------------
# GOLD: store_performance
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("store_performance"),
    comment="Per-store sales, customers, AOV, channel & McCafé attach.",
    table_properties={"quality": "gold"},
)
def store_performance():
    o = spark.read.table("_orders_enriched")
    stores = spark.read.table(bronze("dim_store"))
    agg = o.groupBy("store_id").agg(
        F.count("*").alias("orders"),
        F.sum("net_amount").alias("revenue"),
        F.avg("net_amount").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum(F.when(F.col("channel") == "DriveThru", 1).otherwise(0)).alias("drive_thru_orders"),
        F.sum(F.when(F.col("channel") == "McDelivery", 1).otherwise(0)).alias("delivery_orders"),
    )
    return (
        stores.join(agg, "store_id", "left")
        .withColumn("revenue", F.coalesce("revenue", F.lit(0).cast("decimal(16,2)")))
        .withColumn("drive_thru_share", F.round(F.col("drive_thru_orders") / F.nullif(F.col("orders"), F.lit(0)), 3))
        .withColumn("revenue_per_customer", F.round(F.col("revenue") / F.nullif(F.col("unique_customers"), F.lit(0)), 2))
    )


# ---------------------------------------------------------------------------
# GOLD: daily_sales  (date x region x channel)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("daily_sales"),
    comment="Daily sales by region and channel for trend analysis.",
    table_properties={"quality": "gold"},
    cluster_by=["order_date"],
)
def daily_sales():
    return (
        spark.read.table("_orders_enriched")
        .groupBy("order_date", "region", "channel").agg(
            F.count("*").alias("orders"),
            F.sum("net_amount").alias("revenue"),
            F.countDistinct("customer_id").alias("active_customers"),
            F.avg("net_amount").alias("avg_order_value"),
            F.sum("discount_amount").alias("promo_discount"),
        )
    )


# ---------------------------------------------------------------------------
# GOLD: category_mix
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("category_mix"),
    comment="Revenue and units by menu category and region.",
    table_properties={"quality": "gold"},
)
def category_mix():
    items = spark.read.table("order_items_clean")
    stores = spark.read.table(bronze("dim_store")).select("store_id", "region")
    orders = (
        spark.read.table("orders_clean")
        .filter(F.col("order_status") == "COMPLETED")
        .join(stores, "store_id", "left")
        .select("order_id", "region", "order_date")
    )
    prod = spark.read.table(bronze("dim_product")).select("product_id", "category", "is_localized")
    return (
        items.join(orders, "order_id").join(prod, "product_id", "left")
        .groupBy("region", "category", "is_localized").agg(
            F.sum("line_amount").alias("revenue"),
            F.sum("quantity").alias("units"),
            F.countDistinct("order_id").alias("orders"),
        )
    )
