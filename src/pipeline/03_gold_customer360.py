"""Module 2 · Gold — curated Member 360 + MyMcDonald's Rewards marts.

Materialized views published to the Gold schema (fully-qualified names). The
centerpiece is `customer_360`: one row per member enriched with the loyalty
**points economy** (balance, lifetime earned/redeemed/expired, redemption rate,
qualified status tier, points liability) plus RFM scores, CLV estimate, churn
risk, and favorites. Supporting marts model the rewards program itself:

* `points_economy`   — daily earn/redeem/expire and running outstanding balance.
* `reward_performance` — per-reward redemptions, points spent, and peso value.
* `tier_migration`   — held-tier × qualified-tier transition matrix.

These power the AI/BI dashboard (Module 3) and the Lakebase-backed app (Module 4).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window as W

CATALOG = spark.conf.get("workshop.catalog")
BRONZE = spark.conf.get("workshop.bronze_schema")
GOLD = spark.conf.get("workshop.gold_schema")

# Blended peso value of one point — used for the outstanding points-liability
# estimate. Mirrors POINT_VALUE_PHP in the data generator.
POINT_VALUE_PHP = 0.04
# Rolling-12mo earned-point thresholds that qualify a member for each status tier
# (must match dim_tier / TIER_DEF in the generator).
TIER_THRESHOLDS = [("Platinum", 4, 40000), ("Gold", 3, 15000), ("Silver", 2, 5000), ("Member", 1, 0)]


def _tier_col(earned, want_rank=False):
    """Map a rolling-12mo earned-points column to its qualified status tier
    (name, or rank 1-4 when want_rank). Nested CASE, highest threshold wins."""
    out = F.lit(1 if want_rank else "Member")
    for name, rank, thresh in reversed(TIER_THRESHOLDS[:-1]):
        out = F.when(earned >= thresh, F.lit(rank if want_rank else name)).otherwise(out)
    return out


def _rank_name(rank_col):
    """Map a status-tier rank (1-4) back to its tier name."""
    return (F.when(rank_col == 4, "Platinum").when(rank_col == 3, "Gold")
            .when(rank_col == 2, "Silver").otherwise("Member"))


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


@dp.temporary_view
def _member_points():
    """Per-member MyMcDonald's Rewards points rollup from the ledger."""
    earn_types = F.col("txn_type").isin("EARN", "BONUS")
    yr = F.expr("current_timestamp() - interval 365 days")
    return (
        spark.read.table("points_ledger_clean").groupBy("customer_id").agg(
            F.sum("points").alias("_net_points"),
            F.sum(F.when(earn_types, F.col("points")).otherwise(0)).alias("lifetime_points_earned"),
            F.sum(F.when(F.col("txn_type") == "REDEEM", -F.col("points")).otherwise(0)).alias("lifetime_points_redeemed"),
            F.sum(F.when(F.col("txn_type") == "EXPIRE", -F.col("points")).otherwise(0)).alias("points_expired"),
            F.sum(F.when(F.col("txn_type") == "BONUS", F.col("points")).otherwise(0)).alias("bonus_points"),
            F.sum(F.when(earn_types & (F.col("txn_ts") >= yr), F.col("points")).otherwise(0)).alias("points_earned_12mo"),
            F.sum(F.when(F.col("txn_type") == "REDEEM", 1).otherwise(0)).alias("redemptions_count"),
            F.max(F.when(F.col("txn_type") == "REDEEM", F.col("txn_date"))).alias("last_redeem_date"),
            F.max(F.when(earn_types, F.col("txn_date"))).alias("last_earn_date"),
        )
    )


# ---------------------------------------------------------------------------
# GOLD: customer_360
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("customer_360"),
    comment="One row per loyalty member: profile + RFM + CLV + churn + favorites + engagement.",
    table_properties={"quality": "gold"},
    # Cluster on `region` only: Delta collects stats on the first 32 columns and
    # the derived tier/points columns are appended well beyond that window, so
    # they can't be clustering keys. Tier filtering is served by the metric view
    # and the Lakebase `current_tier` index, not Gold clustering.
    cluster_by=["region"],
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
        .join(spark.read.table("_member_points"), "customer_id", "left")
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
            + F.coalesce(F.col("redemptions_count"), F.lit(0)) * 5
            + F.coalesce(F.col("total_orders"), F.lit(0)) * 3, 0))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("age", (F.datediff(F.current_date(), F.col("birth_date")) / 365).cast("int"))
    )

    # ── MyMcDonald's Rewards points economy ────────────────────────────────
    df = (
        df
        .withColumn("lifetime_points_earned", F.coalesce("lifetime_points_earned", F.lit(0)))
        .withColumn("lifetime_points_redeemed", F.coalesce("lifetime_points_redeemed", F.lit(0)))
        .withColumn("points_expired", F.coalesce("points_expired", F.lit(0)))
        .withColumn("bonus_points", F.coalesce("bonus_points", F.lit(0)))
        .withColumn("points_earned_12mo", F.coalesce("points_earned_12mo", F.lit(0)))
        .withColumn("redemptions_count", F.coalesce("redemptions_count", F.lit(0)))
        # Displayed balance can't go negative (independent synthetic redeem/expire).
        .withColumn("points_balance", F.greatest(F.coalesce("_net_points", F.lit(0)), F.lit(0)))
        .withColumn("points_liability_php",
                    F.round(F.col("points_balance") * F.lit(POINT_VALUE_PHP), 2))
        .withColumn("redemption_rate",
                    F.round(F.col("lifetime_points_redeemed")
                            / F.nullif(F.col("lifetime_points_earned"), F.lit(0)), 3))
        # qualified_tier = what the member's rolling-12mo earnings justify.
        .withColumn("qualified_tier", _tier_col(F.col("points_earned_12mo")))
        .withColumn("qualified_tier_rank", _tier_col(F.col("points_earned_12mo"), want_rank=True))
        # current_tier = the tier the member currently HOLDS. Mostly tracks the
        # qualified tier, with realistic drift: ~12% hold one tier above (grace /
        # tier inflation → downgrade risk), ~10% one below (recent upgrade not yet
        # reflected → upgrade eligible). Deterministic via a hash of customer_id.
        .withColumn("_drift", F.pmod(F.hash(F.col("customer_id")), F.lit(100)))
        .withColumn("current_tier_rank",
                    F.when(F.col("_drift") < 12, F.least(F.col("qualified_tier_rank") + 1, F.lit(4)))
                    .when(F.col("_drift") < 22, F.greatest(F.col("qualified_tier_rank") - 1, F.lit(1)))
                    .otherwise(F.col("qualified_tier_rank")))
        .withColumn("current_tier", _rank_name(F.col("current_tier_rank")))
        .withColumn("tier_status",
                    F.when(F.col("qualified_tier_rank") > F.col("current_tier_rank"), "Upgrade eligible")
                    .when(F.col("qualified_tier_rank") < F.col("current_tier_rank"), "Downgrade risk")
                    .otherwise("On track"))
        # Points still needed this year to reach the next status tier.
        .withColumn("points_to_next_tier",
                    F.when(F.col("qualified_tier_rank") >= 4, F.lit(0))
                    .when(F.col("qualified_tier_rank") == 3, F.lit(40000) - F.col("points_earned_12mo"))
                    .when(F.col("qualified_tier_rank") == 2, F.lit(15000) - F.col("points_earned_12mo"))
                    .otherwise(F.lit(5000) - F.col("points_earned_12mo")))
        .withColumn("days_since_last_redeem", F.datediff(F.current_date(), F.col("last_redeem_date")))
        .withColumn("_refreshed_at", F.current_timestamp())
        .drop("_drift", "loyalty_tier")
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


# ---------------------------------------------------------------------------
# GOLD: points_economy  (daily earn / redeem / expire + running liability)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("points_economy"),
    comment="Daily points earned/redeemed/expired/bonus, net flow, and running outstanding balance.",
    table_properties={"quality": "gold"},
    cluster_by=["txn_date"],
)
def points_economy():
    daily = (
        spark.read.table("points_ledger_clean")
        .groupBy("txn_date").agg(
            F.sum(F.when(F.col("txn_type") == "EARN", F.col("points")).otherwise(0)).alias("points_earned"),
            F.sum(F.when(F.col("txn_type") == "BONUS", F.col("points")).otherwise(0)).alias("bonus_points"),
            F.sum(F.when(F.col("txn_type") == "REDEEM", -F.col("points")).otherwise(0)).alias("points_redeemed"),
            F.sum(F.when(F.col("txn_type") == "EXPIRE", -F.col("points")).otherwise(0)).alias("points_expired"),
            F.sum("points").alias("net_points"),
            F.sum(F.when(F.col("txn_type") == "REDEEM", 1).otherwise(0)).alias("redemptions"),
            F.countDistinct("customer_id").alias("active_members"),
        )
        .filter(F.col("txn_date").isNotNull())
    )
    # Running outstanding balance = cumulative net points issued (the liability).
    running = W.orderBy("txn_date").rowsBetween(W.unboundedPreceding, W.currentRow)
    return (
        daily
        .withColumn("outstanding_balance", F.sum("net_points").over(running))
        .withColumn("outstanding_liability_php",
                    F.round(F.col("outstanding_balance") * F.lit(POINT_VALUE_PHP), 2))
        .withColumn("redemption_ratio",
                    F.round(F.col("points_redeemed") / F.nullif(F.col("points_earned"), F.lit(0)), 3))
    )


# ---------------------------------------------------------------------------
# GOLD: reward_performance  (per catalog reward)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("reward_performance"),
    comment="Per-reward redemptions, points spent, unique members, and peso value delivered.",
    table_properties={"quality": "gold"},
)
def reward_performance():
    rewards = spark.read.table(bronze("dim_reward"))
    redeems = (
        spark.read.table("points_ledger_clean")
        .filter(F.col("txn_type") == "REDEEM")
        .groupBy("reward_id").agg(
            F.count("*").alias("redemptions"),
            F.sum(-F.col("points")).alias("points_spent"),
            F.countDistinct("customer_id").alias("unique_members"),
            F.max("txn_date").alias("last_redeemed_date"),
        )
    )
    return (
        rewards.join(redeems, "reward_id", "left")
        .withColumn("redemptions", F.coalesce("redemptions", F.lit(0)))
        .withColumn("points_spent", F.coalesce("points_spent", F.lit(0)))
        .withColumn("unique_members", F.coalesce("unique_members", F.lit(0)))
        .withColumn("value_delivered_php", F.round(F.col("redemptions") * F.col("est_value_php"), 2))
        .withColumn("pct_of_redemptions",
                    F.round(F.col("redemptions")
                            / F.nullif(F.sum("redemptions").over(W.partitionBy()), F.lit(0)), 4))
    )


# ---------------------------------------------------------------------------
# GOLD: tier_migration  (held-tier × qualified-tier transition matrix)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=gold("tier_migration"),
    comment="Members by held status tier × the tier their rolling-12mo points qualify for.",
    table_properties={"quality": "gold"},
)
def tier_migration():
    return (
        spark.read.table(gold("customer_360"))
        .groupBy("current_tier", "current_tier_rank", "qualified_tier", "qualified_tier_rank", "tier_status")
        .agg(
            F.count("*").alias("members"),
            F.sum("points_balance").alias("points_balance"),
            F.round(F.sum("points_liability_php"), 2).alias("points_liability_php"),
        )
    )
