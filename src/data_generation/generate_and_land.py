# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate & land synthetic source data (McDonald's PH — MyMcDonald's Rewards)
# MAGIC
# MAGIC Produces a realistic **membership / loyalty** dataset for the *MyMcDonald's
# MAGIC Rewards* program and lands the *event/fact* feeds as JSON files on the
# MAGIC **S3 landing volume** so that Auto Loader (Module 1) has real files to
# MAGIC ingest. Reference dimensions (`dim_store`, `dim_product`, `dim_reward`,
# MAGIC `dim_tier`) are written straight to Bronze as Delta.
# MAGIC
# MAGIC ### Rewards model (synthetic, modeled on MyMcDonald's Rewards)
# MAGIC * **Earn** 1 point per ₱1 net spend on app-linked orders, times a
# MAGIC   **status-tier multiplier** (Member 1.0 · Silver 1.1 · Gold 1.25 · Platinum 1.5).
# MAGIC * **Redeem** points against a rewards catalog priced in the four canonical
# MAGIC   tiers **1500 / 3000 / 4500 / 6000** points.
# MAGIC * Points **expire** after 12 months (drives *breakage*); occasional **bonus**
# MAGIC   promos add points. The `points_ledger` feed is the single source of truth
# MAGIC   for the points economy (earn / redeem / expire / bonus / adjust).
# MAGIC
# MAGIC Everything is generated **Spark-native** (no Faker / pip installs) so it runs
# MAGIC on plain serverless. Runs in two modes:
# MAGIC
# MAGIC * `full`        — historical backfill (default volumes below).
# MAGIC * `incremental` — a small slice of *recent* orders/events, to demonstrate
# MAGIC   Auto Loader picking up **only new files** on the next run.
# MAGIC
# MAGIC Data is intentionally **skewed** (a minority of members buy frequently) so the
# MAGIC Gold RFM / churn / CLV logic has something meaningful to segment.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "dante_classic_stable_catalog")
dbutils.widgets.text("bronze_schema", "mcdo_ph_bronze")
dbutils.widgets.text(
    "landing_root",
    "s3://dante-classic-stable-ext-s3-049629455384-g7v92y/mcdo_ph/landing",
)
dbutils.widgets.dropdown("mode", "full", ["full", "incremental"])
dbutils.widgets.text("batch_id", "0")
dbutils.widgets.text("n_customers", "8000")
dbutils.widgets.text("n_orders", "150000")
dbutils.widgets.text("n_app_events", "250000")
dbutils.widgets.text("months_history", "12")

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
landing = dbutils.widgets.get("landing_root").rstrip("/")
mode = dbutils.widgets.get("mode")
batch_id = dbutils.widgets.get("batch_id")

N_CUST = int(dbutils.widgets.get("n_customers"))
N_STORE = 60
DAYS = int(dbutils.widgets.get("months_history")) * 30

if mode == "incremental":
    # A recent burst: ~3% of orders, last 3 days, no new customers/dims.
    N_ORDERS = max(1, int(dbutils.widgets.get("n_orders")) // 30)
    N_EVENTS = max(1, int(dbutils.widgets.get("n_app_events")) // 30)
    DAYS = 3
else:
    N_ORDERS = int(dbutils.widgets.get("n_orders"))
    N_EVENTS = int(dbutils.widgets.get("n_app_events"))

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{bronze}`")
print(f"mode={mode} batch={batch_id} | customers={N_CUST} orders={N_ORDERS} events={N_EVENTS} days={DAYS}")

# `full` overwrites its landing files so a backfill is idempotent (re-runnable
# without accumulating stale rows); `incremental` appends a fresh batch to
# demonstrate Auto Loader picking up only new files.
land_mode = "overwrite" if mode == "full" else "append"

# COMMAND ----------

# MAGIC %md ### Reference vocabularies (Philippines-localized)


# COMMAND ----------

def sql_arr(values):
    """Render a Python list as a SQL array literal of strings."""
    escaped = ",".join("'" + str(v).replace("'", "''") + "'" for v in values)
    return f"array({escaped})"


# region -> representative city/province, weighted toward Metro Manila (NCR)
GEO = [
    ("NCR", "Metro Manila", "Quezon City"), ("NCR", "Metro Manila", "Manila"),
    ("NCR", "Metro Manila", "Makati"), ("NCR", "Metro Manila", "Taguig"),
    ("NCR", "Metro Manila", "Pasig"), ("NCR", "Metro Manila", "Mandaluyong"),
    ("NCR", "Metro Manila", "Parañaque"), ("NCR", "Metro Manila", "Caloocan"),
    ("Region III", "Pampanga", "San Fernando"), ("Region III", "Bulacan", "Malolos"),
    ("Region IV-A", "Cavite", "Bacoor"), ("Region IV-A", "Laguna", "Santa Rosa"),
    ("Region IV-A", "Batangas", "Batangas City"), ("Region VII", "Cebu", "Cebu City"),
    ("Region VII", "Cebu", "Mandaue"), ("Region XI", "Davao del Sur", "Davao City"),
    ("Region VI", "Iloilo", "Iloilo City"), ("Region I", "Pangasinan", "Dagupan"),
]
FIRST_NAMES = ["Juan", "Maria", "Jose", "Angelica", "Mark", "Ana", "Paolo", "Kristine",
               "John", "Grace", "Michael", "Joy", "Carlo", "Nicole", "Rafael", "Bea",
               "Miguel", "Andrea", "Gabriel", "Isabella", "Daniel", "Camille", "Josh",
               "Patricia", "Kevin", "Ella", "Jerome", "Trisha", "Aaron", "Sofia"]
LAST_NAMES = ["Santos", "Reyes", "Cruz", "Bautista", "Ocampo", "Garcia", "Mendoza",
              "Torres", "Flores", "Villanueva", "Ramos", "Aquino", "Del Rosario",
              "Castillo", "Domingo", "Fernandez", "Gonzales", "Lim", "Tan", "Dela Cruz"]
STORE_FORMATS = ["Freestanding", "In-Mall", "Drive-Thru", "Kiosk"]
CHANNELS = ["DriveThru", "CounterDineIn", "KioskDineIn", "McDelivery", "InAppPickup"]
PAYMENTS = ["Cash", "GCash", "Maya", "CreditCard", "OnlinePay"]
TIERS = ["Member", "Silver", "Gold", "Platinum"]
SIGNUP_CH = ["iOS", "Android", "Web", "In-Store"]
LANGS = ["Filipino", "English"]
GENDERS = ["F", "M", "U"]
DELIVERY_PARTNERS = ["McDelivery", "GrabFood", "foodpanda"]
EVENT_TYPES = ["app_open", "menu_view", "add_to_cart", "checkout",
               "reward_redeem", "push_received", "push_opened", "coupon_view"]

# product_name, category, base_price (PHP), is_localized
PRODUCTS = [
    ("Chicken McDo (1pc) w/ Rice", "Chicken", 99.0, True),
    ("Chicken McDo (2pc) w/ Rice", "Chicken", 175.0, True),
    ("Burger McDo", "Burgers", 45.0, True),
    ("Cheeseburger", "Burgers", 65.0, False),
    ("Big Mac", "Burgers", 160.0, False),
    ("Quarter Pounder w/ Cheese", "Burgers", 175.0, False),
    ("McChicken", "Burgers", 95.0, False),
    ("Filet-O-Fish", "Burgers", 105.0, False),
    ("McSpaghetti", "Pasta", 65.0, True),
    ("McSpaghetti w/ Chicken McDo", "Value Meals", 165.0, True),
    ("Longganisa McDo w/ Egg & Rice", "Breakfast", 120.0, True),
    ("Corned Beef McDo w/ Egg & Rice", "Breakfast", 120.0, True),
    ("Chicken McDo w/ Egg & Rice", "Breakfast", 150.0, True),
    ("Hotcakes", "Breakfast", 85.0, False),
    ("Sausage McMuffin w/ Egg", "Breakfast", 99.0, False),
    ("World Famous Fries (Med)", "Sides", 75.0, False),
    ("Hashbrown", "Sides", 45.0, False),
    ("Coke Float", "Beverages", 55.0, False),
    ("Coca-Cola (Med)", "Beverages", 55.0, False),
    ("Sprite (Med)", "Beverages", 55.0, False),
    ("Iced Tea", "Beverages", 55.0, False),
    ("Premium Roast Coffee", "McCafé", 65.0, False),
    ("Iced Caramel Macchiato", "McCafé", 120.0, False),
    ("Caramel Frappé", "McCafé", 135.0, False),
    ("McFlurry Oreo", "Desserts", 65.0, False),
    ("Hot Fudge Sundae", "Desserts", 40.0, False),
    ("Apple Pie", "Desserts", 40.0, False),
    ("6pc Chicken McNuggets", "Chicken", 130.0, False),
]

# --- MyMcDonald's Rewards program config -----------------------------------
# Status tier -> (tier_rank, min rolling-12mo points to qualify, earn multiplier)
TIER_DEF = {
    "Member":   (1, 0,     1.00),
    "Silver":   (2, 5000,  1.10),
    "Gold":     (3, 15000, 1.25),
    "Platinum": (4, 40000, 1.50),
}
# Blended peso value of one point (used for the points-liability estimate in Gold).
POINT_VALUE_PHP = 0.04

# reward_name, category, point_cost, est_value_php  (grouped by the 4 canonical tiers)
REWARDS_CATALOG = [
    # 1500-point tier — snacks / sides / desserts
    ("Free World Famous Fries (Med)", "Sides",     1500, 75.0),
    ("Free Hashbrown",                "Sides",     1500, 45.0),
    ("Free Coke Float",               "Beverages", 1500, 55.0),
    ("Free Hot Fudge Sundae",         "Desserts",  1500, 40.0),
    # 3000-point tier — singles / McCafé
    ("Free McChicken",                "Burgers",   3000, 95.0),
    ("Free McSpaghetti",              "Pasta",     3000, 65.0),
    ("Free Premium Roast Coffee",     "McCafé",    3000, 65.0),
    ("Free 6pc Chicken McNuggets",    "Chicken",   3000, 130.0),
    # 4500-point tier — premium sandwiches
    ("Free Big Mac",                  "Burgers",   4500, 160.0),
    ("Free Quarter Pounder w/ Cheese","Burgers",   4500, 175.0),
    ("Free Chicken McDo (1pc) w/ Rice","Chicken",  4500, 99.0),
    # 6000-point tier — value meals / premium McCafé
    ("Free Chicken McDo (2pc) w/ Rice","Chicken",  6000, 175.0),
    ("Free McSpaghetti w/ Chicken McDo","Value Meals", 6000, 165.0),
    ("Free Caramel Frappé",           "McCafé",    6000, 135.0),
]

# COMMAND ----------

# MAGIC %md ### Dimensions (written to Bronze as Delta — reference data)

# COMMAND ----------

if mode == "full":
    stores = (
        spark.range(N_STORE)
        .withColumn("store_id", F.format_string("STR-%04d", F.col("id").cast("int")))
        .withColumn("g", (F.rand(7) * F.lit(len(GEO))).cast("int"))
        .withColumn("region", F.element_at(F.expr(sql_arr([g[0] for g in GEO])), F.col("g") + 1))
        .withColumn("province", F.element_at(F.expr(sql_arr([g[1] for g in GEO])), F.col("g") + 1))
        .withColumn("city", F.element_at(F.expr(sql_arr([g[2] for g in GEO])), F.col("g") + 1))
        .withColumn("store_name", F.concat(F.lit("McDonald's "), F.col("city"), F.lit(" #"),
                                           F.col("id").cast("int")))
        .withColumn("store_format", F.element_at(F.expr(sql_arr(STORE_FORMATS)),
                                                 (F.rand(1) * len(STORE_FORMATS)).cast("int") + 1))
        .withColumn("has_drive_thru", (F.col("store_format") == "Drive-Thru") | (F.rand(2) > 0.5))
        .withColumn("has_mccafe", F.rand(3) > 0.35)
        .withColumn("latitude", F.round(F.lit(14.5) + (F.rand(4) - 0.5) * 3.0, 6))
        .withColumn("longitude", F.round(F.lit(121.0) + (F.rand(5) - 0.5) * 3.0, 6))
        .withColumn("open_date", F.expr("date_sub(current_date(), cast(rand(6)*3650 as int))"))
        .drop("id", "g")
    )
    stores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_store")
    print(f"✓ dim_store ({stores.count()} rows)")

    prod_rows = [(f"PRD-{i:04d}", p[0], p[1], float(p[2]), p[3]) for i, p in enumerate(PRODUCTS)]
    products = spark.createDataFrame(
        prod_rows, "product_id string, product_name string, category string, unit_price double, is_localized boolean"
    )
    products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_product")
    print(f"✓ dim_product ({products.count()} rows)")

    tier_rows = [(name, r[0], r[1], r[2]) for name, r in TIER_DEF.items()]
    tiers = spark.createDataFrame(
        tier_rows, "tier_name string, tier_rank int, min_points_12mo int, earn_multiplier double"
    )
    tiers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_tier")
    print(f"✓ dim_tier ({tiers.count()} rows)")

    reward_rows = [
        (f"RWD-{i:03d}", r[0], r[1], int(r[2]), float(r[3]),
         f"{r[2]} pts", float(r[3]) / r[2])  # value per point (redeemed)
        for i, r in enumerate(REWARDS_CATALOG)
    ]
    rewards = spark.createDataFrame(
        reward_rows,
        "reward_id string, reward_name string, category string, point_cost int, "
        "est_value_php double, reward_tier string, value_per_point double",
    ).withColumn("is_active", F.lit(True))
    rewards.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_reward")
    print(f"✓ dim_reward ({rewards.count()} rows)")

products = spark.table("dim_product")
N_PROD = products.count()

# COMMAND ----------

# MAGIC %md ### Customers  → land JSON to `landing/customers/`
# MAGIC A few records carry deliberately dirty values (bad email, null id, future
# MAGIC signup date) so Module 2's data-quality expectations have something to catch.

# COMMAND ----------

if mode == "full":
    customers = (
        spark.range(N_CUST)
        .withColumn("customer_id", F.format_string("CUST-%07d", F.col("id").cast("int")))
        .withColumn("g", (F.rand(11) * F.lit(len(GEO))).cast("int"))
        .withColumn("first_name", F.element_at(F.expr(sql_arr(FIRST_NAMES)),
                                               (F.rand(12) * len(FIRST_NAMES)).cast("int") + 1))
        .withColumn("last_name", F.element_at(F.expr(sql_arr(LAST_NAMES)),
                                              (F.rand(13) * len(LAST_NAMES)).cast("int") + 1))
        .withColumn("email", F.concat(F.lower(F.col("first_name")), F.lit("."),
                                      F.lower(F.col("last_name")), F.col("id").cast("int"),
                                      F.lit("@example.ph")))
        .withColumn("mobile", F.concat(F.lit("+639"), F.lpad((F.rand(14) * 1e9).cast("long").cast("string"), 9, "0")))
        .withColumn("gender", F.element_at(F.expr(sql_arr(GENDERS)), (F.rand(15) * 3).cast("int") + 1))
        .withColumn("birth_date", F.expr("date_sub(current_date(), cast((rand(16)*40 + 16)*365 as int))"))
        .withColumn("region", F.element_at(F.expr(sql_arr([g[0] for g in GEO])), F.col("g") + 1))
        .withColumn("province", F.element_at(F.expr(sql_arr([g[1] for g in GEO])), F.col("g") + 1))
        .withColumn("city", F.element_at(F.expr(sql_arr([g[2] for g in GEO])), F.col("g") + 1))
        .withColumn("signup_channel", F.element_at(F.expr(sql_arr(SIGNUP_CH)), (F.rand(17) * 4).cast("int") + 1))
        .withColumn("signup_date", F.expr(f"date_sub(current_date(), cast(rand(18)*{DAYS+400} as int))"))
        .withColumn("loyalty_tier", F.element_at(F.expr(sql_arr(TIERS)),
                    F.when(F.rand(19) > 0.85, 4).when(F.rand(19) > 0.65, 3).when(F.rand(19) > 0.35, 2).otherwise(1)))
        .withColumn("preferred_language", F.element_at(F.expr(sql_arr(LANGS)), (F.rand(20) * 2).cast("int") + 1))
        .withColumn("marketing_consent", F.rand(21) > 0.25)
        # Inject ~0.5% dirty rows for the DQ story:
        .withColumn("email", F.when(F.rand(22) < 0.004, F.lit("not-an-email")).otherwise(F.col("email")))
        .withColumn("customer_id", F.when(F.rand(23) < 0.002, F.lit(None)).otherwise(F.col("customer_id")))
        .withColumn("signup_date", F.when(F.rand(24) < 0.003,
                    F.expr("date_add(current_date(), 30)")).otherwise(F.col("signup_date")))
        .withColumn("_source_system", F.lit("loyalty-app"))
        .drop("id", "g")
    )
    (customers.repartition(8).write.mode("overwrite")
        .json(f"{landing}/customers"))
    print(f"✓ landed customers -> {landing}/customers")

# COMMAND ----------

# MAGIC %md ### Orders + order items  → land JSON
# MAGIC Customer selection is **power-law skewed** (`pow(rand(),2.2)`) so a small set
# MAGIC of members become high-frequency buyers. `order_ts` decays toward the present
# MAGIC so recency is meaningful.

# COMMAND ----------

orders_skeleton = (
    spark.range(N_ORDERS)
    .withColumn("order_id", F.format_string(f"ORD-B{batch_id}-%09d", F.col("id").cast("int")))
    .withColumn("cust_idx", (F.pow(F.rand(31), F.lit(2.2)) * N_CUST).cast("int"))
    .withColumn("customer_id", F.format_string("CUST-%07d", F.col("cust_idx")))
    .withColumn("store_id", F.format_string("STR-%04d", (F.rand(32) * N_STORE).cast("int")))
    .withColumn("order_ts", F.expr(
        f"current_timestamp() - make_dt_interval(cast(pow(rand(33),1.5)*{DAYS} as int), "
        f"cast(rand(34)*15 + 7 as int), cast(rand(35)*60 as int), 0)"))
    .withColumn("channel", F.element_at(F.expr(sql_arr(CHANNELS)), (F.rand(36) * len(CHANNELS)).cast("int") + 1))
    .withColumn("is_delivery", F.col("channel") == "McDelivery")
    .withColumn("delivery_partner", F.when(F.col("is_delivery"),
                F.element_at(F.expr(sql_arr(DELIVERY_PARTNERS)), (F.rand(37) * 3).cast("int") + 1)))
    .withColumn("payment_method", F.element_at(F.expr(sql_arr(PAYMENTS)), (F.rand(38) * len(PAYMENTS)).cast("int") + 1))
    .withColumn("promo_code", F.when(F.rand(39) < 0.22,
                F.element_at(F.expr(sql_arr(["PISOFRIES", "BFF", "MCSAVERS", "APPDEAL50", "GOLDPERK"])),
                             (F.rand(40) * 5).cast("int") + 1)))
    .withColumn("item_count", (F.rand(41) * 4 + 1).cast("int"))
    .withColumn("order_status", F.when(F.rand(42) < 0.03, F.lit("CANCELLED")).otherwise(F.lit("COMPLETED")))
    .select("order_id", "customer_id", "store_id", "order_ts", "channel", "is_delivery",
            "delivery_partner", "payment_method", "promo_code", "item_count", "order_status")
)
# Serverless has no .cache(); stage to Delta so rand()-derived rows are stable
# across the item explosion and the header/items join (otherwise recomputation
# would produce mismatched random values).
(orders_skeleton.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("_stage_orders"))
stage = spark.table("_stage_orders")

# Explode into line items and price them from dim_product
items = (
    stage
    .withColumn("line_no", F.explode(F.expr("sequence(1, item_count)")))
    .withColumn("product_id", F.format_string("PRD-%04d", (F.rand(51) * N_PROD).cast("int")))
    .withColumn("quantity", (F.rand(52) * 2 + 1).cast("int"))
    .join(products.select("product_id", "unit_price"), "product_id", "left")
    .withColumn("line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .select("order_id", "line_no", "product_id", "quantity", "unit_price", "line_amount")
)
(items.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("_stage_order_items"))
items = spark.table("_stage_order_items")

# Order header amounts derived from items (so silver can validate header == sum(items))
order_totals = items.groupBy("order_id").agg(
    F.round(F.sum("line_amount"), 2).alias("gross_amount"),
    F.sum("quantity").alias("total_units"),
)
orders = (
    stage.join(order_totals, "order_id")
    .withColumn("discount_amount", F.when(F.col("promo_code").isNotNull(),
                F.round(F.col("gross_amount") * (F.rand(61) * 0.25 + 0.05), 2)).otherwise(F.lit(0.0)))
    .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
    .withColumn("_source_system", F.lit("pos"))
)

(orders.repartition(12).write.mode(land_mode).json(f"{landing}/orders/batch={batch_id}"))
(items.repartition(12).write.mode(land_mode).json(f"{landing}/order_items/batch={batch_id}"))
print(f"✓ landed orders + order_items (batch={batch_id})")

# COMMAND ----------

# MAGIC %md ### App engagement events → land JSON

# COMMAND ----------

events = (
    spark.range(N_EVENTS)
    .withColumn("event_id", F.format_string(f"EVT-B{batch_id}-%010d", F.col("id").cast("int")))
    .withColumn("cust_idx", (F.pow(F.rand(71), F.lit(1.8)) * N_CUST).cast("int"))
    .withColumn("customer_id", F.format_string("CUST-%07d", F.col("cust_idx")))
    .withColumn("event_ts", F.expr(
        f"current_timestamp() - make_dt_interval(cast(pow(rand(72),1.4)*{DAYS} as int), "
        f"cast(rand(73)*24 as int), cast(rand(74)*60 as int), 0)"))
    .withColumn("event_type", F.element_at(F.expr(sql_arr(EVENT_TYPES)), (F.rand(75) * len(EVENT_TYPES)).cast("int") + 1))
    .withColumn("platform", F.element_at(F.expr(sql_arr(["iOS", "Android", "Web"])), (F.rand(76) * 3).cast("int") + 1))
    .withColumn("session_id", F.format_string("SES-%012d", (F.rand(77) * 5e8).cast("long")))
    .withColumn("store_id", F.when(F.rand(78) < 0.4,
                F.format_string("STR-%04d", (F.rand(79) * N_STORE).cast("int"))))
    .select("event_id", "customer_id", "event_ts", "event_type", "platform", "session_id", "store_id")
)
(events.repartition(10).write.mode(land_mode).json(f"{landing}/app_events/batch={batch_id}"))
print(f"✓ landed app_events (batch={batch_id})")

# COMMAND ----------

# MAGIC %md ### Points ledger  → land JSON to `landing/points_ledger/`
# MAGIC The MyMcDonald's Rewards points economy: **EARN** rows are derived from
# MAGIC completed orders (1 pt/₱1 × the member's status-tier multiplier); **REDEEM**
# MAGIC rows consume a reward's `point_cost`; **BONUS** promos add points; and (full
# MAGIC backfill only) **EXPIRE** rows age out old points to create *breakage*.
# MAGIC A few rows are deliberately dirty (null member, unknown txn type) for the
# MAGIC Module 2 data-quality story.

# COMMAND ----------

REWARD_IDS = [f"RWD-{i:03d}" for i in range(len(REWARDS_CATALOG))]
REWARD_COSTS = [r[2] for r in REWARDS_CATALOG]
# Tuned so redemptions consume ~20-25% of earned points and expiry (breakage)
# ~8-10% — a realistic, healthy loyalty economy rather than a points deficit.
N_REDEEM = max(1, int(N_ORDERS * 0.04))
N_BONUS = max(1, int(N_ORDERS * 0.03))
N_EXPIRE = max(1, int(N_CUST * 0.35))
LEDGER_COLS = ["ledger_id", "customer_id", "txn_ts", "txn_type", "points",
               "order_id", "reward_id", "source_channel"]

# Per-customer status tier, reproduced deterministically from the SAME seeded
# expression used when generating customers, so EARN multipliers line up with
# each member's tier without needing to re-read the landed customers feed.
tier_mult = (F.when(F.col("loyalty_tier") == "Platinum", F.lit(1.50))
             .when(F.col("loyalty_tier") == "Gold", F.lit(1.25))
             .when(F.col("loyalty_tier") == "Silver", F.lit(1.10))
             .otherwise(F.lit(1.00)))
cust_tier = (
    spark.range(N_CUST)
    .withColumn("customer_id", F.format_string("CUST-%07d", F.col("id").cast("int")))
    .withColumn("loyalty_tier", F.element_at(F.expr(sql_arr(TIERS)),
                F.when(F.rand(19) > 0.85, 4).when(F.rand(19) > 0.65, 3)
                 .when(F.rand(19) > 0.35, 2).otherwise(1)))
    .withColumn("earn_multiplier", tier_mult)
    .select("customer_id", "earn_multiplier")
)

# EARN — one ledger row per completed order.
earn = (
    orders.filter(F.col("order_status") == "COMPLETED")
    .join(cust_tier, "customer_id", "left")
    .withColumn("earn_multiplier", F.coalesce(F.col("earn_multiplier"), F.lit(1.0)))
    .withColumn("points", F.floor(F.col("net_amount") * F.col("earn_multiplier")).cast("int"))
    .filter(F.col("points") > 0)
    .withColumn("ledger_id", F.concat(F.lit(f"PL-E-B{batch_id}-"), F.col("order_id")))
    .withColumn("txn_type", F.lit("EARN"))
    .withColumn("txn_ts", F.col("order_ts"))
    .withColumn("reward_id", F.lit(None).cast("string"))
    .withColumn("source_channel", F.col("channel"))
    .select(*LEDGER_COLS)
)

# REDEEM — members spend points on catalog rewards (skewed toward cheaper tiers).
cost_arr = "array(" + ",".join(str(c) for c in REWARD_COSTS) + ")"
redeem = (
    spark.range(N_REDEEM)
    .withColumn("cust_idx", (F.pow(F.rand(81), F.lit(1.7)) * N_CUST).cast("int"))
    .withColumn("customer_id", F.format_string("CUST-%07d", F.col("cust_idx")))
    .withColumn("ridx", (F.pow(F.rand(82), F.lit(2.4)) * len(REWARDS_CATALOG)).cast("int"))
    .withColumn("reward_id", F.element_at(F.expr(sql_arr(REWARD_IDS)), F.col("ridx") + 1))
    .withColumn("points", -F.element_at(F.expr(cost_arr), F.col("ridx") + 1).cast("int"))
    .withColumn("txn_ts", F.expr(
        f"current_timestamp() - make_dt_interval(cast(pow(rand(83),1.3)*{max(1, DAYS - 2)} as int), "
        f"cast(rand(84)*24 as int), cast(rand(85)*60 as int), 0)"))
    .withColumn("ledger_id", F.format_string(f"PL-R-B{batch_id}-%09d", F.col("id").cast("int")))
    .withColumn("txn_type", F.lit("REDEEM"))
    .withColumn("order_id", F.lit(None).cast("string"))
    .withColumn("source_channel", F.lit("app"))
    .select(*LEDGER_COLS)
)

# BONUS — promo point boosts (e.g. app-birthday, double-points weekends).
bonus = (
    spark.range(N_BONUS)
    .withColumn("cust_idx", (F.pow(F.rand(86), F.lit(1.4)) * N_CUST).cast("int"))
    .withColumn("customer_id", F.format_string("CUST-%07d", F.col("cust_idx")))
    .withColumn("points", (F.rand(87) * 400 + 100).cast("int"))
    .withColumn("txn_ts", F.expr(
        f"current_timestamp() - make_dt_interval(cast(rand(88)*{max(1, DAYS)} as int), "
        f"cast(rand(89)*24 as int), 0, 0)"))
    .withColumn("ledger_id", F.format_string(f"PL-B-B{batch_id}-%09d", F.col("id").cast("int")))
    .withColumn("txn_type", F.lit("BONUS"))
    .withColumn("order_id", F.lit(None).cast("string"))
    .withColumn("reward_id", F.lit(None).cast("string"))
    .withColumn("source_channel", F.lit("promo"))
    .select(*LEDGER_COLS)
)

ledger = earn.unionByName(redeem).unionByName(bonus)

if mode == "full":
    # EXPIRE — points aging out after 12 months, dated in the older half of the
    # window so they sit behind recent activity (this is program breakage).
    expire = (
        spark.range(N_EXPIRE)
        .withColumn("cust_idx", (F.rand(90) * N_CUST).cast("int"))
        .withColumn("customer_id", F.format_string("CUST-%07d", F.col("cust_idx")))
        .withColumn("points", -(F.rand(91) * 3000 + 500).cast("int"))
        .withColumn("txn_ts", F.expr(
            f"current_timestamp() - make_dt_interval(cast(rand(92)*{max(1, DAYS // 2)} + {DAYS // 2} as int), "
            f"0, 0, 0)"))
        .withColumn("ledger_id", F.format_string("PL-X-%09d", F.col("id").cast("int")))
        .withColumn("txn_type", F.lit("EXPIRE"))
        .withColumn("order_id", F.lit(None).cast("string"))
        .withColumn("reward_id", F.lit(None).cast("string"))
        .withColumn("source_channel", F.lit("system"))
        .select(*LEDGER_COLS)
    )
    ledger = ledger.unionByName(expire)

# Inject ~0.3% dirty rows for the DQ expectations in Module 2.
ledger = (
    ledger
    .withColumn("customer_id", F.when(F.rand(93) < 0.002, F.lit(None)).otherwise(F.col("customer_id")))
    .withColumn("txn_type", F.when(F.rand(94) < 0.001, F.lit("UNKNOWN")).otherwise(F.col("txn_type")))
    .withColumn("_source_system", F.lit("loyalty-rewards"))
)
(ledger.repartition(12).write.mode(land_mode).json(f"{landing}/points_ledger/batch={batch_id}"))
print(f"✓ landed points_ledger (batch={batch_id})")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS _stage_orders")
spark.sql("DROP TABLE IF EXISTS _stage_order_items")

# COMMAND ----------

dbutils.notebook.exit(
    f"Landed {mode} batch={batch_id}: {N_ORDERS} orders, {N_EVENTS} events, "
    f"points_ledger (earn+{N_REDEEM} redeem+{N_BONUS} bonus) at {landing}")
