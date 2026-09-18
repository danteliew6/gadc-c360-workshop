# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2 (cont.) · Governed metric view
# MAGIC
# MAGIC A Unity Catalog **metric view** gives BI, Genie, and ad-hoc SQL one
# MAGIC consistent definition of the core loyalty KPIs (active members, churn
# MAGIC rate, repeat rate, CLV, AOV) sliced by region, tier, and RFM segment.
# MAGIC Runs after the SDP pipeline has materialized `gold.customer_360`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dante_classic_stable_catalog")
dbutils.widgets.text("gold_schema", "mcdo_ph_gold")
catalog = dbutils.widgets.get("catalog")
gold = dbutils.widgets.get("gold_schema")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{gold}`.c360_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: {catalog}.{gold}.customer_360
dimensions:
  - name: Region
    expr: region
  - name: Province
    expr: province
  - name: Loyalty Tier
    expr: loyalty_tier
  - name: RFM Segment
    expr: rfm_segment
  - name: Churn Risk
    expr: churn_risk
  - name: Preferred Channel
    expr: preferred_channel
  - name: Signup Channel
    expr: signup_channel
measures:
  - name: Members
    expr: COUNT(DISTINCT customer_id)
  - name: Active Members
    expr: COUNT(DISTINCT CASE WHEN total_orders > 0 THEN customer_id END)
  - name: Total Revenue
    expr: SUM(total_spend)
  - name: Total Orders
    expr: SUM(total_orders)
  - name: Avg Order Value
    expr: SUM(total_spend) / NULLIF(SUM(total_orders), 0)
  - name: Repeat Rate
    expr: COUNT(DISTINCT CASE WHEN total_orders > 1 THEN customer_id END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN total_orders > 0 THEN customer_id END), 0)
  - name: High Churn Risk Members
    expr: COUNT(DISTINCT CASE WHEN churn_risk = 'High' THEN customer_id END)
  - name: Est. CLV
    expr: SUM(clv_estimate)
  - name: Avg Engagement Score
    expr: AVG(engagement_score)
$$
""")
print(f"✓ metric view {catalog}.{gold}.c360_metrics")

# COMMAND ----------

dbutils.notebook.exit("Metric view created")
