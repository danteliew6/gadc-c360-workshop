# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2 (cont.) · Governed metric view
# MAGIC
# MAGIC A Unity Catalog **metric view** gives BI, Genie, and ad-hoc SQL one
# MAGIC consistent definition of the core **MyMcDonald's Rewards** KPIs — active
# MAGIC members, points earned/redeemed, redemption rate, outstanding points
# MAGIC liability, and tier health — sliced by region, status tier, qualified
# MAGIC tier, and RFM segment. Runs after the SDP pipeline materializes
# MAGIC `gold.customer_360`.

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
  - name: Status Tier
    expr: current_tier
  - name: Qualified Tier
    expr: qualified_tier
  - name: Tier Status
    expr: tier_status
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
  - name: Avg Order Value
    expr: SUM(total_spend) / NULLIF(SUM(total_orders), 0)
  - name: Points Earned
    expr: SUM(lifetime_points_earned)
  - name: Points Redeemed
    expr: SUM(lifetime_points_redeemed)
  - name: Points Balance
    expr: SUM(points_balance)
  - name: Avg Points Balance
    expr: AVG(points_balance)
  - name: Outstanding Liability PHP
    expr: SUM(points_liability_php)
  - name: Redemption Rate
    expr: SUM(lifetime_points_redeemed) * 1.0 / NULLIF(SUM(lifetime_points_earned), 0)
  - name: Members Redeeming
    expr: COUNT(DISTINCT CASE WHEN redemptions_count > 0 THEN customer_id END)
  - name: Redemption Penetration
    expr: COUNT(DISTINCT CASE WHEN redemptions_count > 0 THEN customer_id END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN total_orders > 0 THEN customer_id END), 0)
  - name: Upgrade Eligible Members
    expr: COUNT(DISTINCT CASE WHEN tier_status = 'Upgrade eligible' THEN customer_id END)
  - name: Downgrade Risk Members
    expr: COUNT(DISTINCT CASE WHEN tier_status = 'Downgrade risk' THEN customer_id END)
  - name: High Churn Risk Members
    expr: COUNT(DISTINCT CASE WHEN churn_risk = 'High' THEN customer_id END)
  - name: Est. CLV
    expr: SUM(clv_estimate)
$$
""")
print(f"✓ metric view {catalog}.{gold}.c360_metrics")

# COMMAND ----------

dbutils.notebook.exit("Metric view created")
