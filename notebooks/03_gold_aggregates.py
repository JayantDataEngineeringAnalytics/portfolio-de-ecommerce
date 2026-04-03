# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Aggregates
# MAGIC **portfolio_de · E-Commerce Analytics Pipeline**
# MAGIC
# MAGIC Reads from `portfolio_de.silver`, builds five Gold tables optimised for BI queries,
# MAGIC and writes to `portfolio_de.gold`.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `portfolio_de.silver.*` |
# MAGIC | **Target** | `portfolio_de.gold.*` |
# MAGIC | **Write mode** | `CREATE OR REPLACE TABLE` (idempotent) |
# MAGIC
# MAGIC **Gold tables produced:**
# MAGIC | Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `fact_orders` | One row per order_item | Central BI fact table |
# MAGIC | `agg_monthly_revenue` | One row per year-month | Revenue trend chart |
# MAGIC | `agg_seller_performance` | One row per seller | Seller ranking table |
# MAGIC | `agg_product_category` | One row per category | Category mix chart |
# MAGIC | `agg_delivery_performance` | One row per delivery bucket | Delivery speed chart |
# MAGIC
# MAGIC **Run order:** Notebook 3 of 3. Run after `02_silver_transform.py`.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOG       = "portfolio_de"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

spark.sql(f"USE CATALOG {CATALOG}")

print(f"Catalog : {CATALOG}")
print(f"Source  : {SILVER_SCHEMA}")
print(f"Target  : {GOLD_SCHEMA}")
print(f"Started : {datetime.now():%Y-%m-%d %H:%M:%S}")

# COMMAND ----------

# MAGIC %md ## 1. fact_orders — BI-ready fact table (one row per order_item)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.fact_orders
COMMENT 'Gold — fact table. One row per order_item. Optimised for BI slicing by date, category, state, seller.'
AS
SELECT
  order_id, order_item_id, customer_id, customer_unique_id,
  product_id, seller_id, order_status,
  order_purchase_date, order_purchase_timestamp,
  order_delivered_customer_date, order_estimated_delivery_date,
  item_price, freight_value, total_payment_value,
  payment_type, payment_installments,
  review_score,
  customer_city, customer_state,
  category_english, product_category_name,
  product_weight_g, seller_city, seller_state,
  delivery_days, estimated_days, delivery_delay_days, is_late_delivery,
  purchase_year, purchase_month, purchase_quarter, year_month,
  current_timestamp() AS _updated_ts
FROM {CATALOG}.{SILVER_SCHEMA}.orders_enriched
""")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders").count()
print(f"✓ fact_orders: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 2. agg_monthly_revenue — monthly GMV trend

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.agg_monthly_revenue
COMMENT 'Gold — monthly GMV, order count, AOV, on-time %. Primary source for revenue trend visuals.'
AS
SELECT
  purchase_year                                               AS year,
  purchase_month                                              AS month,
  year_month,
  COUNT(DISTINCT order_id)                                    AS total_orders,
  COUNT(*)                                                    AS total_items,
  ROUND(SUM(item_price), 2)                                   AS total_revenue,
  ROUND(SUM(freight_value), 2)                                AS total_freight,
  ROUND(SUM(item_price) / COUNT(DISTINCT order_id), 2)        AS avg_order_value,
  COUNT(DISTINCT customer_unique_id)                          AS unique_customers,
  ROUND(AVG(review_score), 3)                                 AS avg_review_score,
  SUM(CASE WHEN is_late_delivery = FALSE
            AND order_status = 'delivered' THEN 1 ELSE 0 END) AS on_time_deliveries,
  SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
  current_timestamp()                                         AS _updated_ts
FROM {CATALOG}.{SILVER_SCHEMA}.orders_enriched
WHERE purchase_year IS NOT NULL
GROUP BY purchase_year, purchase_month, year_month
ORDER BY purchase_year, purchase_month
""")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_monthly_revenue").count()
print(f"✓ agg_monthly_revenue: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 3. agg_seller_performance — seller ranking

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.agg_seller_performance
COMMENT 'Gold — seller-level aggregates: GMV, delivery speed, on-time %, review score.'
AS
SELECT
  seller_id, seller_city, seller_state,
  COUNT(DISTINCT order_id)                                     AS total_orders,
  COUNT(*)                                                     AS total_items,
  ROUND(SUM(item_price), 2)                                    AS total_revenue,
  ROUND(AVG(item_price), 2)                                    AS avg_order_value,
  COUNT(DISTINCT category_english)                             AS distinct_categories,
  COUNT(DISTINCT customer_unique_id)                           AS unique_customers,
  ROUND(AVG(CASE WHEN delivery_days IS NOT NULL
                 THEN delivery_days END), 2)                   AS avg_delivery_days,
  ROUND(AVG(review_score), 3)                                  AS avg_review_score,
  SUM(CASE WHEN is_late_delivery = FALSE
            AND order_status = 'delivered' THEN 1 ELSE 0 END)  AS on_time_deliveries,
  SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END)  AS delivered_orders,
  ROUND(
    SUM(CASE WHEN is_late_delivery = FALSE
              AND order_status = 'delivered' THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END), 0)
  , 1)                                                         AS on_time_pct,
  MIN(order_purchase_date)                                     AS first_order_date,
  MAX(order_purchase_date)                                     AS last_order_date,
  current_timestamp()                                          AS _updated_ts
FROM {CATALOG}.{SILVER_SCHEMA}.orders_enriched
WHERE seller_id IS NOT NULL
GROUP BY seller_id, seller_city, seller_state
""")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_seller_performance").count()
print(f"✓ agg_seller_performance: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 4. agg_product_category — category mix & performance

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.agg_product_category
COMMENT 'Gold — category-level aggregates: GMV share, AOV, review score, delivery speed.'
AS
WITH totals AS (
  SELECT SUM(item_price) AS grand_total
  FROM {CATALOG}.{SILVER_SCHEMA}.orders_enriched
)
SELECT
  e.category_english,
  e.product_category_name,
  COUNT(DISTINCT e.order_id)                                    AS total_orders,
  COUNT(*)                                                      AS total_items,
  ROUND(SUM(e.item_price), 2)                                   AS total_revenue,
  ROUND(SUM(e.item_price) * 100.0 / t.grand_total, 2)           AS revenue_share_pct,
  ROUND(SUM(e.item_price) / COUNT(DISTINCT e.order_id), 2)      AS avg_order_value,
  COUNT(DISTINCT e.seller_id)                                   AS distinct_sellers,
  ROUND(AVG(e.review_score), 3)                                 AS avg_review_score,
  ROUND(AVG(CASE WHEN e.delivery_days IS NOT NULL
                 THEN e.delivery_days END), 1)                  AS avg_delivery_days,
  ROUND(AVG(e.product_weight_g), 0)                             AS avg_weight_g,
  current_timestamp()                                           AS _updated_ts
FROM {CATALOG}.{SILVER_SCHEMA}.orders_enriched e
CROSS JOIN totals t
WHERE e.category_english IS NOT NULL
GROUP BY e.category_english, e.product_category_name, t.grand_total
ORDER BY total_revenue DESC
""")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_product_category").count()
print(f"✓ agg_product_category: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 5. agg_delivery_performance — delivery speed buckets

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.agg_delivery_performance
COMMENT 'Gold — delivery speed distribution and avg review score per speed band.'
AS
WITH bucketed AS (
  SELECT
    CASE
      WHEN delivery_days BETWEEN 0  AND 3  THEN '1-3 days'
      WHEN delivery_days BETWEEN 4  AND 7  THEN '4-7 days'
      WHEN delivery_days BETWEEN 8  AND 14 THEN '8-14 days'
      WHEN delivery_days BETWEEN 15 AND 21 THEN '15-21 days'
      WHEN delivery_days > 21              THEN '22+ days'
    END AS delivery_bucket,
    CASE
      WHEN delivery_days BETWEEN 0  AND 3  THEN 1
      WHEN delivery_days BETWEEN 4  AND 7  THEN 2
      WHEN delivery_days BETWEEN 8  AND 14 THEN 3
      WHEN delivery_days BETWEEN 15 AND 21 THEN 4
      WHEN delivery_days > 21              THEN 5
    END AS bucket_order,
    review_score, order_id
  FROM {CATALOG}.{SILVER_SCHEMA}.orders_enriched
  WHERE order_status = 'delivered' AND delivery_days IS NOT NULL
),
totals AS (SELECT COUNT(DISTINCT order_id) AS total FROM bucketed)
SELECT
  b.delivery_bucket, b.bucket_order,
  COUNT(DISTINCT b.order_id)                              AS order_count,
  ROUND(COUNT(DISTINCT b.order_id) * 100.0 / t.total, 1) AS pct_of_delivered,
  ROUND(AVG(b.review_score), 2)                           AS avg_review_score,
  current_timestamp()                                     AS _updated_ts
FROM bucketed b CROSS JOIN totals t
WHERE b.delivery_bucket IS NOT NULL
GROUP BY b.delivery_bucket, b.bucket_order, t.total
ORDER BY b.bucket_order
""")

count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.agg_delivery_performance").count()
print(f"✓ agg_delivery_performance: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 6. Final validation

# COMMAND ----------

print(f"{'Table':<30} {'Rows':>10}")
print("-" * 43)
for t in ["fact_orders","agg_monthly_revenue","agg_seller_performance","agg_product_category","agg_delivery_performance"]:
    n = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{t}").count()
    print(f"  {t:<28} {n:>10,}")

# COMMAND ----------

# MAGIC %md ## 7. Key metrics summary

# COMMAND ----------

spark.sql(f"""
SELECT
  COUNT(DISTINCT order_id)                                       AS total_orders,
  COUNT(DISTINCT customer_unique_id)                             AS unique_customers,
  ROUND(SUM(item_price), 0)                                      AS total_gmv_brl,
  ROUND(SUM(item_price) / COUNT(DISTINCT order_id), 2)           AS aov,
  ROUND(AVG(review_score), 2)                                    AS avg_review_score,
  ROUND(AVG(CASE WHEN delivery_days IS NOT NULL
                 THEN delivery_days END), 1)                     AS avg_delivery_days,
  ROUND(SUM(CASE WHEN is_late_delivery = TRUE  THEN 1 ELSE 0 END) * 100.0
      / SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END), 1) AS late_delivery_pct
FROM {CATALOG}.{GOLD_SCHEMA}.fact_orders
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gold layer complete
# MAGIC
# MAGIC | Table | Rows | Key insight |
# MAGIC |---|---|---|
# MAGIC | fact_orders | 112,650 | One row per order-item, all dims resolved |
# MAGIC | agg_monthly_revenue | 24 | Nov 2017 peak: R$1.01M (Black Friday) |
# MAGIC | agg_seller_performance | 3,095 | Top seller: R$229K, 88.4% on-time |
# MAGIC | agg_product_category | 73 | #1 Health & Beauty: R$1.26M (9.26%) |
# MAGIC | agg_delivery_performance | 5 | 39.4% of orders: 8–14 days; 22+ days → 3.02★ avg |
# MAGIC
# MAGIC **Verified KPIs (real data):**
# MAGIC - Total GMV: **R$13.59M** | Orders: **98,666** | Customers: **95,420**
# MAGIC - AOV: **R$137.75** | Review score: **4.03★** | Avg delivery: **12.4 days**
# MAGIC - Late deliveries: **8,715** (7.9% of delivered orders)
