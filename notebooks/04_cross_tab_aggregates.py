# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Cross-Tab Aggregates (Gold Layer)
# MAGIC
# MAGIC Produces all pre-computed cross-tab tables consumed by the **E-Commerce Analytics HTML report**
# MAGIC (`public/reports/e-commerce-analytics.html` in the portfolio website repo).
# MAGIC
# MAGIC ### Why pre-compute?
# MAGIC The HTML report is a **static file** served from Next.js `/public` — it has no runtime
# MAGIC database connection. All cross-filter interactions (click delivery bar → all visuals update)
# MAGIC are resolved via in-memory JS lookups against constants embedded in the HTML.
# MAGIC These constants are sourced directly from the tables created here.
# MAGIC
# MAGIC ### Pipeline position
# MAGIC ```
# MAGIC Bronze (raw) → Silver (SCD2, dedup) → Gold (fact_orders, agg_*)
# MAGIC                                               ↓
# MAGIC                                    Gold Cross-Tabs (this notebook)
# MAGIC                                               ↓
# MAGIC                                    HTML Report (static embed)
# MAGIC ```
# MAGIC
# MAGIC ### Tables created (all in `portfolio_de.gold`)
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `cross_tab_kpi_by_delivery` | KPI summary per delivery speed bucket |
# MAGIC | `cross_tab_kpi_by_review` | KPI summary per review star (1–5) |
# MAGIC | `cross_tab_kpi_by_category` | KPI summary per top-5 product category |
# MAGIC | `cross_tab_monthly_by_delivery` | Monthly GMV × delivery bucket (20 months) |
# MAGIC | `cross_tab_monthly_by_review` | Monthly GMV × review star (20 months) |
# MAGIC | `cross_tab_monthly_by_category` | Monthly GMV × category (20 months) |
# MAGIC | `cross_tab_category_by_delivery` | Category % share within each delivery bucket |
# MAGIC | `cross_tab_category_by_review` | Category % share within each review star |
# MAGIC | `cross_tab_state_by_delivery` | State % share within each delivery bucket |
# MAGIC | `cross_tab_state_by_review` | State % share within each review star |
# MAGIC | `cross_tab_state_by_category` | State % share within each category |
# MAGIC | `cross_tab_review_by_delivery` | Review distribution within each delivery bucket |
# MAGIC | `cross_tab_delivery_by_review` | Delivery distribution within each review star |
# MAGIC | `cross_tab_delivery_by_category` | Delivery distribution within each category |
# MAGIC | `cross_tab_review_by_category` | Review distribution within each category |

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

spark.sql("USE CATALOG portfolio_de")
spark.sql("USE SCHEMA gold")

# Reusable SQL snippets — embedded as Python strings for DRY queries
# ─────────────────────────────────────────────────────────────────────────────
# delivery_bucket: 5-bucket categorisation of delivery_days
DELIVERY_CASE = """
  CASE
    WHEN delivery_days <= 3  THEN '1-3d'
    WHEN delivery_days <= 7  THEN '4-7d'
    WHEN delivery_days <= 14 THEN '8-14d'
    WHEN delivery_days <= 21 THEN '15-21d'
    ELSE '22+d'
  END
"""

# cat_group: maps raw category_english to 5 display groups + Other
CATEGORY_CASE = """
  CASE
    WHEN category_english = 'health_beauty'        THEN 'Health & Beauty'
    WHEN category_english = 'watches_gifts'        THEN 'Watches & Gifts'
    WHEN category_english = 'bed_bath_table'       THEN 'Bed & Bath'
    WHEN category_english = 'sports_leisure'       THEN 'Sports & Leisure'
    WHEN category_english = 'computers_accessories' THEN 'Computers'
    ELSE 'Other'
  END
"""

# Top-5 category filter (excludes Other for category-filtered KPI tables)
TOP5_FILTER = """
  category_english IN (
    'health_beauty','watches_gifts','bed_bath_table',
    'sports_leisure','computers_accessories'
  )
"""

# Top-8 states filter (used for state distribution tables)
TOP8_STATES = "('SP','RJ','MG','RS','PR','SC','BA','DF')"

print("Setup complete — catalog: portfolio_de, schema: gold")

# COMMAND ----------

# MAGIC %md ## 1 · KPI by Delivery Bucket

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_kpi_by_delivery
COMMENT 'KPI summary per delivery speed bucket. Source: fact_orders. Used by HTML report cross-filter.'
AS
WITH base AS (
  SELECT
    {DELIVERY_CASE} AS delivery_bucket,
    order_id,
    item_price,
    delivery_days,
    review_score
  FROM portfolio_de.gold.fact_orders
  WHERE delivery_days IS NOT NULL
)
SELECT
  delivery_bucket,
  COUNT(DISTINCT order_id)          AS orders,
  ROUND(SUM(item_price), 0)         AS gmv,
  ROUND(SUM(item_price)
        / COUNT(DISTINCT order_id), 2) AS aov,
  ROUND(AVG(review_score), 2)       AS avg_review,
  ROUND(AVG(delivery_days), 1)      AS avg_delivery_days,
  -- rank for ordering in report (fast → slow)
  CASE delivery_bucket
    WHEN '1-3d'   THEN 1
    WHEN '4-7d'   THEN 2
    WHEN '8-14d'  THEN 3
    WHEN '15-21d' THEN 4
    ELSE 5
  END AS sort_order
FROM base
GROUP BY delivery_bucket
ORDER BY sort_order
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_kpi_by_delivery"))

# COMMAND ----------

# MAGIC %md ## 2 · KPI by Review Star

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_kpi_by_review
COMMENT 'KPI summary per review score (1-5). Source: fact_orders. Used by HTML report cross-filter.'
AS
SELECT
  review_score,
  COUNT(DISTINCT order_id)             AS orders,
  ROUND(SUM(item_price), 0)            AS gmv,
  ROUND(SUM(item_price)
        / COUNT(DISTINCT order_id), 2) AS aov,
  ROUND(AVG(delivery_days), 1)         AS avg_delivery_days
FROM portfolio_de.gold.fact_orders
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_kpi_by_review"))

# COMMAND ----------

# MAGIC %md ## 3 · KPI by Category (Top 5)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_kpi_by_category
COMMENT 'KPI summary for top-5 product categories. Source: fact_orders. Used by HTML report cross-filter.'
AS
SELECT
  {CATEGORY_CASE} AS cat_group,
  COUNT(DISTINCT order_id)             AS orders,
  ROUND(SUM(item_price), 0)            AS gmv,
  ROUND(SUM(item_price)
        / COUNT(DISTINCT order_id), 2) AS aov,
  ROUND(AVG(review_score), 2)          AS avg_review,
  ROUND(AVG(delivery_days), 1)         AS avg_delivery_days
FROM portfolio_de.gold.fact_orders
WHERE {TOP5_FILTER}
GROUP BY cat_group
ORDER BY gmv DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_kpi_by_category"))

# COMMAND ----------

# MAGIC %md ## 4 · Monthly GMV × Delivery Bucket

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_monthly_by_delivery
COMMENT 'Monthly GMV broken down by delivery speed bucket (20 months: Jan 2017 – Aug 2018). Used by HTML report revenue chart cross-filter.'
AS
WITH base AS (
  SELECT
    year_month,
    {DELIVERY_CASE} AS delivery_bucket,
    item_price
  FROM portfolio_de.gold.fact_orders
  WHERE delivery_days IS NOT NULL
    AND year_month IS NOT NULL
    AND year_month BETWEEN '2017-01' AND '2018-08'
)
SELECT
  year_month,
  delivery_bucket,
  ROUND(SUM(item_price), 0) AS gmv,
  CASE delivery_bucket
    WHEN '1-3d'   THEN 1
    WHEN '4-7d'   THEN 2
    WHEN '8-14d'  THEN 3
    WHEN '15-21d' THEN 4
    ELSE 5
  END AS bucket_sort
FROM base
GROUP BY year_month, delivery_bucket
ORDER BY year_month, bucket_sort
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_monthly_by_delivery ORDER BY year_month, bucket_sort"))

# COMMAND ----------

# MAGIC %md ## 5 · Monthly GMV × Review Star

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_monthly_by_review
COMMENT 'Monthly GMV broken down by review score (1-5). Used by HTML report revenue chart cross-filter.'
AS
SELECT
  year_month,
  review_score,
  ROUND(SUM(item_price), 0) AS gmv
FROM portfolio_de.gold.fact_orders
WHERE review_score IS NOT NULL
  AND year_month IS NOT NULL
  AND year_month BETWEEN '2017-01' AND '2018-08'
GROUP BY year_month, review_score
ORDER BY year_month, review_score
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_monthly_by_review ORDER BY year_month, review_score"))

# COMMAND ----------

# MAGIC %md ## 6 · Monthly GMV × Category

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_monthly_by_category
COMMENT 'Monthly GMV for top-5 categories + Other. Used by HTML report revenue chart cross-filter.'
AS
WITH base AS (
  SELECT
    year_month,
    {CATEGORY_CASE} AS cat_group,
    item_price
  FROM portfolio_de.gold.fact_orders
  WHERE year_month IS NOT NULL
    AND year_month BETWEEN '2017-01' AND '2018-08'
)
SELECT
  year_month,
  cat_group,
  ROUND(SUM(item_price), 0) AS gmv
FROM base
GROUP BY year_month, cat_group
ORDER BY year_month, cat_group
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_monthly_by_category ORDER BY year_month, cat_group"))

# COMMAND ----------

# MAGIC %md ## 7 · Category Distribution × Delivery Bucket

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_category_by_delivery
COMMENT 'Category % share within each delivery bucket. Used by HTML report donut chart cross-filter.'
AS
WITH base AS (
  SELECT
    {DELIVERY_CASE} AS delivery_bucket,
    {CATEGORY_CASE} AS cat_group
  FROM portfolio_de.gold.fact_orders
  WHERE delivery_days IS NOT NULL
),
counts AS (
  SELECT delivery_bucket, cat_group, COUNT(*) AS order_count
  FROM base
  GROUP BY delivery_bucket, cat_group
)
SELECT
  delivery_bucket,
  cat_group,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY delivery_bucket), 2) AS pct
FROM counts
ORDER BY delivery_bucket, pct DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_category_by_delivery ORDER BY delivery_bucket, pct DESC"))

# COMMAND ----------

# MAGIC %md ## 8 · Category Distribution × Review Star

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_category_by_review
COMMENT 'Category % share within each review score. Used by HTML report donut chart cross-filter.'
AS
WITH base AS (
  SELECT
    review_score,
    {CATEGORY_CASE} AS cat_group
  FROM portfolio_de.gold.fact_orders
  WHERE review_score IS NOT NULL
),
counts AS (
  SELECT review_score, cat_group, COUNT(*) AS order_count
  FROM base
  GROUP BY review_score, cat_group
)
SELECT
  review_score,
  cat_group,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY review_score), 2) AS pct
FROM counts
ORDER BY review_score, pct DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_category_by_review ORDER BY review_score, pct DESC"))

# COMMAND ----------

# MAGIC %md ## 9 · State Distribution × Delivery Bucket

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_state_by_delivery
COMMENT 'Top-8 state % share within each delivery bucket. Used by HTML report state bars cross-filter.'
AS
WITH base AS (
  SELECT
    {DELIVERY_CASE} AS delivery_bucket,
    customer_state
  FROM portfolio_de.gold.fact_orders
  WHERE delivery_days IS NOT NULL
    AND customer_state IS NOT NULL
    AND customer_state IN {TOP8_STATES}
),
counts AS (
  SELECT delivery_bucket, customer_state, COUNT(*) AS order_count
  FROM base
  GROUP BY delivery_bucket, customer_state
)
SELECT
  delivery_bucket,
  customer_state,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY delivery_bucket), 2) AS pct
FROM counts
ORDER BY delivery_bucket, pct DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_state_by_delivery ORDER BY delivery_bucket, pct DESC"))

# COMMAND ----------

# MAGIC %md ## 10 · State Distribution × Review Star

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_state_by_review
COMMENT 'Top-8 state % share within each review score. Used by HTML report state bars cross-filter.'
AS
WITH base AS (
  SELECT
    review_score,
    customer_state
  FROM portfolio_de.gold.fact_orders
  WHERE review_score IS NOT NULL
    AND customer_state IS NOT NULL
    AND customer_state IN {TOP8_STATES}
),
counts AS (
  SELECT review_score, customer_state, COUNT(*) AS order_count
  FROM base
  GROUP BY review_score, customer_state
)
SELECT
  review_score,
  customer_state,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY review_score), 2) AS pct
FROM counts
ORDER BY review_score, pct DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_state_by_review ORDER BY review_score, pct DESC"))

# COMMAND ----------

# MAGIC %md ## 11 · State Distribution × Category

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_state_by_category
COMMENT 'Top-8 state % share within each top-5 category. Used by HTML report state bars cross-filter.'
AS
WITH base AS (
  SELECT
    {CATEGORY_CASE} AS cat_group,
    customer_state
  FROM portfolio_de.gold.fact_orders
  WHERE {TOP5_FILTER}
    AND customer_state IS NOT NULL
    AND customer_state IN {TOP8_STATES}
),
counts AS (
  SELECT cat_group, customer_state, COUNT(*) AS order_count
  FROM base
  GROUP BY cat_group, customer_state
)
SELECT
  cat_group,
  customer_state,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY cat_group), 2) AS pct
FROM counts
ORDER BY cat_group, pct DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_state_by_category ORDER BY cat_group, pct DESC"))

# COMMAND ----------

# MAGIC %md ## 12 · Review Distribution × Delivery Bucket

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_review_by_delivery
COMMENT 'Review score % distribution within each delivery bucket. Used by HTML report review bars cross-filter.'
AS
WITH base AS (
  SELECT
    {DELIVERY_CASE} AS delivery_bucket,
    review_score
  FROM portfolio_de.gold.fact_orders
  WHERE delivery_days IS NOT NULL
    AND review_score IS NOT NULL
),
counts AS (
  SELECT delivery_bucket, review_score, COUNT(*) AS order_count
  FROM base
  GROUP BY delivery_bucket, review_score
)
SELECT
  delivery_bucket,
  review_score,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY delivery_bucket), 2) AS pct
FROM counts
ORDER BY delivery_bucket, review_score DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_review_by_delivery ORDER BY delivery_bucket, review_score DESC"))

# COMMAND ----------

# MAGIC %md ## 13 · Delivery Distribution × Review Star

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_delivery_by_review
COMMENT 'Delivery bucket % distribution within each review score. Used by HTML report delivery bars cross-filter.'
AS
WITH base AS (
  SELECT
    review_score,
    {DELIVERY_CASE} AS delivery_bucket
  FROM portfolio_de.gold.fact_orders
  WHERE review_score IS NOT NULL
    AND delivery_days IS NOT NULL
),
counts AS (
  SELECT review_score, delivery_bucket, COUNT(*) AS order_count
  FROM base
  GROUP BY review_score, delivery_bucket
)
SELECT
  review_score,
  delivery_bucket,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY review_score), 2) AS pct,
  CASE delivery_bucket
    WHEN '1-3d'   THEN 1
    WHEN '4-7d'   THEN 2
    WHEN '8-14d'  THEN 3
    WHEN '15-21d' THEN 4
    ELSE 5
  END AS bucket_sort
FROM counts
ORDER BY review_score, bucket_sort
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_delivery_by_review ORDER BY review_score, bucket_sort"))

# COMMAND ----------

# MAGIC %md ## 14 · Delivery Distribution × Category

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_delivery_by_category
COMMENT 'Delivery bucket % distribution within each top-5 category. Used by HTML report delivery bars cross-filter.'
AS
WITH base AS (
  SELECT
    {CATEGORY_CASE} AS cat_group,
    {DELIVERY_CASE} AS delivery_bucket
  FROM portfolio_de.gold.fact_orders
  WHERE {TOP5_FILTER}
    AND delivery_days IS NOT NULL
),
counts AS (
  SELECT cat_group, delivery_bucket, COUNT(*) AS order_count
  FROM base
  GROUP BY cat_group, delivery_bucket
)
SELECT
  cat_group,
  delivery_bucket,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY cat_group), 2) AS pct,
  CASE delivery_bucket
    WHEN '1-3d'   THEN 1
    WHEN '4-7d'   THEN 2
    WHEN '8-14d'  THEN 3
    WHEN '15-21d' THEN 4
    ELSE 5
  END AS bucket_sort
FROM counts
ORDER BY cat_group, bucket_sort
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_delivery_by_category ORDER BY cat_group, bucket_sort"))

# COMMAND ----------

# MAGIC %md ## 15 · Review Distribution × Category

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE portfolio_de.gold.cross_tab_review_by_category
COMMENT 'Review score % distribution within each top-5 category. Used by HTML report review bars cross-filter.'
AS
WITH base AS (
  SELECT
    {CATEGORY_CASE} AS cat_group,
    review_score
  FROM portfolio_de.gold.fact_orders
  WHERE {TOP5_FILTER}
    AND review_score IS NOT NULL
),
counts AS (
  SELECT cat_group, review_score, COUNT(*) AS order_count
  FROM base
  GROUP BY cat_group, review_score
)
SELECT
  cat_group,
  review_score,
  order_count,
  ROUND(100.0 * order_count / SUM(order_count) OVER (PARTITION BY cat_group), 2) AS pct
FROM counts
ORDER BY cat_group, review_score DESC
""")

display(spark.sql("SELECT * FROM portfolio_de.gold.cross_tab_review_by_category ORDER BY cat_group, review_score DESC"))

# COMMAND ----------

# MAGIC %md ## Summary — Tables Created

# COMMAND ----------

summary = spark.sql("""
  SELECT table_name, comment
  FROM portfolio_de.information_schema.tables
  WHERE table_schema = 'gold'
    AND table_name LIKE 'cross_tab_%'
  ORDER BY table_name
""")

display(summary)

# Row counts for quick sanity check
print("\n── Row counts ──────────────────────────────────────────")
cross_tab_tables = [
  "cross_tab_kpi_by_delivery",
  "cross_tab_kpi_by_review",
  "cross_tab_kpi_by_category",
  "cross_tab_monthly_by_delivery",
  "cross_tab_monthly_by_review",
  "cross_tab_monthly_by_category",
  "cross_tab_category_by_delivery",
  "cross_tab_category_by_review",
  "cross_tab_state_by_delivery",
  "cross_tab_state_by_review",
  "cross_tab_state_by_category",
  "cross_tab_review_by_delivery",
  "cross_tab_delivery_by_review",
  "cross_tab_delivery_by_category",
  "cross_tab_review_by_category",
]
for t in cross_tab_tables:
  count = spark.sql(f"SELECT COUNT(*) AS n FROM portfolio_de.gold.{t}").collect()[0]['n']
  print(f"  {t:<42} {count:>5} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for HTML Report Maintenance
# MAGIC
# MAGIC When refreshing the portfolio report with new data:
# MAGIC
# MAGIC 1. **Re-run notebooks in order:** `01_bronze` → `02_silver` → `03_gold` → `04_cross_tab` (this notebook)
# MAGIC 2. **Export cross-tab values** from these tables into the `CROSS_DATA` JS constant
# MAGIC    in `public/reports/e-commerce-analytics.html`
# MAGIC 3. **No Databricks connection at runtime** — the HTML report is fully self-contained
# MAGIC
# MAGIC ### Key mappings (Databricks table → HTML constant)
# MAGIC | Table | `CROSS_DATA` key |
# MAGIC |---|---|
# MAGIC | `cross_tab_kpi_by_delivery` | `CROSS_DATA.delivery['1-3d'].kpi`, etc. |
# MAGIC | `cross_tab_monthly_by_delivery` | `CROSS_DATA.delivery['1-3d'].monthly`, etc. |
# MAGIC | `cross_tab_category_by_delivery` | `CROSS_DATA.delivery['1-3d'].categories`, etc. |
# MAGIC | `cross_tab_state_by_delivery` | `CROSS_DATA.delivery['1-3d'].states`, etc. |
# MAGIC | `cross_tab_review_by_delivery` | `CROSS_DATA.delivery['1-3d'].reviews`, etc. |
# MAGIC | `cross_tab_kpi_by_review` | `CROSS_DATA.review['5'].kpi`, etc. |
# MAGIC | `cross_tab_monthly_by_review` | `CROSS_DATA.review['5'].monthly`, etc. |
# MAGIC | *(and so on for all 15 tables)* | |
