# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation
# MAGIC **portfolio_de · E-Commerce Analytics Pipeline**
# MAGIC
# MAGIC Reads from `portfolio_de.bronze`, applies data quality rules, joins all 9 datasets,
# MAGIC implements SCD Type 2 on customers, and writes clean tables to `portfolio_de.silver`.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `portfolio_de.bronze.*` |
# MAGIC | **Target** | `portfolio_de.silver.*` |
# MAGIC | **Key patterns** | SCD Type 2, deduplication, INITCAP normalisation, derived metrics |
# MAGIC
# MAGIC **Silver tables produced:**
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `dim_customers` | SCD Type 2 — valid_from, valid_to, is_current |
# MAGIC | `dim_products` | Fixed column names, English categories, volume_cm3 |
# MAGIC | `dim_sellers` | Standardised city/state casing |
# MAGIC | `order_reviews_deduped` | One review per order (latest by review_answer_timestamp) |
# MAGIC | `order_payments_agg` | One payment row per order (sum + dominant method) |
# MAGIC | `orders_enriched` | **Central Silver table** — one row per order_item, all dimensions joined, delivery metrics derived |
# MAGIC
# MAGIC **Run order:** Notebook 2 of 3. Run after `01_bronze_ingestion.py`, before `03_gold_aggregates.py`.

# COMMAND ----------

# MAGIC %md ## 0. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

CATALOG        = "portfolio_de"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"

spark.sql(f"USE CATALOG {CATALOG}")

print(f"Catalog : {CATALOG}")
print(f"Source  : {BRONZE_SCHEMA}")
print(f"Target  : {SILVER_SCHEMA}")
print(f"Started : {datetime.now():%Y-%m-%d %H:%M:%S}")

# COMMAND ----------

# MAGIC %md ## 1. dim_customers — SCD Type 2
# MAGIC
# MAGIC The Olist dataset is a static snapshot, so every customer has a single record.
# MAGIC We implement the SCD Type 2 **pattern** (valid_from / valid_to / is_current)
# MAGIC to demonstrate how the table would behave in a production streaming pipeline
# MAGIC where customer attributes (city, state) can change over time.
# MAGIC
# MAGIC - `valid_from` = date of customer's first order (earliest proxy for record creation)
# MAGIC - `valid_to` = NULL (current record, no superseded version exists)
# MAGIC - `is_current` = TRUE for all rows (single snapshot)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.dim_customers
COMMENT 'Silver — customers with SCD Type 2 columns. valid_from derived from first order date.'
AS
WITH first_orders AS (
  SELECT
    customer_id,
    MIN(CAST(order_purchase_timestamp AS DATE)) AS first_order_date
  FROM {CATALOG}.{BRONZE_SCHEMA}.orders
  WHERE order_purchase_timestamp IS NOT NULL
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.customer_unique_id,
  c.customer_zip_code_prefix,
  INITCAP(TRIM(c.customer_city))  AS customer_city,
  UPPER(TRIM(c.customer_state))   AS customer_state,
  COALESCE(fo.first_order_date, CAST('2016-01-01' AS DATE)) AS valid_from,
  CAST(NULL AS DATE)              AS valid_to,
  TRUE                            AS is_current,
  current_timestamp()             AS _updated_ts
FROM {CATALOG}.{BRONZE_SCHEMA}.customers c
LEFT JOIN first_orders fo ON c.customer_id = fo.customer_id
""")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customers").count()
print(f"✓ dim_customers: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 2. dim_products — column rename + English categories + derived volume

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.dim_products
COMMENT 'Silver — products: fixed typo column names, English category via join, derived volume_cm3.'
AS
SELECT
  p.product_id,
  p.product_category_name,
  COALESCE(ct.product_category_name_english, p.product_category_name) AS category_english,
  p.product_name_lenght        AS product_name_length,
  p.product_description_lenght AS product_description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,
  (p.product_length_cm * p.product_height_cm * p.product_width_cm) AS volume_cm3,
  current_timestamp()          AS _updated_ts
FROM {CATALOG}.{BRONZE_SCHEMA}.products p
LEFT JOIN {CATALOG}.{BRONZE_SCHEMA}.category_translation ct
  ON p.product_category_name = ct.product_category_name
""")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_products").count()
print(f"✓ dim_products: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 3. dim_sellers — standardise city/state casing

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.dim_sellers
COMMENT 'Silver — sellers with standardised INITCAP city and UPPER state.'
AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  INITCAP(TRIM(seller_city))  AS seller_city,
  UPPER(TRIM(seller_state))   AS seller_state,
  current_timestamp()         AS _updated_ts
FROM {CATALOG}.{BRONZE_SCHEMA}.sellers
""")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_sellers").count()
print(f"✓ dim_sellers: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 4. order_reviews_deduped — one review per order
# MAGIC
# MAGIC Bronze contains **104,162 reviews** for **99,742 distinct orders** — a delta of ~4,420
# MAGIC multi-review orders. We keep the most recent review per order using `ROW_NUMBER()`
# MAGIC partitioned by `order_id`, ordered by `review_answer_timestamp DESC`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.order_reviews_deduped
COMMENT 'Silver — one review per order. Duplicates resolved by latest review_answer_timestamp.'
AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY review_answer_timestamp DESC NULLS LAST,
               review_creation_date    DESC NULLS LAST
    ) AS rn
  FROM {CATALOG}.{BRONZE_SCHEMA}.order_reviews
  WHERE review_id IS NOT NULL AND order_id IS NOT NULL
)
SELECT
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_date,
  review_answer_timestamp,
  current_timestamp() AS _updated_ts
FROM ranked
WHERE rn = 1
""")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.order_reviews_deduped").count()
print(f"✓ order_reviews_deduped: {count:,} rows (from 104,162 Bronze rows)")

# COMMAND ----------

# MAGIC %md ## 5. order_payments_agg — one row per order
# MAGIC
# MAGIC Bronze has **103,886 payment rows** for **99,440 orders** — 4,526 orders used split
# MAGIC payment methods (e.g. credit card + voucher). We aggregate to one row per order:
# MAGIC - `total_payment_value` = SUM of all payment parts
# MAGIC - `dominant_payment_type` = payment method with the highest individual value
# MAGIC - `installments` = installment count for the dominant method

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.order_payments_agg
COMMENT 'Silver — payments aggregated to one row per order. Dominant type = highest-value method.'
AS
WITH ranked_payments AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY payment_value DESC NULLS LAST
    ) AS rn
  FROM {CATALOG}.{BRONZE_SCHEMA}.order_payments
  WHERE order_id IS NOT NULL
)
SELECT
  order_id,
  SUM(payment_value)                          AS total_payment_value,
  COUNT(*)                                    AS payment_parts,
  MAX(CASE WHEN rn = 1 THEN payment_type       END) AS dominant_payment_type,
  MAX(CASE WHEN rn = 1 THEN payment_installments END) AS installments,
  current_timestamp()                         AS _updated_ts
FROM ranked_payments
GROUP BY order_id
""")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.order_payments_agg").count()
print(f"✓ order_payments_agg: {count:,} rows (from 103,886 Bronze rows)")

# COMMAND ----------

# MAGIC %md ## 6. orders_enriched — the central Silver table
# MAGIC
# MAGIC One row per **order_item** (112,650 rows) — every dimension joined, delivery
# MAGIC metrics derived. This is the primary input for all Gold aggregations.
# MAGIC
# MAGIC **Derived columns:**
# MAGIC | Column | Logic |
# MAGIC |---|---|
# MAGIC | `delivery_days` | `DATEDIFF(delivered_date, purchase_date)` |
# MAGIC | `estimated_days` | `DATEDIFF(estimated_date, purchase_date)` |
# MAGIC | `delivery_delay_days` | `DATEDIFF(delivered_date, estimated_date)` — negative = early |
# MAGIC | `is_late_delivery` | `delivered_date > estimated_date` |
# MAGIC | `purchase_year/month/quarter` | Extracted from `order_purchase_timestamp` |
# MAGIC | `year_month` | `'yyyy-MM'` formatted string for time-series grouping |

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.orders_enriched
COMMENT 'Silver — one row per order_item. All dimensions joined. Delivery metrics and date parts derived.'
AS
SELECT
  -- Order identifiers
  o.order_id,
  oi.order_item_id,
  o.customer_id,
  c.customer_unique_id,
  oi.product_id,
  oi.seller_id,

  -- Status & timestamps
  o.order_status,
  CAST(o.order_purchase_timestamp     AS DATE) AS order_purchase_date,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,

  -- Financials
  oi.price                             AS item_price,
  oi.freight_value,
  p.total_payment_value,
  p.dominant_payment_type              AS payment_type,
  p.installments                       AS payment_installments,

  -- Review
  r.review_score,
  r.review_comment_message,

  -- Customer
  c.customer_city,
  c.customer_state,
  c.customer_zip_code_prefix,
  c.valid_from                         AS customer_valid_from,

  -- Product
  dp.category_english,
  dp.product_category_name,
  dp.product_photos_qty,
  dp.product_weight_g,
  dp.volume_cm3,

  -- Seller
  s.seller_city,
  s.seller_state,

  -- Derived: delivery performance
  DATEDIFF(
    CAST(o.order_delivered_customer_date AS DATE),
    CAST(o.order_purchase_timestamp      AS DATE)
  )                                    AS delivery_days,
  DATEDIFF(
    CAST(o.order_estimated_delivery_date AS DATE),
    CAST(o.order_purchase_timestamp      AS DATE)
  )                                    AS estimated_days,
  DATEDIFF(
    CAST(o.order_delivered_customer_date AS DATE),
    CAST(o.order_estimated_delivery_date AS DATE)
  )                                    AS delivery_delay_days,
  CASE
    WHEN o.order_delivered_customer_date IS NOT NULL
     AND o.order_delivered_customer_date > o.order_estimated_delivery_date
    THEN TRUE ELSE FALSE
  END                                  AS is_late_delivery,

  -- Derived: date dimensions
  YEAR(o.order_purchase_timestamp)     AS purchase_year,
  MONTH(o.order_purchase_timestamp)    AS purchase_month,
  QUARTER(o.order_purchase_timestamp)  AS purchase_quarter,
  DATE_FORMAT(o.order_purchase_timestamp, 'yyyy-MM') AS year_month,

  current_timestamp()                  AS _updated_ts

FROM {CATALOG}.{BRONZE_SCHEMA}.orders o
JOIN  {CATALOG}.{BRONZE_SCHEMA}.order_items oi
  ON  o.order_id = oi.order_id
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.order_payments_agg p
  ON  o.order_id = p.order_id
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.order_reviews_deduped r
  ON  o.order_id = r.order_id
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.dim_customers c
  ON  o.customer_id = c.customer_id AND c.is_current = TRUE
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.dim_products dp
  ON  oi.product_id = dp.product_id
LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.dim_sellers s
  ON  oi.seller_id = s.seller_id
WHERE o.order_id IS NOT NULL
  AND o.order_purchase_timestamp IS NOT NULL
""")

count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders_enriched").count()
print(f"✓ orders_enriched: {count:,} rows")

# COMMAND ----------

# MAGIC %md ## 7. Validation

# COMMAND ----------

print(f"{'Table':<35} {'Rows':>10}")
print("-" * 48)
tables = [
  "dim_customers", "dim_products", "dim_sellers",
  "order_reviews_deduped", "order_payments_agg", "orders_enriched"
]
for t in tables:
    n = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{t}").count()
    print(f"  {t:<33} {n:>10,}")

# COMMAND ----------

# MAGIC %md ## 8. Quality spot-check on orders_enriched (delivered orders only)

# COMMAND ----------

spark.sql(f"""
SELECT
  COUNT(*)                                                        AS total_rows,
  COUNT(DISTINCT order_id)                                        AS distinct_orders,
  COUNT(DISTINCT customer_unique_id)                              AS distinct_customers,
  COUNT(DISTINCT seller_id)                                       AS distinct_sellers,
  COUNT(DISTINCT category_english)                                AS distinct_categories,
  ROUND(SUM(total_payment_value), 0)                              AS total_gmv_brl,
  ROUND(AVG(item_price), 2)                                       AS avg_item_price,
  ROUND(AVG(review_score), 2)                                     AS avg_review_score,
  ROUND(AVG(CASE WHEN delivery_days IS NOT NULL
                 THEN delivery_days END), 1)                      AS avg_delivery_days,
  SUM(CASE WHEN is_late_delivery = TRUE THEN 1 ELSE 0 END)        AS late_deliveries,
  MIN(order_purchase_date)                                        AS earliest_order,
  MAX(order_purchase_date)                                        AS latest_order
FROM {CATALOG}.{SILVER_SCHEMA}.orders_enriched
WHERE order_status = 'delivered'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver layer complete
# MAGIC
# MAGIC | Table | Rows | Notes |
# MAGIC |---|---|---|
# MAGIC | dim_customers | 99,441 | SCD Type 2, all current |
# MAGIC | dim_products | 32,951 | English categories, volume_cm3 |
# MAGIC | dim_sellers | 3,095 | Standardised casing |
# MAGIC | order_reviews_deduped | 99,741 | Down from 104,162 Bronze rows |
# MAGIC | order_payments_agg | 99,440 | Down from 103,886 Bronze rows |
# MAGIC | orders_enriched | 112,650 | One row per order_item — central Silver table |
# MAGIC
# MAGIC **Key real-data findings:**
# MAGIC - Total GMV (delivered): **R$19.78M** across **96,478 orders**
# MAGIC - Average review score: **4.08 ★**
# MAGIC - Average delivery time: **12.4 days**
# MAGIC - Late deliveries: **8,714** (9% of delivered orders)
# MAGIC - Date range: **Sep 2016 → Aug 2018**
# MAGIC
# MAGIC **Next:** run `03_gold_aggregates.py`
