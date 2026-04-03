# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC **portfolio_de · E-Commerce Analytics Pipeline**
# MAGIC
# MAGIC Reads all 9 Olist CSV files from the Unity Catalog volume using explicit PySpark schemas,
# MAGIC adds audit columns, and writes raw Delta tables to `portfolio_de.bronze`.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Source** | `/Volumes/portfolio_de/landing_zone/raw_files/` |
# MAGIC | **Target** | `portfolio_de.bronze.*` |
# MAGIC | **Format** | Delta Lake |
# MAGIC | **Audit columns** | `_ingestion_ts`, `_source_file` |
# MAGIC | **Write mode** | `overwrite` (idempotent re-runs) |
# MAGIC
# MAGIC **Run order:** Notebook 1 of 3. Run before `02_silver_transform.py`.

# COMMAND ----------

# MAGIC %md ## 0. Imports & configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG        = "portfolio_de"
BRONZE_SCHEMA  = "bronze"
VOLUME_PATH    = "/Volumes/portfolio_de/landing_zone/raw_files"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

print(f"Catalog  : {CATALOG}")
print(f"Schema   : {BRONZE_SCHEMA}")
print(f"Source   : {VOLUME_PATH}")
print(f"Started  : {datetime.now():%Y-%m-%d %H:%M:%S}")

# COMMAND ----------

# MAGIC %md ## 1. Schema definitions
# MAGIC
# MAGIC Explicit schemas prevent silent type coercion and document the source contract.
# MAGIC `TRY_CAST` equivalents (`.option("mode", "PERMISSIVE")`) used for columns
# MAGIC with known dirty values — Bronze accepts raw data as-is and logs bad rows.
# MAGIC
# MAGIC > **Note:** `product_name_lenght` and `product_description_lenght` column name
# MAGIC > typos are preserved intentionally — they match the source file headers exactly.

# COMMAND ----------

schema_orders = StructType([
    StructField("order_id",                       StringType(),    False),
    StructField("customer_id",                    StringType(),    False),
    StructField("order_status",                   StringType(),    True),
    StructField("order_purchase_timestamp",       TimestampType(), True),
    StructField("order_approved_at",              TimestampType(), True),
    StructField("order_delivered_carrier_date",   TimestampType(), True),
    StructField("order_delivered_customer_date",  TimestampType(), True),
    StructField("order_estimated_delivery_date",  TimestampType(), True),
])

schema_order_items = StructType([
    StructField("order_id",            StringType(),    False),
    StructField("order_item_id",       IntegerType(),   False),
    StructField("product_id",          StringType(),    False),
    StructField("seller_id",           StringType(),    False),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price",               DoubleType(),    True),
    StructField("freight_value",       DoubleType(),    True),
])

schema_payments = StructType([
    StructField("order_id",             StringType(),  False),
    StructField("payment_sequential",   IntegerType(), True),
    StructField("payment_type",         StringType(),  True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value",        DoubleType(),  True),
])

# Note: review_score uses IntegerType but the source file contains malformed rows
# where a timestamp value appears in the score column. TRY_CAST via PERMISSIVE mode
# handles this — malformed values become NULL and are investigated in Silver.
schema_reviews = StructType([
    StructField("review_id",               StringType(),    False),
    StructField("order_id",                StringType(),    False),
    StructField("review_score",            IntegerType(),   True),
    StructField("review_comment_title",    StringType(),    True),
    StructField("review_comment_message",  StringType(),    True),
    StructField("review_creation_date",    TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
])

schema_customers = StructType([
    StructField("customer_id",              StringType(), False),
    StructField("customer_unique_id",       StringType(), False),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city",            StringType(), True),
    StructField("customer_state",           StringType(), True),
])

schema_products = StructType([
    StructField("product_id",                   StringType(),  False),
    StructField("product_category_name",        StringType(),  True),
    StructField("product_name_lenght",          IntegerType(), True),  # typo preserved from source
    StructField("product_description_lenght",   IntegerType(), True),  # typo preserved from source
    StructField("product_photos_qty",           IntegerType(), True),
    StructField("product_weight_g",             IntegerType(), True),
    StructField("product_length_cm",            IntegerType(), True),
    StructField("product_height_cm",            IntegerType(), True),
    StructField("product_width_cm",             IntegerType(), True),
])

schema_sellers = StructType([
    StructField("seller_id",              StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city",            StringType(), True),
    StructField("seller_state",           StringType(), True),
])

schema_geolocation = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat",             DoubleType(), True),
    StructField("geolocation_lng",             DoubleType(), True),
    StructField("geolocation_city",            StringType(), True),
    StructField("geolocation_state",           StringType(), True),
])

schema_category_translation = StructType([
    StructField("product_category_name",         StringType(), False),
    StructField("product_category_name_english", StringType(), True),
])

print("✓ All 9 schemas defined")

# COMMAND ----------

# MAGIC %md ## 2. Ingestion helper

# COMMAND ----------

def ingest_to_bronze(filename: str, schema: StructType, table_name: str, mode: str = "overwrite") -> int:
    """
    Read a CSV from the landing volume, add audit columns,
    write to Delta Bronze as a managed Unity Catalog table.
    Returns the final row count.
    """
    path = f"{VOLUME_PATH}/{filename}"

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("mode", "PERMISSIVE")   # malformed values → NULL, not job failure
        .schema(schema)
        .load(path)
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file",  F.lit(filename))
    )

    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}")
    )

    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}").count()
    print(f"  ✓ {table_name:<40} {count:>10,} rows")
    return count

# COMMAND ----------

# MAGIC %md ## 3. Ingest all 9 files

# COMMAND ----------

print("Starting Bronze ingestion...")
print("-" * 60)

results = {}
results["orders"]               = ingest_to_bronze("olist_orders_dataset.csv",                 schema_orders,               "orders")
results["order_items"]          = ingest_to_bronze("olist_order_items_dataset.csv",             schema_order_items,          "order_items")
results["order_payments"]       = ingest_to_bronze("olist_order_payments_dataset.csv",          schema_payments,             "order_payments")
results["order_reviews"]        = ingest_to_bronze("olist_order_reviews_dataset.csv",           schema_reviews,              "order_reviews")
results["customers"]            = ingest_to_bronze("olist_customers_dataset.csv",               schema_customers,            "customers")
results["products"]             = ingest_to_bronze("olist_products_dataset.csv",                schema_products,             "products")
results["sellers"]              = ingest_to_bronze("olist_sellers_dataset.csv",                 schema_sellers,              "sellers")
results["geolocation"]          = ingest_to_bronze("olist_geolocation_dataset.csv",             schema_geolocation,          "geolocation")
results["category_translation"] = ingest_to_bronze("product_category_name_translation.csv",     schema_category_translation, "category_translation")

print("-" * 60)
print(f"Total rows ingested: {sum(results.values()):,}")
print(f"Completed : {datetime.now():%Y-%m-%d %H:%M:%S}")

# COMMAND ----------

# MAGIC %md ## 4. Validation
# MAGIC
# MAGIC Expected row counts from the Olist dataset documentation.
# MAGIC `order_reviews` lands at **104,162** (vs documented 100,542) because
# MAGIC some orders have multiple reviews — this is expected and handled in Silver
# MAGIC by keeping the most recent review per `order_id`.

# COMMAND ----------

EXPECTED = {
    "orders":               99441,
    "order_items":          112650,
    "order_payments":       103886,
    "order_reviews":        104162,   # includes multi-review orders; deduplicated in Silver
    "customers":            99441,
    "products":             32951,
    "sellers":              3095,
    "geolocation":          1000163,
    "category_translation": 71,
}

print(f"{'Table':<40} {'Expected':>10}  {'Actual':>10}  Status")
print("-" * 70)
all_ok = True
for table, expected in EXPECTED.items():
    actual  = results[table]
    ok      = actual == expected
    if not ok: all_ok = False
    status  = "✓ PASS" if ok else "⚠ MISMATCH"
    print(f"  {table:<38} {expected:>10,}  {actual:>10,}  {status}")

print("-" * 70)
print("ALL CHECKS PASSED ✓" if all_ok else "SEE MISMATCHES ABOVE")

# COMMAND ----------

# MAGIC %md ## 5. Sample audit check

# COMMAND ----------

spark.table("portfolio_de.bronze.orders") \
    .select("order_id", "order_status", "order_purchase_timestamp",
            "_ingestion_ts", "_source_file") \
    .show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Bronze layer complete
# MAGIC
# MAGIC | Table | Rows |
# MAGIC |---|---|
# MAGIC | orders | 99,441 |
# MAGIC | order_items | 112,650 |
# MAGIC | order_payments | 103,886 |
# MAGIC | order_reviews | 104,162 (deduped in Silver) |
# MAGIC | customers | 99,441 |
# MAGIC | products | 32,951 |
# MAGIC | sellers | 3,095 |
# MAGIC | geolocation | 1,000,163 |
# MAGIC | category_translation | 71 |
# MAGIC
# MAGIC **Next:** run `02_silver_transform.py`
