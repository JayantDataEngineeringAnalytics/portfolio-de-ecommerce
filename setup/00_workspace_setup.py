# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup — portfolio_de Catalog
# MAGIC
# MAGIC Run this notebook **once** to provision all schemas and volumes required for the
# MAGIC E-Commerce Analytics pipeline.
# MAGIC Requires: Unity Catalog enabled, `portfolio_de` catalog already exists.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify catalog

# COMMAND ----------

# DBTITLE 1,Confirm catalog exists
spark.sql("SHOW CATALOGS").filter("catalog = 'portfolio_de'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Landing Zone — raw file uploads

# COMMAND ----------

# DBTITLE 1,Create landing_zone schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_de.landing_zone
    COMMENT 'Landing zone for raw uploaded files — Olist Brazilian E-Commerce CSVs and future datasets'
""")
print("✓ Schema portfolio_de.landing_zone ready")

# COMMAND ----------

# DBTITLE 1,Create raw_files volume (upload CSVs here)
spark.sql("""
    CREATE VOLUME IF NOT EXISTS portfolio_de.landing_zone.raw_files
    COMMENT 'External volume for raw CSV uploads — upload Olist CSV files here before Bronze ingestion'
""")
print("✓ Volume portfolio_de.landing_zone.raw_files ready")
print("  Upload path: /Volumes/portfolio_de/landing_zone/raw_files/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze schema — raw Delta tables

# COMMAND ----------

# DBTITLE 1,Create bronze schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_de.bronze
    COMMENT 'Bronze layer — raw ingested Delta tables with audit columns, no transformations'
""")
print("✓ Schema portfolio_de.bronze ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver schema — cleansed & conformed tables

# COMMAND ----------

# DBTITLE 1,Create silver schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_de.silver
    COMMENT 'Silver layer — cleansed, joined, deduplicated tables. SCD Type 2 on dim_customers.'
""")
print("✓ Schema portfolio_de.silver ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold schema — business aggregates

# COMMAND ----------

# DBTITLE 1,Create gold schema
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS portfolio_de.gold
    COMMENT 'Gold layer — business-ready aggregates and fact tables optimised for BI queries'
""")
print("✓ Schema portfolio_de.gold ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify all schemas

# COMMAND ----------

# DBTITLE 1,List all schemas in portfolio_de
schemas = spark.sql("SHOW SCHEMAS IN portfolio_de").collect()
print("Schemas in portfolio_de:")
for s in schemas:
    print(f"  • {s['databaseName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify volume and confirm upload path

# COMMAND ----------

# DBTITLE 1,Show volume details
spark.sql("SHOW VOLUMES IN portfolio_de.landing_zone").display()

# COMMAND ----------

# DBTITLE 1,Print upload instructions
UPLOAD_PATH = "/Volumes/portfolio_de/landing_zone/raw_files/"

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print()
print("Upload the following Olist CSV files to:")
print(f"  {UPLOAD_PATH}")
print()
files = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]
for f in files:
    print(f"  ✓ {f}")
print()
print("Then run: notebooks/01_bronze_ingestion.py")
print("=" * 60)
