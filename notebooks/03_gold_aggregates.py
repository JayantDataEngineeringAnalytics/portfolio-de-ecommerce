# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Aggregates
# MAGIC **portfolio_de · E-Commerce Analytics Pipeline**
# MAGIC
# MAGIC Reads from `portfolio_de.silver`, builds the four Gold tables
# MAGIC (`fact_orders`, `agg_monthly_revenue`, `agg_seller_performance`, `agg_product_category`),
# MAGIC and writes to `portfolio_de.gold` optimised for BI queries.
# MAGIC
# MAGIC **Prerequisite:** `02_silver_transform.py` completed successfully.

# COMMAND ----------

# MAGIC %md ## Coming in Step 5
# MAGIC This notebook will be fully implemented after Silver transformation is validated.
