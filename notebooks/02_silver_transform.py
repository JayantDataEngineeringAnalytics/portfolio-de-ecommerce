# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation
# MAGIC **portfolio_de · E-Commerce Analytics Pipeline**
# MAGIC
# MAGIC Reads from `portfolio_de.bronze`, joins all 9 datasets, cleanses, deduplicates,
# MAGIC implements SCD Type 2 on customers, and writes to `portfolio_de.silver`.
# MAGIC
# MAGIC **Prerequisite:** `01_bronze_ingestion.py` completed successfully.

# COMMAND ----------

# MAGIC %md ## Coming in Step 4
# MAGIC This notebook will be fully implemented after Bronze ingestion is validated.
