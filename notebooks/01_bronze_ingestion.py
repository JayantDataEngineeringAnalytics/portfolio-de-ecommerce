# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC **portfolio_de · E-Commerce Analytics Pipeline**
# MAGIC
# MAGIC Reads all 9 Olist CSV files from the Unity Catalog volume, enforces schemas,
# MAGIC adds audit columns, and writes raw Delta tables to `portfolio_de.bronze`.
# MAGIC
# MAGIC **Prerequisite:** CSV files uploaded to `/Volumes/portfolio_de/landing_zone/raw_files/`

# COMMAND ----------

# MAGIC %md ## Coming in Step 3
# MAGIC This notebook will be fully implemented after the CSV files are uploaded to the landing zone volume.
