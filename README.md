# E-Commerce Sales Analytics — Data Engineering & Analytics Portfolio

An end-to-end data engineering and analytics project built on the **Olist Brazilian E-Commerce** public dataset.
Demonstrates a production-grade **Medallion Architecture** on Databricks with Delta Lake, Unity Catalog, and an interactive analytics report.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE                                                         │
│  Olist Dataset · 9 CSV files · 100K+ orders · 2016–2018        │
└───────────────────────────┬─────────────────────────────────────┘
                            │ Upload to Unity Catalog Volume
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE  (portfolio_de.bronze)                                  │
│  • Raw ingestion via PySpark                                    │
│  • Schema enforcement with explicit StructType                  │
│  • Audit columns: _ingestion_ts, _source_file                   │
│  • Stored as Delta tables — append-only                         │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER  (portfolio_de.silver)                                  │
│  • All 9 datasets joined into unified view                      │
│  • Null handling, date standardisation, deduplication           │
│  • SCD Type 2 on dim_customers (valid_from / valid_to)          │
│  • Stored as Delta tables with OPTIMIZE + Z-ORDER               │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD  (portfolio_de.gold)                                      │
│  • fact_orders           — order-level fact table               │
│  • agg_monthly_revenue   — monthly GMV, orders, AOV             │
│  • agg_seller_performance — fulfilment, delivery, revenue       │
│  • agg_product_category  — category-level sales & ratings       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  REPORTING                                                      │
│  • Interactive HTML dashboard (embedded in portfolio)           │
│  • Power BI star schema model (documented in portfolio)         │
│  • Key DAX measures: Revenue YoY%, AOV, On-Time Delivery %      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage & Compute | Databricks (Unity Catalog, Delta Lake) |
| Transformation | PySpark, Spark SQL |
| Orchestration | Databricks Workflows |
| Data Format | Delta (Parquet + transaction log) |
| Reporting | Interactive HTML / Power BI |
| Version Control | Git + GitHub |
| Language | Python 3 |

---

## Dataset

**[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**
— 100,000 orders · 9 CSV files · September 2016 – October 2018

| File | Rows | Description |
|---|---|---|
| olist_orders_dataset.csv | 99,441 | Order header — status, timestamps |
| olist_order_items_dataset.csv | 112,650 | Line items — product, seller, price |
| olist_order_payments_dataset.csv | 103,886 | Payment method and value |
| olist_order_reviews_dataset.csv | 100,542 | Customer review scores and comments |
| olist_customers_dataset.csv | 99,441 | Customer location details |
| olist_products_dataset.csv | 32,951 | Product attributes and category |
| olist_sellers_dataset.csv | 3,095 | Seller location |
| olist_geolocation_dataset.csv | 1,000,163 | Zip code → lat/lng mapping |
| product_category_name_translation.csv | 71 | Portuguese → English category names |

---

## Repository Structure

```
portfolio-de-ecommerce/
├── setup/
│   └── 00_workspace_setup.py       # Unity Catalog schema & volume provisioning
├── notebooks/
│   ├── 01_bronze_ingestion.py      # Raw CSV → Delta Bronze
│   ├── 02_silver_transform.py      # Cleanse, join, SCD Type 2 → Silver
│   └── 03_gold_aggregates.py       # Business aggregates → Gold
├── docs/
│   └── data_dictionary.md          # Column definitions for all Gold tables
├── data/
│   └── schema/                     # StructType schema definitions (JSON)
└── README.md
```

---

## How to Run

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- `portfolio_de` catalog created
- Olist CSV files downloaded from Kaggle

### Step 1 — Workspace setup
Run `setup/00_workspace_setup.py` in your Databricks workspace.
This creates all schemas (`landing_zone`, `bronze`, `silver`, `gold`) and the upload volume.

### Step 2 — Upload data
Upload all 9 CSV files to:
```
/Volumes/portfolio_de/landing_zone/raw_files/
```

### Step 3 — Run notebooks in order
```
01_bronze_ingestion.py   → Ingests CSVs into Delta Bronze tables
02_silver_transform.py   → Cleanses and joins into Silver tables
03_gold_aggregates.py    → Builds business aggregates in Gold
```

---

## Portfolio

This project is featured in the [JM Analytics Portfolio](https://jayantmohite.com/portfolio/e-commerce-analytics).

---

## License

Dataset: [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) — Olist
Code: MIT
