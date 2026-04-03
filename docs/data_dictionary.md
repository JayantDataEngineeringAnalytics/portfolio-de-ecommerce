# Data Dictionary — Gold Layer Tables

All tables reside in the `portfolio_de.gold` schema.

---

## fact_orders

One row per order line item — the central fact table for all BI reporting.

| Column | Type | Description |
|---|---|---|
| order_id | STRING | Unique order identifier |
| order_item_id | INT | Line item sequence within the order |
| customer_id | STRING | FK → dim_customers |
| customer_unique_id | STRING | De-duplicated customer identifier (used for retention analysis) |
| product_id | STRING | FK → dim_products |
| seller_id | STRING | FK → dim_sellers |
| order_purchase_date | DATE | Date the order was placed |
| order_delivered_date | DATE | Actual delivery date (null if not yet delivered) |
| order_estimated_date | DATE | Estimated delivery date at purchase time |
| order_status | STRING | delivered / shipped / cancelled / unavailable / etc. |
| payment_value | DOUBLE | Total payment amount (R$) |
| freight_value | DOUBLE | Freight component of payment (R$) |
| product_value | DOUBLE | Product price component (R$) |
| delivery_days | INT | Days from purchase to delivery (null if not delivered) |
| estimated_days | INT | Days from purchase to estimated delivery |
| delivery_delay_days | INT | delivery_days − estimated_days (negative = early) |
| review_score | INT | Customer review score (1–5, null if no review) |
| year | INT | Derived from order_purchase_date |
| month | INT | Derived from order_purchase_date |
| quarter | INT | Derived from order_purchase_date |
| category_english | STRING | English product category name |
| customer_city | STRING | Customer city |
| customer_state | STRING | Customer state (2-char code) |
| seller_city | STRING | Seller city |
| seller_state | STRING | Seller state (2-char code) |

---

## agg_monthly_revenue

Monthly aggregates for revenue trend analysis.

| Column | Type | Description |
|---|---|---|
| year | INT | Calendar year |
| month | INT | Calendar month (1–12) |
| year_month | STRING | 'YYYY-MM' formatted label |
| total_revenue | DOUBLE | Sum of payment_value (R$) |
| total_orders | BIGINT | Count of distinct orders |
| total_items | BIGINT | Count of order line items |
| avg_order_value | DOUBLE | total_revenue / total_orders |
| avg_freight_value | DOUBLE | Average freight per order |
| on_time_deliveries | BIGINT | Orders delivered on or before estimated date |
| late_deliveries | BIGINT | Orders delivered after estimated date |
| on_time_pct | DOUBLE | on_time_deliveries / total delivered orders |
| avg_review_score | DOUBLE | Average review score for orders in this month |

---

## agg_seller_performance

One row per seller — used for seller ranking and performance reporting.

| Column | Type | Description |
|---|---|---|
| seller_id | STRING | Unique seller identifier |
| seller_city | STRING | Seller city |
| seller_state | STRING | Seller state |
| total_orders | BIGINT | Total orders fulfilled |
| total_revenue | DOUBLE | Total GMV generated (R$) |
| avg_order_value | DOUBLE | Average order value (R$) |
| avg_delivery_days | DOUBLE | Average days to deliver |
| on_time_pct | DOUBLE | Percentage of on-time deliveries |
| avg_review_score | DOUBLE | Average customer review score |
| total_products | BIGINT | Distinct products listed |
| total_categories | BIGINT | Distinct categories sold in |
| first_order_date | DATE | Date of seller's first order |
| last_order_date | DATE | Date of seller's most recent order |

---

## agg_product_category

Category-level aggregates for category mix and performance analysis.

| Column | Type | Description |
|---|---|---|
| category_english | STRING | English category name |
| total_orders | BIGINT | Orders containing products in this category |
| total_revenue | DOUBLE | Total revenue from this category (R$) |
| revenue_share_pct | DOUBLE | Category revenue as % of total GMV |
| avg_order_value | DOUBLE | Average order value for this category (R$) |
| avg_review_score | DOUBLE | Average review score for this category |
| avg_delivery_days | DOUBLE | Average delivery time for this category |
| total_sellers | BIGINT | Distinct sellers selling in this category |
| avg_product_photos | DOUBLE | Average number of product photos |
