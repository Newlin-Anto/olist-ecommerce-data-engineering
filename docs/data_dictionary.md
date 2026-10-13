# Data dictionary and table grain

## Source data

| Source file/table | Approximate observed rows | Purpose |
|---|---:|---|
| customers | 99,441 | Customer record and location |
| geolocation | 1,000,163 | ZIP-prefix geographic observations |
| order_items | 112,650 | Product/seller line items per order |
| payments | 103,886 | Payment sequence and amount per order |
| reviews | 99,224 | Order review, score and timestamps |
| orders | 99,441 | Order status and lifecycle timestamps |
| products | 32,951 | Product attributes and source category |
| sellers | 3,095 | Seller record and location |
| category_translation | 71 | Portuguese category name to English translation |

Counts refer to the source dataset version inspected during project preparation; verify them against your local download.

## Gold dimension/fact tables

| Table | Grain | Key/identifier |
|---|---|---|
| `dim_customers` | One row per source customer record | `customer_id` |
| `dim_products` | One row per product | `product_id` |
| `dim_sellers` | One row per seller | `seller_id` |
| `dim_date` | One row per distinct order purchase date | `date_key` (`YYYYMMDD`) |
| `fact_orders` | One row per order | `order_id` |
| `fact_order_items` | One row per item within an order | `(order_id, order_item_id)` |
| `fact_payments` | One row per payment sequence within an order | `(order_id, payment_sequential)` |
| `fact_reviews` | One row per source review record | `review_record_id` (run-level surrogate) |

`fact_order_items.item_total = price + freight_value`. It is an item-level value measure, not a guaranteed measure of cash received. Payment facts are kept separate to avoid double counting across one-to-many joins.
