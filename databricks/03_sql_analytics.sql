-- Databricks notebook source
-- Replace `main`.`olist_de` below if you selected another catalog/schema.

-- COMMAND ----------
-- Monthly order-item value (price + freight). This is item value, not cash received.
SELECT
  date_trunc('MONTH', order_purchase_timestamp) AS order_month,
  ROUND(SUM(item_total), 2) AS gross_item_value,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(*) AS order_items
FROM `main`.`olist_de`.`fact_order_items`
GROUP BY date_trunc('MONTH', order_purchase_timestamp)
ORDER BY order_month;

-- COMMAND ----------
-- Top 10 product categories by item value.
SELECT
  p.product_category_name_english AS product_category,
  ROUND(SUM(i.item_total), 2) AS gross_item_value,
  COUNT(*) AS items_sold
FROM `main`.`olist_de`.`fact_order_items` i
JOIN `main`.`olist_de`.`dim_products` p ON i.product_id = p.product_id
GROUP BY p.product_category_name_english
ORDER BY gross_item_value DESC
LIMIT 10;

-- COMMAND ----------
-- Order status summary.
SELECT order_status, COUNT(*) AS order_count
FROM `main`.`olist_de`.`fact_orders`
GROUP BY order_status
ORDER BY order_count DESC;

-- COMMAND ----------
-- Average delivery time and estimated delivery performance for delivered orders.
SELECT
  ROUND(AVG((unix_timestamp(order_delivered_customer_date) - unix_timestamp(order_purchase_timestamp)) / 86400.0), 2) AS avg_purchase_to_delivery_days,
  ROUND(100.0 * AVG(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1.0 ELSE 0.0 END), 2) AS pct_delivered_by_estimate,
  COUNT(*) AS delivered_orders
FROM `main`.`olist_de`.`fact_orders`
WHERE order_status = 'delivered'
  AND order_purchase_timestamp IS NOT NULL
  AND order_delivered_customer_date IS NOT NULL
  AND order_estimated_delivery_date IS NOT NULL;

-- COMMAND ----------
-- Payment method mix. Do not join payments and order items before aggregating each fact independently.
SELECT
  payment_type,
  COUNT(*) AS payment_records,
  ROUND(SUM(payment_value), 2) AS total_payment_value,
  ROUND(AVG(payment_value), 2) AS avg_payment_value
FROM `main`.`olist_de`.`fact_payments`
GROUP BY payment_type
ORDER BY total_payment_value DESC;

-- COMMAND ----------
-- Review score distribution.
SELECT review_score, COUNT(*) AS review_records
FROM `main`.`olist_de`.`fact_reviews`
GROUP BY review_score
ORDER BY review_score;

-- COMMAND ----------
-- Quality checks requiring attention.
SELECT check_name, table_name, status, issue_count, message
FROM `main`.`olist_de`.`data_quality_checks`
WHERE status IN ('WARN', 'FAIL')
ORDER BY CASE status WHEN 'FAIL' THEN 1 ELSE 2 END, table_name, check_name;
