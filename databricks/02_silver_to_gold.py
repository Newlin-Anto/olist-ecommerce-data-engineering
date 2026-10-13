# Databricks notebook source
# Olist DE project: create dimensional Gold Delta tables from Silver tables.

# COMMAND ----------
from pyspark.sql import functions as F
import re

# Keep the widget values aligned with 01_bronze_to_silver.py.
dbutils.widgets.text("s3_bucket", "", "Private S3 bucket name")
dbutils.widgets.text("s3_prefix", "olist-ecommerce", "S3 prefix")
dbutils.widgets.text("catalog_name", "main", "Unity Catalog catalog")
dbutils.widgets.text("schema_name", "olist_de", "Unity Catalog schema")

S3_BUCKET = dbutils.widgets.get("s3_bucket").strip()
S3_PREFIX = dbutils.widgets.get("s3_prefix").strip("/")
CATALOG = dbutils.widgets.get("catalog_name").strip()
SCHEMA = dbutils.widgets.get("schema_name").strip()

if not S3_BUCKET:
    raise ValueError("Set the s3_bucket notebook widget to your private S3 bucket name.")
for label, value in [("catalog_name", CATALOG), ("schema_name", SCHEMA)]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} must be a simple Unity Catalog identifier.")
BASE_S3 = f"s3://{S3_BUCKET}/{S3_PREFIX}" if S3_PREFIX else f"s3://{S3_BUCKET}"
SCHEMA_FQN = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_FQN}")

# COMMAND ----------
def read_silver(name: str):
    return spark.table(f"{CATALOG}.{SCHEMA}.silver_{name}")


def write_delta_table(df, table_name: str):
    path = f"{BASE_S3}/gold/{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    quoted_path = path.replace("'", "''")
    table_fqn = f"{SCHEMA_FQN}.`{table_name}`"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_fqn} USING DELTA LOCATION '{quoted_path}'")
    spark.catalog.refreshTable(f"{CATALOG}.{SCHEMA}.{table_name}")
    print(f"Wrote {table_name} to {path}")


def date_key(column):
    return F.date_format(F.to_date(F.col(column)), "yyyyMMdd").cast("int")

# COMMAND ----------
customers = read_silver("customers")
orders = read_silver("orders")
items = read_silver("order_items")
payments = read_silver("payments")
reviews = read_silver("reviews")
products = read_silver("products")
sellers = read_silver("sellers")
translations = read_silver("category_translation")

# Validate references before joining. These checks complement the Silver key checks.
foreign_key_checks = []
relationships = [
    ("orders", orders, "customer_id", "customers", customers, "customer_id"),
    ("order_items", items, "order_id", "orders", orders, "order_id"),
    ("order_items", items, "product_id", "products", products, "product_id"),
    ("order_items", items, "seller_id", "sellers", sellers, "seller_id"),
    ("payments", payments, "order_id", "orders", orders, "order_id"),
    ("reviews", reviews, "order_id", "orders", orders, "order_id"),
]
for child_name, child_df, child_col, parent_name, parent_df, parent_col in relationships:
    null_count = child_df.filter(F.col(child_col).isNull()).count()
    parent_keys = parent_df.select(parent_col).where(F.col(parent_col).isNotNull()).dropDuplicates()
    orphan_count = child_df.where(F.col(child_col).isNotNull()).join(
        parent_keys, child_df[child_col] == parent_keys[parent_col], "left_anti"
    ).count()
    for check_name, issue_count, message in [
        (f"foreign_key_nulls::{child_name}.{child_col}", null_count, f"Null {child_col} references to {parent_name}.{parent_col}."),
        (f"foreign_key_orphans::{child_name}.{child_col}", orphan_count, f"Non-null {child_col} values not found in {parent_name}.{parent_col}."),
    ]:
        foreign_key_checks.append({
            "check_name": check_name,
            "table_name": child_name,
            "status": "FAIL" if issue_count else "PASS",
            "issue_count": int(issue_count),
            "message": message,
        })

# COMMAND ----------
# Dimension tables

dim_customers = customers.select(
    "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
)

translation_lookup = translations.select("product_category_name", "product_category_name_english")
products_joined = products.join(translation_lookup, on="product_category_name", how="left")
dim_products = (
    products_joined
    .withColumn(
        "category_translation_status",
        F.when(F.col("product_category_name").isNull(), F.lit("missing_category"))
         .when(F.col("product_category_name_english").isNull(), F.lit("untranslated_source_category"))
         .otherwise(F.lit("translated")),
    )
    .withColumn(
        "product_category_name_english",
        F.when(F.col("product_category_name").isNull(), F.lit("unknown"))
         .when(F.col("product_category_name_english").isNull(), F.col("product_category_name"))
         .otherwise(F.col("product_category_name_english")),
    )
    .select(
        "product_id", "product_category_name", "product_category_name_english", "category_translation_status",
        "product_name_length", "product_description_length", "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    )
)

dim_sellers = sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
purchase_dates = orders.select(F.to_date("order_purchase_timestamp").alias("full_date")).where(F.col("full_date").isNotNull()).distinct()
dim_date = (
    purchase_dates
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date") - F.lit(2))
    .withColumn("day_of_week", F.when(F.col("day_of_week") < 0, F.lit(6)).otherwise(F.col("day_of_week")))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .select("date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name")
)

# Fact tables. Keep separate fact grains to avoid multiplying item and payment amounts in joins.
fact_orders = (
    orders
    .withColumn("order_purchase_date_key", date_key("order_purchase_timestamp"))
    .select(
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
        "order_purchase_date_key",
    )
)

order_context = orders.select(
    "order_id", "customer_id", "order_status", "order_purchase_timestamp"
).withColumn("order_purchase_date_key", date_key("order_purchase_timestamp"))

fact_order_items = (
    items.alias("i")
    .join(order_context.alias("o"), on="order_id", how="inner")
    .withColumn("item_total", F.col("price") + F.col("freight_value"))
    .select(
        "order_id", "order_item_id", "product_id", "seller_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_purchase_date_key", "shipping_limit_date",
        "price", "freight_value", "item_total",
    )
)

fact_payments = payments.select(
    "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"
)
fact_reviews = reviews.select(
    "review_record_id", "review_id", "order_id", "review_score", "review_comment_title",
    "review_comment_message", "review_creation_date", "review_answer_timestamp",
)

# COMMAND ----------
GOLD_TABLES = {
    "dim_customers": dim_customers,
    "dim_products": dim_products,
    "dim_sellers": dim_sellers,
    "dim_date": dim_date,
    "fact_orders": fact_orders,
    "fact_order_items": fact_order_items,
    "fact_payments": fact_payments,
    "fact_reviews": fact_reviews,
}

for table_name, dataframe in GOLD_TABLES.items():
    write_delta_table(dataframe, table_name)

# Add the foreign-key validation results to the quality table.
existing_quality = spark.table(f"{CATALOG}.{SCHEMA}.data_quality_checks").collect()
all_quality_checks = [row.asDict(recursive=True) for row in existing_quality] + foreign_key_checks
quality_df = spark.createDataFrame(all_quality_checks)
quality_path = f"{BASE_S3}/quality/data_quality_checks"
quality_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(quality_path)
quoted_path = quality_path.replace("'", "''")
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {SCHEMA_FQN}.`data_quality_checks` USING DELTA LOCATION '{quoted_path}'"
)
spark.catalog.refreshTable(f"{CATALOG}.{SCHEMA}.data_quality_checks")

display(quality_df.orderBy(F.col("status"), F.col("table_name"), F.col("check_name")))
failed_fk_checks = [check for check in foreign_key_checks if check["status"] == "FAIL"]
if failed_fk_checks:
    raise ValueError(f"Gold layer stopped: {len(failed_fk_checks)} foreign-key checks failed. Review data_quality_checks.")

print(f"Gold layer complete. Tables registered in {CATALOG}.{SCHEMA}.")
