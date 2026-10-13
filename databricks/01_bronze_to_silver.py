# Databricks notebook source
# Olist DE project: read raw CSVs from S3 Bronze, validate and write Silver Delta tables.
# Before running: configure a Unity Catalog external location over s3://<bucket>/<prefix>/.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re

# Widgets make the notebook portable across buckets and catalogs.
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
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError("catalog_name must be a simple Unity Catalog identifier.")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", SCHEMA):
    raise ValueError("schema_name must be a simple Unity Catalog identifier.")
if S3_PREFIX and not re.fullmatch(r"[A-Za-z0-9/_-]+", S3_PREFIX):
    raise ValueError("s3_prefix may contain letters, numbers, slash, underscore and hyphen only.")

BASE_S3 = f"s3://{S3_BUCKET}/{S3_PREFIX}" if S3_PREFIX else f"s3://{S3_BUCKET}"
RAW_S3 = f"{BASE_S3}/bronze/raw"
SILVER_S3 = f"{BASE_S3}/silver"
QUALITY_S3 = f"{BASE_S3}/quality"
SCHEMA_FQN = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_FQN}")

# COMMAND ----------
SOURCE_FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

REQUIRED_COLUMNS = {
    "customers": {"customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"},
    "geolocation": {"geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"},
    "order_items": {"order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"},
    "payments": {"order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"},
    "reviews": {"review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"},
    "orders": {"order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"},
    "products": {"product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"},
    "sellers": {"seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"},
    "category_translation": {"product_category_name", "product_category_name_english"},
}

DATE_COLUMNS = {
    "orders": ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
    "order_items": ["shipping_limit_date"],
    "reviews": ["review_creation_date", "review_answer_timestamp"],
}
NUMERIC_TYPES = {
    "geolocation": {"geolocation_lat": "double", "geolocation_lng": "double"},
    "order_items": {"order_item_id": "int", "price": "double", "freight_value": "double"},
    "payments": {"payment_sequential": "int", "payment_installments": "int", "payment_value": "double"},
    "reviews": {"review_score": "int"},
    "products": {"product_name_lenght": "int", "product_description_lenght": "int", "product_photos_qty": "int", "product_weight_g": "double", "product_length_cm": "double", "product_height_cm": "double", "product_width_cm": "double"},
}
PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_record_id"],
    "category_translation": ["product_category_name"],
}

# COMMAND ----------
def write_delta_table(df, table_name: str, layer: str):
    """Write a Delta directory to the configured S3 prefix and register it in Unity Catalog."""
    path = f"{BASE_S3}/{layer}/{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    quoted_path = path.replace("'", "''")
    table_fqn = f"{SCHEMA_FQN}.`{table_name}`"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_fqn} USING DELTA LOCATION '{quoted_path}'")
    spark.catalog.refreshTable(f"{CATALOG}.{SCHEMA}.{table_name}")
    return path


def record_check(checks, name, table_name, status, issue_count, message):
    checks.append({
        "check_name": name,
        "table_name": table_name,
        "status": status,
        "issue_count": int(issue_count),
        "message": message,
    })


def count_key_nulls(df, columns):
    predicate = None
    for column in columns:
        current = F.col(column).isNull()
        predicate = current if predicate is None else (predicate | current)
    return df.filter(predicate).count()

# COMMAND ----------
checks = []
silver_tables = {}

for table_name, filename in SOURCE_FILES.items():
    source_path = f"{RAW_S3}/{filename}"
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "FAILFAST")
        .csv(source_path)
    )

    missing_columns = sorted(REQUIRED_COLUMNS[table_name] - set(df.columns))
    if missing_columns:
        raise ValueError(f"{filename} is missing required columns: {missing_columns}")

    # Standardize whitespace and blank strings, while retaining raw source files in S3.
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(cleaned == "", F.lit(None)).otherwise(cleaned))

    # ZIP/postal prefixes are identifiers, so preserve leading zeroes.
    zip_cols = {
        "customers": ["customer_zip_code_prefix"],
        "sellers": ["seller_zip_code_prefix"],
        "geolocation": ["geolocation_zip_code_prefix"],
    }.get(table_name, [])
    for column in zip_cols:
        df = df.withColumn(column, F.lpad(F.col(column), 5, "0"))

    for column in {"customers": ["customer_state"], "sellers": ["seller_state"], "geolocation": ["geolocation_state"]}.get(table_name, []):
        df = df.withColumn(column, F.upper(F.col(column)))

    # Cache the standardized source scan while profiling/casting this table so repeated
    # quality-check actions do not repeatedly reread a large CSV such as geolocation.
    cached_source = df.cache()
    df = cached_source

    # Cast dates. Invalid non-empty timestamp values are warnings; missing lifecycle
    # timestamps are preserved because they can be valid for canceled/in-flight orders.
    for column in DATE_COLUMNS.get(table_name, []):
        non_empty = F.col(column).isNotNull() & (F.trim(F.col(column)) != "")
        invalid_count = df.filter(non_empty & F.to_timestamp(F.col(column)).isNull()).count()
        record_check(
            checks,
            f"timestamp_parse::{table_name}.{column}",
            table_name,
            "WARN" if invalid_count else "PASS",
            invalid_count,
            "Non-empty values that did not parse as timestamps; rows retained with null parsed value.",
        )
        df = df.withColumn(column, F.to_timestamp(F.col(column)))

    # Cast numeric fields and report malformed non-empty values.
    for column, target_type in NUMERIC_TYPES.get(table_name, {}).items():
        non_empty = F.col(column).isNotNull() & (F.trim(F.col(column)) != "")
        invalid_count = df.filter(non_empty & F.col(column).cast(target_type).isNull()).count()
        record_check(
            checks,
            f"numeric_parse::{table_name}.{column}",
            table_name,
            "WARN" if invalid_count else "PASS",
            invalid_count,
            f"Non-empty values that did not cast to {target_type}; rows retained with null parsed value.",
        )
        df = df.withColumn(column, F.col(column).cast(target_type))

    # Correct misspellings in the source field names but preserve their values.
    if table_name == "products":
        df = df.withColumnRenamed("product_name_lenght", "product_name_length")
        df = df.withColumnRenamed("product_description_lenght", "product_description_length")

    # Source review_id is reused across multiple orders. Assign a run-level surrogate key
    # so every source row is preserved in the curated review fact.
    if table_name == "reviews":
        df = df.withColumn("review_record_id", F.monotonically_increasing_id())

    row_count = df.count()
    record_check(checks, f"row_count::{table_name}", table_name, "PASS", 0, f"Read {row_count:,} rows from {filename}.")

    if table_name in PRIMARY_KEYS:
        key_columns = PRIMARY_KEYS[table_name]
        null_keys = count_key_nulls(df, key_columns)
        duplicate_keys = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
        record_check(checks, f"primary_key_nulls::{table_name}", table_name, "FAIL" if null_keys else "PASS", null_keys, f"Rows with null values in key {key_columns}.")
        record_check(checks, f"primary_key_duplicates::{table_name}", table_name, "FAIL" if duplicate_keys else "PASS", duplicate_keys, f"Rows repeating key {key_columns}.")

    if table_name == "reviews":
        repeated_ids = df.filter(F.col("review_id").isNotNull()).groupBy("review_id").agg(F.countDistinct("order_id").alias("orders")).filter(F.col("orders") > 1).count()
        record_check(checks, "source_review_id_reuse", table_name, "WARN" if repeated_ids else "PASS", repeated_ids, "Source review_id values linked to multiple orders; all source rows are retained.")

    if table_name == "geolocation":
        exact_duplicate_rows = row_count - df.dropDuplicates().count()
        record_check(checks, "geolocation_exact_duplicate_rows", table_name, "WARN" if exact_duplicate_rows else "PASS", exact_duplicate_rows, "Exact duplicate source rows are retained in Silver; assess before aggregation.")

    silver_tables[table_name] = df
    output_table = f"silver_{table_name}"
    output_path = write_delta_table(df, output_table, "silver")
    cached_source.unpersist()
    print(f"Loaded {filename}: {row_count:,} rows -> {output_table} at {output_path}")

# Persist quality checks even if the source has a failing key check, then fail the run.
quality_df = spark.createDataFrame(checks)
quality_path = write_delta_table(quality_df, "data_quality_checks", "quality")
display(quality_df.orderBy(F.col("status"), F.col("table_name"), F.col("check_name")))

failed_checks = [check for check in checks if check["status"] == "FAIL"]
if failed_checks:
    raise ValueError(f"Silver validation failed with {len(failed_checks)} failed key checks. Review {quality_path}.")

print(f"Silver layer complete. Unity Catalog schema: {CATALOG}.{SCHEMA}")
