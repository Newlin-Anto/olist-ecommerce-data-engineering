from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

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

STRING_DTYPES = {
    "customers": {
        "customer_id": "string", "customer_unique_id": "string",
        "customer_zip_code_prefix": "string", "customer_city": "string",
        "customer_state": "string",
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "string", "geolocation_city": "string",
        "geolocation_state": "string",
    },
    "order_items": {
        "order_id": "string", "product_id": "string", "seller_id": "string",
    },
    "payments": {"order_id": "string", "payment_type": "string"},
    "reviews": {
        "review_id": "string", "order_id": "string",
        "review_comment_title": "string", "review_comment_message": "string",
    },
    "orders": {
        "order_id": "string", "customer_id": "string", "order_status": "string",
    },
    "products": {"product_id": "string", "product_category_name": "string"},
    "sellers": {
        "seller_id": "string", "seller_zip_code_prefix": "string",
        "seller_city": "string", "seller_state": "string",
    },
    "category_translation": {
        "product_category_name": "string", "product_category_name_english": "string",
    },
}

DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "reviews": ["review_creation_date", "review_answer_timestamp"],
}

NUMERIC_COLUMNS = {
    "geolocation": ["geolocation_lat", "geolocation_lng"],
    "order_items": ["order_item_id", "price", "freight_value"],
    "payments": ["payment_sequential", "payment_installments", "payment_value"],
    "reviews": ["review_score"],
    "products": [
        "product_name_lenght", "product_description_lenght", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    ],
}

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "category_translation": ["product_category_name"],
    # The source review_id is reused across orders in this dataset. The pipeline adds
    # review_record_id to identify each preserved source row without deleting records.
    "reviews": ["review_record_id"],
}

FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("reviews", "order_id", "orders", "order_id"),
]


def _read_csv(path: Path, table: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required source file not found: {path}\n"
            "Download the Olist dataset and put all nine CSV files in data/raw/."
        )
    frame = pd.read_csv(path, dtype=STRING_DTYPES.get(table, {}), low_memory=False)
    missing_columns = sorted(REQUIRED_COLUMNS[table] - set(frame.columns))
    if missing_columns:
        raise ValueError(
            f"Source schema mismatch in {path.name}; missing required columns: {missing_columns}"
        )
    return frame


def _clean_text(series: pd.Series, *, uppercase: bool = False, lowercase: bool = False) -> pd.Series:
    result = series.astype("string").str.strip()
    if uppercase:
        result = result.str.upper()
    if lowercase:
        result = result.str.lower()
    return result


def _parse_dates(df: pd.DataFrame, columns: list[str], table: str, checks: list[dict[str, Any]]) -> None:
    for column in columns:
        original = df[column].astype("string")
        has_value = original.notna() & original.str.strip().ne("")
        parsed = pd.to_datetime(df[column], errors="coerce")
        invalid_count = int((has_value & parsed.isna()).sum())
        checks.append({
            "name": f"parseable_timestamps::{table}.{column}",
            "status": "WARN" if invalid_count else "PASS",
            "count": invalid_count,
            "message": f"{invalid_count} non-empty values could not be parsed as timestamps.",
        })
        df[column] = parsed


def _convert_numeric(df: pd.DataFrame, table: str, columns: list[str], checks: list[dict[str, Any]]) -> None:
    for column in columns:
        original = df[column].astype("string")
        has_value = original.notna() & original.str.strip().ne("")
        parsed = pd.to_numeric(df[column], errors="coerce")
        invalid_count = int((has_value & parsed.isna()).sum())
        checks.append({
            "name": f"numeric_values::{table}.{column}",
            "status": "WARN" if invalid_count else "PASS",
            "count": invalid_count,
            "message": f"{invalid_count} non-empty values could not be parsed as numeric values.",
        })
        if column in {"order_item_id", "payment_sequential", "payment_installments", "review_score",
                      "product_name_lenght", "product_description_lenght", "product_photos_qty"}:
            df[column] = parsed.astype("Int64")
        else:
            df[column] = parsed


def _transform_tables(raw: dict[str, pd.DataFrame], checks: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    # Transform the in-memory frames in place to avoid duplicating the 1M-row geolocation
    # table. The source CSV files on disk remain untouched.
    tables = raw

    for table, df in tables.items():
        for column in df.columns:
            if pd.api.types.is_string_dtype(df[column].dtype):
                df[column] = _clean_text(df[column])

    # ZIP-code prefixes are identifiers, not numbers; retain leading zeroes.
    tables["customers"]["customer_zip_code_prefix"] = (
        tables["customers"]["customer_zip_code_prefix"].astype("string").str.zfill(5)
    )
    tables["sellers"]["seller_zip_code_prefix"] = (
        tables["sellers"]["seller_zip_code_prefix"].astype("string").str.zfill(5)
    )
    tables["geolocation"]["geolocation_zip_code_prefix"] = (
        tables["geolocation"]["geolocation_zip_code_prefix"].astype("string").str.zfill(5)
    )
    for table, column in [("customers", "customer_state"), ("sellers", "seller_state"),
                          ("geolocation", "geolocation_state")]:
        tables[table][column] = _clean_text(tables[table][column], uppercase=True)
    for table, column in [("customers", "customer_city"), ("sellers", "seller_city"),
                          ("geolocation", "geolocation_city")]:
        tables[table][column] = _clean_text(tables[table][column])

    for table, columns in DATE_COLUMNS.items():
        _parse_dates(tables[table], columns, table, checks)

    for table, columns in NUMERIC_COLUMNS.items():
        _convert_numeric(tables[table], table, columns, checks)

    # Standardize misspelled source column names without discarding their values.
    tables["products"] = tables["products"].rename(columns={
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    })

    # The source review_id is repeated for multiple orders. Preserve every row and add
    # a pipeline-level row identifier rather than silently dropping records.
    tables["reviews"].insert(0, "review_record_id", range(1, len(tables["reviews"]) + 1))

    # Add English categories where available. Untranslated Portuguese values are retained;
    # absent categories are labeled explicitly as unknown in the English/reporting field.
    translation = tables["category_translation"].dropna(subset=["product_category_name"])
    translation_map = dict(zip(
        translation["product_category_name"],
        translation["product_category_name_english"],
    ))
    products = tables["products"]
    original_category = products["product_category_name"].astype("string")
    translated = original_category.map(translation_map).astype("string")
    products["category_translation_status"] = "translated"
    products.loc[original_category.isna(), "category_translation_status"] = "missing_source_category"
    products.loc[original_category.notna() & translated.isna(), "category_translation_status"] = "untranslated_source_category"
    products["product_category_name_english"] = translated.fillna(original_category).fillna("unknown")

    return tables


def _add_check(checks: list[dict[str, Any]], name: str, count: int, message: str,
               *, warning: bool = False, failure: bool = False) -> None:
    if failure and count:
        status = "FAIL"
    elif count and warning:
        status = "WARN"
    elif count:
        status = "FAIL"
    else:
        status = "PASS"
    checks.append({"name": name, "status": status, "count": int(count), "message": message})


def _validate_tables(tables: dict[str, pd.DataFrame], checks: list[dict[str, Any]]) -> None:
    # Table keys
    for table, key_columns in PRIMARY_KEYS.items():
        df = tables[table]
        missing = int(df[key_columns].isna().any(axis=1).sum())
        duplicates = int(df.duplicated(subset=key_columns).sum())
        _add_check(checks, f"primary_key_nulls::{table}", missing,
                   f"Rows with missing values in key {key_columns}.", failure=True)
        _add_check(checks, f"primary_key_duplicates::{table}", duplicates,
                   f"Rows repeating key {key_columns}.", failure=True)

    # Foreign-key/reference checks
    for child_table, child_col, parent_table, parent_col in FOREIGN_KEYS:
        child = tables[child_table][child_col]
        parent_values = set(tables[parent_table][parent_col].dropna().astype(str))
        null_count = int(child.isna().sum())
        orphans = int((child.notna() & ~child.astype("string").isin(parent_values)).sum())
        _add_check(checks, f"foreign_key_nulls::{child_table}.{child_col}", null_count,
                   f"Null values in reference to {parent_table}.{parent_col}.", failure=True)
        _add_check(checks, f"foreign_key_orphans::{child_table}.{child_col}", orphans,
                   f"Non-null values not found in {parent_table}.{parent_col}.", failure=True)

    # Document known source-data anomalies as warnings; don't silently delete.
    reviews = tables["reviews"]
    duplicate_review_rows = int(reviews.duplicated(subset=["review_id"]).sum())
    duplicate_review_groups = int(reviews.loc[
        reviews["review_id"].duplicated(keep=False), "review_id"
    ].nunique())
    _add_check(checks, "source_review_id_reuse", duplicate_review_rows,
               f"{duplicate_review_groups} review_id values repeat across records; source rows are preserved.",
               warning=True)

    geo = tables["geolocation"]
    exact_geo_duplicates = int(geo.duplicated().sum())
    _add_check(checks, "geolocation_exact_duplicate_rows", exact_geo_duplicates,
               "Exact duplicate geolocation rows are retained in staging; assess before any aggregation.",
               warning=True)

    products = tables["products"]
    missing_categories = int(products["product_category_name"].isna().sum())
    untranslated_rows = int((products["category_translation_status"] == "untranslated_source_category").sum())
    _add_check(checks, "product_missing_categories", missing_categories,
               "Products with no source category; retained and labeled unknown for reporting.", warning=True)
    _add_check(checks, "product_untranslated_categories", untranslated_rows,
               "Products whose category exists but lacks an English translation; source label is retained.", warning=True)

    geo_prefixes = set(geo["geolocation_zip_code_prefix"].dropna().astype(str))
    for table, col in [("customers", "customer_zip_code_prefix"), ("sellers", "seller_zip_code_prefix")]:
        values = tables[table][col].astype("string")
        missing = int(values.isna().sum())
        unmatched = int((values.notna() & ~values.isin(geo_prefixes)).sum())
        _add_check(checks, f"geolocation_missing_prefix::{table}", missing,
                   "Missing ZIP-code prefixes.", warning=True)
        _add_check(checks, f"geolocation_unmatched_prefix::{table}", unmatched,
                   "ZIP-code prefixes not represented in the geolocation source; rows are retained.", warning=True)

    # Delivered orders with absent carrier/delivery timestamps are noteworthy but not auto-fixed.
    orders = tables["orders"]
    delivered = orders[orders["order_status"].astype("string").str.lower().eq("delivered")]
    for column in ["order_delivered_carrier_date", "order_delivered_customer_date"]:
        missing = int(delivered[column].isna().sum())
        _add_check(checks, f"delivered_order_missing_timestamp::{column}", missing,
                   "Delivered orders missing a lifecycle timestamp; flagged for review, not imputed.",
                   warning=True)


def _make_curated(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    customers = tables["customers"].copy()
    products = tables["products"].copy()
    sellers = tables["sellers"].copy()
    orders = tables["orders"].copy()
    items = tables["order_items"].copy()
    payments = tables["payments"].copy()
    reviews = tables["reviews"].copy()

    # Build one date dimension from the order purchase date.
    purchase_dates = orders["order_purchase_timestamp"].dropna().dt.normalize().drop_duplicates().sort_values()
    dim_date = pd.DataFrame({"full_date": purchase_dates})
    if len(dim_date):
        dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype("int64")
        dim_date["year"] = dim_date["full_date"].dt.year.astype("int64")
        dim_date["quarter"] = dim_date["full_date"].dt.quarter.astype("int64")
        dim_date["month"] = dim_date["full_date"].dt.month.astype("int64")
        dim_date["month_name"] = dim_date["full_date"].dt.month_name()
        dim_date["day"] = dim_date["full_date"].dt.day.astype("int64")
        dim_date["day_of_week"] = dim_date["full_date"].dt.dayofweek.astype("int64")
        dim_date["day_name"] = dim_date["full_date"].dt.day_name()
        dim_date = dim_date[["date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name"]]
    else:
        dim_date = pd.DataFrame(columns=["date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name"])

    orders["order_purchase_date_key"] = pd.to_datetime(orders["order_purchase_timestamp"]).dt.strftime("%Y%m%d")
    orders["order_purchase_date_key"] = pd.to_numeric(orders["order_purchase_date_key"], errors="coerce").astype("Int64")

    order_context = orders[[
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_purchase_date_key"
    ]]
    fact_items = items.merge(order_context, on="order_id", how="left", validate="many_to_one")
    fact_items["item_total"] = fact_items["price"] + fact_items["freight_value"]
    fact_items = fact_items[[
        "order_id", "order_item_id", "product_id", "seller_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_purchase_date_key", "shipping_limit_date", "price", "freight_value", "item_total"
    ]]

    dim_customers = customers[["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]].copy()
    dim_products = products[[
        "product_id", "product_category_name", "product_category_name_english", "category_translation_status",
        "product_name_length", "product_description_length", "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm"
    ]].copy()
    dim_sellers = sellers[["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]].copy()
    fact_orders = orders[[
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date",
        "order_purchase_date_key"
    ]].copy()
    fact_payments = payments[["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]].copy()
    fact_reviews = reviews[[
        "review_record_id", "review_id", "order_id", "review_score", "review_comment_title",
        "review_comment_message", "review_creation_date", "review_answer_timestamp"
    ]].copy()

    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_sellers": dim_sellers,
        "dim_date": dim_date,
        "fact_orders": fact_orders,
        "fact_order_items": fact_items,
        "fact_payments": fact_payments,
        "fact_reviews": fact_reviews,
    }


def run_pipeline(project_root: str | Path | None = None) -> dict[str, Any]:
    """Run the full local batch pipeline and return its quality report."""
    root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parents[2]
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    staging_dir = processed_dir / "staging"
    curated_dir = processed_dir / "curated"
    reports_dir = root / "reports"
    for folder in [staging_dir, curated_dir, reports_dir, root / "logs"]:
        folder.mkdir(parents=True, exist_ok=True)

    log_file = root / "logs" / "pipeline.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
        force=True,
    )

    checks: list[dict[str, Any]] = []
    LOGGER.info("Starting Olist e-commerce batch ETL")

    raw: dict[str, pd.DataFrame] = {}
    table_summary: dict[str, Any] = {}
    for table, filename in SOURCE_FILES.items():
        frame = _read_csv(raw_dir / filename, table)
        raw[table] = frame
        table_summary[table] = {
            "source_file": filename,
            "source_rows": int(len(frame)),
            "source_columns": int(len(frame.columns)),
            "source_exact_duplicate_rows": int(frame.duplicated().sum()),
            "source_missing_values_by_column": {
                str(k): int(v) for k, v in frame.isna().sum().items() if int(v) > 0
            },
        }
        LOGGER.info("Extracted %s rows from %s", f"{len(frame):,}", filename)

    tables = _transform_tables(raw, checks)
    _validate_tables(tables, checks)

    # Staged outputs contain standardized column names and data types while retaining source rows.
    for table, frame in tables.items():
        out_file = staging_dir / f"{table}.csv"
        frame.to_csv(out_file, index=False)
        table_summary[table]["staging_rows"] = int(len(frame))
        table_summary[table]["staging_file"] = str(out_file.relative_to(root))

    curated = _make_curated(tables)
    for table, frame in curated.items():
        out_file = curated_dir / f"{table}.csv"
        frame.to_csv(out_file, index=False)
        table_summary[table] = {
            "curated_rows": int(len(frame)),
            "curated_columns": int(len(frame.columns)),
            "curated_file": str(out_file.relative_to(root)),
        }
        LOGGER.info("Wrote %s rows to %s", f"{len(frame):,}", out_file.relative_to(root))

    failures = sum(1 for c in checks if c["status"] == "FAIL")
    warnings = sum(1 for c in checks if c["status"] == "WARN")
    report = {
        "project": "Olist E-Commerce Batch Data Engineering Pipeline",
        "pipeline_version": "1.0.0",
        "status": "FAIL" if failures else ("WARN" if warnings else "PASS"),
        "summary": {
            "source_tables": len(SOURCE_FILES),
            "checks_total": len(checks),
            "checks_passed": sum(1 for c in checks if c["status"] == "PASS"),
            "checks_warned": warnings,
            "checks_failed": failures,
        },
        "tables": table_summary,
        "checks": checks,
        "notes": [
            "Raw source files are never modified by this pipeline.",
            "Known data issues are reported; source rows are preserved unless an explicit rule says otherwise.",
            "review_id is not unique in the supplied source, so fact_reviews uses review_record_id as a row identifier.",
            "This is a full-refresh batch pipeline; it rewrites generated processed outputs on each run.",
        ],
    }
    report_path = reports_dir / "data_quality_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    check_frame = pd.DataFrame(checks)
    check_frame.to_csv(reports_dir / "data_quality_checks.csv", index=False)
    LOGGER.info("Quality report status: %s (%s warnings, %s failures)", report["status"], warnings, failures)
    LOGGER.info("Pipeline finished. Report: %s", report_path.relative_to(root))

    if failures:
        raise RuntimeError(
            f"Pipeline completed with {failures} failing data-quality checks. "
            f"Review {report_path} before using the curated data."
        )
    return report
