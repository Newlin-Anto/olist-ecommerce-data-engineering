# Olist E-Commerce Data Engineering Pipeline — Python, PySpark, SQL, AWS, Databricks

A portfolio-ready Level 2 batch data engineering project built with the Brazilian Olist e-commerce public dataset. The main cloud pipeline uses **Amazon S3 for object storage, Databricks + PySpark for processing, Delta Lake for curated data, Unity Catalog for table governance, and Databricks SQL for analytics**.

> The ZIP does not contain the Olist data. Download it from [the Olist dataset page on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), follow its license, and keep the raw CSVs out of a public GitHub repository.

## Architecture

```text
Olist CSV files on your PC
          |
          | scripts/upload_raw_to_s3.py (boto3)
          v
Amazon S3 — Bronze/raw (source CSVs, unchanged)
          |
          | Databricks notebook 01 — PySpark
          v
Amazon S3 — Silver (cleaned/typed Delta tables)
          |
          | Databricks notebook 02 — PySpark + dimensional modeling
          v
Amazon S3 — Gold (fact/dimension Delta tables)
          |
          v
Unity Catalog external tables + Databricks SQL analytics
```

A local Pandas reference pipeline is also included for local testing, early data profiling and repeatable quality checks. The Databricks notebooks implement the cloud path and write Silver/Gold Delta data to the S3 prefix registered in Unity Catalog. The local and cloud execution paths are documented separately on purpose.

## What's included

- `src/ecommerce_de/pipeline.py`: local reference batch ETL that profiles/validates nine sources and emits staging CSVs, curated CSVs and data-quality reports.
- `scripts/run_pipeline.py`: run the local reference pipeline.
- `scripts/upload_raw_to_s3.py`: upload only source CSV files to a private S3 `bronze/raw/` prefix; supports `--dry-run`.
- `databricks/01_bronze_to_silver.py`: importable Databricks notebook source; reads S3 CSVs, checks source schemas, standardizes ZIP prefixes, casts dates/numbers, checks keys, and writes Silver Delta tables plus a quality table.
- `databricks/02_silver_to_gold.py`: validates relationships and builds dimensions/facts as Gold Delta tables.
- `databricks/03_sql_analytics.sql`: Databricks SQL analysis queries.
- `tests/`: automated local tests for the reference ETL and S3 manifest.
- `docs/data_dictionary.md`, `docs/interview_topics.md`, `docs/databricks_setup.md`: schema notes, setup guide and interview topics.

## 1. Download and place the source data

1. Download and extract the Olist dataset from Kaggle.
2. Copy its nine CSV files into `data/raw/`.
3. Keep the source files unchanged. The pipeline writes outputs to different paths.

Expected file names:

```text
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

## 2. Run the local reference pipeline (recommended first)

In Windows PowerShell from the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts/run_pipeline.py
python -m pytest -q
```

The local reference pipeline writes to `data/processed/staging/`, `data/processed/curated/`, `reports/`, and `logs/`. Review `reports/data_quality_report.json`; expected source anomalies are warnings, not automatically deleted records.

## 3. Upload raw files to Amazon S3

Use a **private** S3 bucket. Databricks Unity Catalog external locations have compatibility requirements; use a bucket name without dots. Keep Block Public Access enabled and never commit credentials or `.env`.

Install optional uploader dependencies:

```powershell
python -m pip install -r requirements-s3.txt
```

Copy `.env.example` to `.env` and set your bucket/prefix. Use AWS CLI profile/role credentials; don't paste access keys into source code.

```dotenv
AWS_REGION=ap-south-1
AWS_PROFILE=
S3_BUCKET=your-private-bucket-name
S3_PREFIX=olist-ecommerce
```

Preview first:

```powershell
python scripts/upload_raw_to_s3.py --dry-run
```

Then upload:

```powershell
python scripts/upload_raw_to_s3.py
```

Expected raw prefix:

```text
s3://<bucket>/olist-ecommerce/bronze/raw/<source-file>.csv
```

## 4. Configure Databricks access to S3

Follow `docs/databricks_setup.md`. This project is designed for Databricks on AWS with Unity Catalog. You or your workspace admin must configure a Unity Catalog storage credential and external location scoped to your project prefix. The notebook needs permission to read the Bronze objects and write under `silver/`, `gold/` and `quality/` within that approved location.

Official docs:
- [Connect Databricks to an S3 external location](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3)
- [Databricks notebook source format](https://docs.databricks.com/aws/en/notebooks/notebook-format)

Do not give the notebook unrestricted bucket access. Use least privilege and a private development bucket.

## 5. Import and run Databricks notebooks

Import these files into a Databricks workspace as notebooks. Databricks recognizes Python and SQL source notebooks by the `Databricks notebook source` header. In both Python notebooks, fill the widgets:

- `s3_bucket`: your private bucket name
- `s3_prefix`: `olist-ecommerce` (or the prefix configured in `.env`)
- `catalog_name`: a Unity Catalog catalog you have permission to use (default `main`)
- `schema_name`: `olist_de` (or another simple schema name)

Run in this order:

1. `databricks/01_bronze_to_silver.py`
2. `databricks/02_silver_to_gold.py`
3. Import/run `databricks/03_sql_analytics.sql` as a SQL notebook, or copy its queries to Databricks SQL.

The notebooks write Delta directories to `silver/`, `gold/` and `quality/` in the configured S3 prefix, then register those paths as Unity Catalog tables. The configured external location must permit external-table creation at these paths. The first run can take several minutes because geolocation has about one million rows.

Tables produced in `<catalog>.olist_de`:

**Silver:** `silver_customers`, `silver_geolocation`, `silver_order_items`, `silver_payments`, `silver_reviews`, `silver_orders`, `silver_products`, `silver_sellers`, `silver_category_translation`.

**Gold:** `dim_customers`, `dim_products`, `dim_sellers`, `dim_date`, `fact_orders`, `fact_order_items`, `fact_payments`, `fact_reviews`.

**Quality:** `data_quality_checks`.

## Data modeling notes

- `dim_customers`: one row per source customer record (`customer_id`).
- `dim_products`: one row per product (`product_id`), including translated category label and translation status.
- `dim_sellers`: one row per seller (`seller_id`).
- `dim_date`: one row per distinct purchase date (`date_key`, formatted `YYYYMMDD`).
- `fact_orders`: one row per order (`order_id`).
- `fact_order_items`: one row per `(order_id, order_item_id)`; `item_total = price + freight_value`.
- `fact_payments`: one row per `(order_id, payment_sequential)`.
- `fact_reviews`: one row per source review record, keyed by a run-level `review_record_id`; raw `review_id` repeats across orders in this dataset.

Keep order items and payment facts at separate grains. Joining two one-to-many fact tables directly before aggregation can multiply amounts and lead to double counting.

## Data-quality choices

- Raw source objects remain unchanged in `bronze/raw/`.
- Postal/ZIP prefixes are treated as strings to preserve leading zeroes.
- Missing order lifecycle timestamps are not imputed; a missing delivery time can be valid for canceled or in-progress orders.
- Repeated source `review_id` values are logged as warnings and rows are preserved.
- Product categories that have no English translation keep their source category label; genuinely missing categories are marked `unknown` in the reporting field.
- Geolocation duplicates are reported and retained in Silver, not blindly removed.

## GitHub safety

Upload code, SQL, notebooks, documentation, tests and README. Do **not** commit `data/raw/`, generated outputs, `.env`, AWS credentials, customer-level exports or logs. `.gitignore` is configured to exclude local data, generated outputs and secrets.

## Cost and limitations

- AWS S3 can incur storage and request costs. Use a private learning bucket and delete unused objects when finished.
- Databricks compute can incur charges depending on your workspace/account. Stop compute when done and monitor usage.
- Actual S3/Databricks execution isn't covered by the local automated tests. Test the cloud configuration in your own account before claiming the full cloud run is verified.
- This is a full-refresh batch project. Incremental loads, Airflow/Lakeflow orchestration and streaming are future improvements, not claims about the current version.
