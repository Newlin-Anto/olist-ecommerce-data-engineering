# Databricks + AWS S3 setup guide

This project uses Databricks on AWS, PySpark, Delta Lake, Unity Catalog, and S3. The notebooks use direct S3 paths for the data files and register those Delta locations as Unity Catalog external tables. A workspace administrator must first configure governed access with Unity Catalog.

## Prerequisites

- A Databricks workspace attached to a Unity Catalog metastore.
- Permission to use a catalog/schema and execute notebooks on permitted compute.
- A private S3 bucket and an IAM role with least-privilege permissions to the project prefix.
- The Olist CSVs uploaded to `s3://<bucket>/<prefix>/bronze/raw/`.

Avoid putting AWS access keys in notebooks, widgets, Git, or README screenshots.

## 1. Create the S3 bucket

1. Create a bucket in your preferred AWS region. Use a name without dots, because Databricks documents compatibility restrictions for dotted bucket names in external locations.
2. Keep Block Public Access enabled.
3. Leave the bucket private and use default encryption.
4. Keep your source prefix isolated, for example `olist-ecommerce/`.

## 2. Configure Unity Catalog for the S3 prefix

Databricks recommends Unity Catalog to govern cloud-storage access. A workspace/metastore admin can use **Catalog Explorer → Connect → External Locations** and the automated setup flow where available, or manually create a storage credential from an AWS IAM role and then an external location for the intended S3 path.

Useful official instructions:
- https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3
- https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/automatedsetups3
- https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual

The external location should cover `s3://<bucket>/<prefix>/` (or suitable sub-prefixes) and should not be broader than necessary. The notebook's execution identity needs permission to read files under `bronze/raw/`, write files under `silver/`, `gold/`, and `quality/`, and create external tables under the approved location. Depending on your organization's privileges, an admin may need to grant `READ FILES`, `WRITE FILES`, `CREATE EXTERNAL TABLE`, and catalog/schema create/use privileges.

Do not copy a random IAM policy from a tutorial and attach broad `s3:*` access. Follow the trust and resource policies generated/documented for your Databricks account and scope them to the learning bucket/prefix.

## 3. Upload the source CSVs from Windows

From the project root, copy `.env.example` to `.env`. Set `S3_BUCKET` and `S3_PREFIX`. Configure AWS CLI credentials/profile on your PC using your normal AWS workflow, then run:

```powershell
python -m pip install -r requirements-s3.txt
python scripts/upload_raw_to_s3.py --dry-run
python scripts/upload_raw_to_s3.py
```

A dry run lists the upload manifest only. It does not call S3 upload operations.

## 4. Import the notebook source files

Databricks supports notebook source files whose first line uses `# Databricks notebook source`. In the Databricks workspace, use **Workspace → Import** (or your workspace's notebook import action) to import:

- `databricks/01_bronze_to_silver.py`
- `databricks/02_silver_to_gold.py`
- `databricks/03_sql_analytics.sql`

Official format reference: https://docs.databricks.com/aws/en/notebooks/notebook-format

The source `.py` and `.sql` files are deliberately version-control-friendly. Keep the downloaded project in Git and import from these files; don't commit notebook outputs that reveal credentials or customer-level data.

## 5. Configure notebook widgets

In both Python notebooks, set the widgets to the same values:

- `s3_bucket`: your actual bucket name (without `s3://`).
- `s3_prefix`: normally `olist-ecommerce`.
- `catalog_name`: an existing Unity Catalog catalog you can use; the notebooks default to `main`.
- `schema_name`: `olist_de`.

Run notebook 01 first. It reads the nine source CSVs, validates schema/key expectations, standardizes types, writes Silver Delta tables and a data-quality Delta table. Then run notebook 02, which validates foreign-key references and creates the Gold fact/dimension tables. If your catalog isn't named `main`, change the widget and replace the catalog in the SQL analytics queries.

## Common errors

- **AccessDenied / permission denied:** check S3 bucket policy, IAM role trust, storage credential, external location, and grants. The local `.env` AWS profile is used only by the uploader; Databricks uses its governed workspace/Unity Catalog identity.
- **No such key / path not found:** verify that the files are under `s3://<bucket>/<prefix>/bronze/raw/` and names match the Olist source files exactly.
- **CREATE TABLE / external location errors:** your identity may lack `CREATE EXTERNAL TABLE` on the external location or privileges on the catalog/schema. Ask the workspace admin rather than bypassing Unity Catalog.
- **Schema/catalog not found:** choose a catalog present in Catalog Explorer and available to your identity.
- **Cost:** stop compute when you finish and check your workspace/account usage. S3 storage and request charges may also apply.

## Note about free/trial environments

Workspace editions and cloud-storage capabilities differ. Databricks Free Edition is designed for learning but has feature limitations; do not assume that direct integration with your own AWS S3 external location is available in every edition. Confirm the permissions and supported storage features for the workspace you actually have before creating billable AWS or Databricks resources.
