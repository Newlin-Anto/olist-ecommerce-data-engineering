# Interview preparation topics — Olist Python / PySpark / SQL / AWS / Databricks project

Work through these only after running the relevant component and reading its code.

## Architecture
1. Explain the flow from local Olist CSV files to S3 Bronze, Silver/Gold Delta tables, Unity Catalog, and SQL analytics.
2. Why use S3 for object storage and Databricks for distributed processing/SQL?
3. What are Bronze, Silver, and Gold layers? Are these formal product requirements or a project design choice?
4. What does Unity Catalog govern, and how does a storage credential differ from an external location?

## Python and ETL
1. Explain Extract, Transform, Load in this project.
2. Why are identifiers represented as strings?
3. How does the local `Path`-based pipeline locate files regardless of the terminal's current directory?
4. How are exceptions, missing values, malformed timestamps, and logging handled?
5. What is the difference between a warning and a failing data-quality check?

## PySpark and Delta Lake
1. What is a Spark DataFrame?
2. Explain transformations vs actions and lazy evaluation.
3. Why can `count()` trigger a Spark job?
4. Why keep raw Bronze data immutable?
5. Why store Silver/Gold as Delta instead of CSV?
6. What does `mode("overwrite")` do, and what are the risks of a full refresh?
7. What is the difference between `repartition` and `coalesce`? When would you partition data?
8. Explain Delta Lake transaction logs and schema enforcement/evolution at a high level.

## SQL and dimensional modeling
1. Define primary key, foreign key, and grain.
2. Why are order items and payments separate fact tables?
3. Why can directly joining two one-to-many fact tables double count revenue?
4. Explain INNER JOIN vs LEFT JOIN, CTEs, GROUP BY, and window functions.
5. Why create `dim_date` and `date_key`?
6. Write queries for monthly item value, category sales, late deliveries, review-score distribution, and payment mix.

## AWS and Databricks security
1. Why keep the S3 bucket private and never commit credentials?
2. Compare IAM roles / temporary credentials to hard-coded access keys.
3. What does least privilege mean?
4. How does Unity Catalog integrate S3 through storage credentials and external locations?
5. Why isn't a laptop's AWS CLI profile automatically used by Databricks compute?
6. What AWS and Databricks costs could this project generate?

## Known source-data anomalies to explain
- 610 products have no source product category.
- Some product categories lack an English translation.
- Some order lifecycle timestamps are absent; don't impute them blindly.
- Source `review_id` values can repeat across orders; preserve the source rows and document the anomaly.
- The geolocation source includes exact duplicates and multiple rows per ZIP prefix.

## Strong next improvements
- Incremental ingestion using a watermark or source manifest.
- Databricks Workflows or Lakeflow Jobs for scheduling/retries.
- More deterministic ingestion IDs and idempotency checks.
- Unit tests for pure transformations plus cloud integration tests in a development workspace.
- Metrics and alerting on row counts, orphan keys, timestamp parsing, and run duration.
