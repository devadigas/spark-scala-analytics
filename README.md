# Retail Sales Batch ETL — Spark + Scala + Power BI

This project is a batch ETL pipeline built with Scala and Apache Spark. It reads retail order, customer, product, and return CSV files, cleans and enriches the data, and produces Parquet datasets for sales, customer, and product analytics. The outputs can be explored in Power BI. The sample data is synthetic and intended for learning.

## Data files
- `data/raw/orders.csv`: one row per order line (includes one intentional duplicate to practice deduplication)
- `data/raw/customers.csv`: customer attributes (includes a missing city)
- `data/raw/products.csv`: product and cost master
- `data/raw/returns.csv`: returned orders and reason

All data is synthetic and for learning purposes.

## Architecture
Raw CSV → Spark Scala DataFrames → clean/enriched fact table + aggregates → Parquet → Power BI

## Prerequisites
- JDK 17 (or compatible JDK supported by your Spark setup)
- sbt
- Power BI Desktop (Windows)
- Configure `JAVA_HOME` and ensure `java -version` and `sbt --version` work in PowerShell.

This sample uses Spark 3.5.5 with Scala 2.12. Do not mix it with a Spark installation compiled for a different Scala binary version.

## Run the pipeline
From the project root:
```powershell
sbt run
```
If sbt asks which main class to run, select `com.retail.RetailBatchApp`.

The output will be written to:
- `data/processed/fact_sales/` (Parquet, partitioned by order year)
- `data/processed/monthly_sales/` (Power BI-ready aggregate)
- `data/processed/product_performance/` (product performance aggregate)
- `data/processed/customer_analytics/` and `data/processed/product_analytics/`

## Write outputs to PostgreSQL
The batch app can also write each processed dataset to its own PostgreSQL table:
`fact_sales`, `monthly_sales`, `product_performance`, `customer_analytics`, and
`product_analytics`. Configure the connection in PowerShell before running:

```powershell
$env:PG_URL = "jdbc:postgresql://localhost:5432/retail"
$env:PG_USER = "your_user"
$env:PG_PASSWORD = "your_password"
# Optional; defaults to data.
# The database user needs permission to create this schema and its tables.
$env:PG_SCHEMA = "data"
sbt run
```

The PostgreSQL JDBC driver is included in the build. Before writing each table,
the app runs `DROP TABLE IF EXISTS`, then creates and populates the table to
match the full-batch Parquet output. **This deletes the previous table and its
data on every run.** The configured schema is created automatically if missing.
The app reapplies primary-key, not-null, and check constraints after each write:

- `fact_sales`: primary key on `order_id`; quantity must be positive, sales
  non-negative, and discount between 0 and 1.
- `monthly_sales`: primary key on month, region, and category; positive order
  count and non-negative sales.
- `product_performance`: primary key on category and product name; positive
  units, non-negative sales, and average discount between 0 and 1.
- `customer_analytics`: primary key on `customer_id`; positive orders/units,
  non-negative sales, bounded return count, and return rate between 0 and 1.
- `product_analytics`: primary key on `product_id`; positive orders/units,
  non-negative sales, and bounded return count.

The PostgreSQL user needs permission to create tables and add constraints.
Keep credentials in environment variables or a secret manager; do not commit
them. If `PG_URL` is not set, the pipeline continues to write Parquet and skips
PostgreSQL.

If you run it from another working directory, pass the project root as the first application argument through your IDE/run configuration.

## Run with Airflow

The `retail_spark_scala_etl` DAG runs the Scala batch app inside the local
Airflow container. The Compose setup builds a custom Airflow image with Java 17
and sbt, and mounts this repository at `/opt/airflow/project`.

Start the local Airflow stack from the project root:

```powershell
docker compose up --build airflow-init
docker compose up --build -d
```

In the Airflow UI, unpause `retail_spark_scala_etl` and trigger it manually.
The DAG has no automatic schedule because each run overwrites the Parquet
outputs and, when `PG_URL` is configured, drops and recreates the output
PostgreSQL tables. To enable PostgreSQL writes, provide `PG_URL`, `PG_USER`,
`PG_PASSWORD`, and optionally `PG_SCHEMA` in the Compose `.env` file. For a
database running on the Windows host, use `host.docker.internal` in `PG_URL`
instead of `localhost`.

## Customer and product analytics
`RetailBatchApp` calls `CustomerApp` and `ProductApp` using the same Spark session and prepared fact data. Their outputs are written to `data/processed/customer_analytics/` and `data/processed/product_analytics/`.

## Run tests
Run the JUnit tests from the project root in PowerShell:
```powershell
$env:JDK_JAVA_OPTIONS = "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
sbt test
```

## What the Scala application demonstrates
1. Explicit schema for orders
2. CSV ingestion
3. Date parsing
4. Duplicate removal and invalid-row filtering
5. Handling missing customer city
6. Joining orders with customer/product/returns data
7. Derived columns: return flag, shipping days, year and month
8. Aggregations by month, region and category
9. Parquet writes, including partitioning

## Power BI connection (easy local approach)
Power BI Desktop's folder connector may not always combine nested Parquet partition folders in the way you expect. Start with the unpartitioned aggregates:
1. Open Power BI Desktop.
2. Select **Get data → Parquet**.
3. Browse to `data/processed/monthly_sales` and load the Parquet output. Repeat for `product_performance`.
4. To load the partitioned fact table, use **Get data → Folder**, select `data/processed/fact_sales`, then use **Combine & Transform** if the connector exposes the Parquet files correctly. If that route is awkward, use `monthly_sales` and `product_performance` first, or create an unpartitioned Power BI export from Spark.
5. In Power Query, verify date/numeric types and column names.
6. Create measures:
```DAX
Revenue = SUM(monthly_sales[total_sales])
Profit = SUM(monthly_sales[total_profit])
Orders = SUM(monthly_sales[order_count])
Profit Margin % = DIVIDE([Profit], [Revenue], 0)
```
Note: `order_count` is pre-aggregated by month/region/category. If you slice across multiple categories/regions, summing counts can count the same order more than once. For a correct distinct order KPI, use `fact_sales` and `DISTINCTCOUNT(fact_sales[order_id])`.

## Suggested report pages
### Page 1 — Executive overview
- KPI cards: Revenue, Profit, Profit Margin %, Orders
- Line chart: `order_month` against Revenue and Profit
- Slicers: region, category, order year

### Page 2 — Product performance
- Bar chart: product name by total sales
- Bar chart: product name by total profit
- Scatter chart: average discount vs total profit
- Table: product, units sold, sales, profit

### Page 3 — Operations & returns
- Bar chart: average shipping days by ship mode
- Return rate by region/category
- Table: returned orders and return reason

## Important learning improvements (after first run)
- Add a reject/quarantine output for invalid records rather than silently filtering them.
- Add data quality checks and fail the job if mandatory fields are missing.
- Make returns metrics precise at the order-line grain; returns currently flag an order ID.
- Add a run date / audit table and structured logging.
- Add a date dimension and explicit fact/dimension Parquet outputs.
- Add an optional Airflow DAG to run the batch app on a schedule.
- Push the project to GitHub with README, architecture diagram, sample screenshots and validation evidence.

## Troubleshooting
- `JAVA_HOME` not set: set it to the installed JDK directory and restart PowerShell.
- `sbt` not recognized: check sbt installation and PATH.
- Scala/Spark version conflict: keep Spark 3.5.x and Scala 2.12 for this starter.
- If `sbt run` fails due to network access, confirm sbt can download dependencies.
