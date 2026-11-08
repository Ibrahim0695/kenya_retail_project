# Spark medallion pipeline

This batch pipeline reads the KAMIS, KNBS PDF, and operational CSV snapshots
under `data/landing/` and writes Parquet datasets into:

- `data/bronze/`: every landed CSV, preserving its raw values, with source,
  source-batch, source-file, and ingestion-time metadata.
- `data/silver/`: typed and cleaned tables. It combines and de-duplicates the
  KAMIS and KNBS report snapshots; repeated KAMIS observations at the same
  market/commodity/classification/date grain are consolidated using mean prices
  and summed supply volume. For operational data it uses the newest full
  snapshot to avoid multiplying repeated synthetic history.
- `data/gold/`: daily sales by store and product, inventory position, latest
  KAMIS market prices, and latest KNBS CPI/inflation report.

Writes use Parquet and overwrite the generated dataset on each complete run,
making the batch repeatable without duplicating output rows. Landing CSV files
remain unchanged. The KNBS PDF output contains inflation percentages and
weights, not CPI index values.

## Install and run (PowerShell)

Use Java 17 or newer and a Python environment supported by PySpark:

```powershell
cd "C:\Users\Ibrahim Njuguna\desktop\astronomer\kenya-retail-school"
& "C:\Users\Ibrahim Njuguna\python-database\myenvironment\Scripts\python.exe" -m pip install -r spark\requirements.txt
& "C:\Users\Ibrahim Njuguna\python-database\myenvironment\Scripts\python.exe" -m spark.pipeline
```

On Windows, the pipeline configures `HADOOP_HOME` from the user-local
`%LOCALAPPDATA%\hadoop\hadoop-3.3.6` installation when it is not already set.
If Hadoop is installed elsewhere, set `HADOOP_HOME` to that directory before
running the command.

The command expects a complete landing batch for each of `kamis`, `knbs_pdf`,
and `operational`. A Spark runtime error is reported rather than silently
skipping a missing source.

## Load gold datasets into PostgreSQL

The loader reads the existing Parquet datasets under `data/gold/` and refreshes
the corresponding tables in PostgreSQL's `gold` schema and the dimensional
warehouse. It loads the project-root `.env` first, then `.env.docker`, and
prefers `DATABASE_NAME`, `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_USER`,
and `DATABASE_PASSWORD`. If those are absent, it accepts the legacy
`WAREHOUSE_DB_*` variables; the fallback host is `localhost` and the fallback
port is `5433`.

Install the Spark dependencies if needed, then run from the project root:

```powershell
python -m pip install -r spark\requirements.txt
python -m spark.load_gold_to_postgres
```

The loader refreshes the five `gold` tables and the `warehouse` dimensions and
facts in a single transaction. For the containerized deployment, `.env`
provides the host PostgreSQL credentials and Compose overrides the host and
port to `host.docker.internal:5432` inside the Airflow containers.

The five generated datasets become `gold.sales_daily_by_store`,
`gold.sales_daily_by_product`, `gold.inventory_position`,
`gold.kamis_latest_market_prices`, and `gold.knbs_latest_cpi_inflation`.
All table refreshes run in one PostgreSQL transaction, so a failed load rolls
back rather than leaving a partially refreshed gold layer. Existing tables are
truncated and reused, preserving their table identities and downstream
references; new Parquet columns are added as nullable columns.

The same command also maintains a dimensional model in the separate
`warehouse` schema. Its dimensions are `dim_store`, `dim_product`, `dim_market`,
and a continuous `dim_date` calendar covering the source date range. Fact tables
include:

- `fact_sales`: one row per POS transaction line, sourced from silver transaction
  lines joined to their transaction date and store. It retains package-level
  quantity/pricing and adds standardized quantity and price measures using each
  product's declared package size.
- `fact_market_prices`: one row per commodity/market/date and classification;
  it retains the source price unit and reported prices. Normalized measures
  cover KAMIS kilogram and litre units; `_per_kg` measures are populated only
  for kilogram reports.
- `fact_macro_indicators`: one row per KNBS series and month, sourced from the
  complete silver history.
- `fact_sales_by_store`, `fact_sales_by_product`,
  `fact_inventory_position`, and `fact_cpi_inflation`: supplementary marts
  retained from the initial warehouse model.

The fact tables reference dimension surrogate keys and refresh in the same
transaction as the gold marts, leaving the `gold` tables available unchanged
for existing queries. Store regions are derived from the available county
values. Package sizes are explicit in the warehouse model (for example 2 kg
for Maize Flour 2kg and 0.4 kg for Bread 400g); products with uncertain KAMIS
equivalence retain a null `market_mapping_code`. The price-competitiveness
query compares only mapped products and matching base units, using recent
supermarket sales against the latest available KAMIS survey per commodity.
KNBS currently supplies inflation rates and weights, not CPI index values, so
`fact_macro_indicators.cpi_index` is nullable.

For the warehouse ERD, PostgreSQL DDL, table grains, and example analytics SQL,
see [warehouse_model.md](warehouse_model.md) and
[warehouse_queries.sql](warehouse_queries.sql).

For scheduled ingestion, retries, backfills, and the PostgreSQL quality gate,
see the [Airflow DAG documentation](../dags/README.md).

## Medallion output evidence

The following counts are from a successful Spark 3.5.4 run on 2026-11-08 using
the landed batches in `data/landing/`. Bronze retains every available snapshot;
Silver deduplicates or consolidates records; Gold changes to reporting grains.

| Example dataset | Bronze | Silver | Gold |
| --- | ---: | ---: | ---: |
| KAMIS prices | 8,886 source observations | 3,276 market/day rows | 415 latest-market rows |
| KNBS CPI | 56 rows across reports | 14 series/month rows | 14 latest-report rows |
| Operational sales lines | 297,783 rows across snapshots | 56,512 rows from newest snapshot | 6,923 daily product rows |

Selected lines from the successful run:

```text
[spark:bronze] kamis/kamis: 8,886 rows
[spark:bronze] operational/transaction_lines: 297,783 rows
[spark:silver] operational/transaction_lines: 56,512 rows
[spark:silver] KAMIS observations consolidated: 8,886 rows -> 3,276 market/day rows
[spark:gold] sales_daily_by_product: 6,923 rows
[spark] medallion outputs written under /opt/retail/data
```

For example, the KAMIS key `(Lungalunga, Water Melon, -, -, -, Kg, Kwale,
2026-10-16)` appears twice in the source data:

| Layer | Wholesale | Retail | Supply volume | Rows |
| --- | ---: | ---: | ---: | ---: |
| Bronze observations | 100, 30 | 150, 50 | 700, 800 | 2 |
| Silver consolidated | 65 | 100 | 1,500 | 1 |

Silver averages the prices and sums supply volume at the market/day grain.
For a type change, `base_price` is `string` in Bronze
`operational/products` and `double` in Silver `operational/products`.

Representative column/type schemas from that run:

| Layer and dataset | Columns (`name:type`) |
| --- | --- |
| Bronze `operational/products` | `product_id:string`, `product_name:string`, `category:string`, `base_price:string`, `_source:string`, `_source_batch:string`, `_source_file:string`, `_ingested_at:timestamp` |
| Silver `operational/products` | `product_id:string`, `product_name:string`, `category:string`, `base_price:double` |
| Gold `sales_daily_by_store` | `sale_date:date`, `store_id:string`, `store_name:string`, `county:string`, `payment_method:string`, `transaction_count:bigint`, `units_sold:bigint`, `gross_sales:double` |

## Data quality gate

The Airflow DAG runs `spark.data_quality.validate_data_quality` after building
the medallion layers and before loading PostgreSQL. It checks silver/gold
column and type contracts, non-empty datasets, required values, unique
business keys, and basic numeric rules. Findings are recorded as warnings in
`data/quality/` and do not block PostgreSQL refresh. The silver transforms
deduplicate operational business keys and aggregate repeated KAMIS market/day
observations before data publication. Actual ingestion, Spark, or database
errors still fail the run; see the DAG documentation for operational details.
