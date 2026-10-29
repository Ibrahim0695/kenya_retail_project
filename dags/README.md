# Airflow pipeline

The `kenya_retail_daily_pipeline` DAG runs once a day in the `Africa/Nairobi`
timezone. It processes KAMIS prices, KNBS inflation data, and the operational
retail snapshot through these steps:

1. Check the scheduled date range or validate the dates supplied for a manual
   backfill.
2. Ingest KAMIS, KNBS, and operational data in parallel.
3. Use Spark to build the bronze, silver, and gold datasets.
4. Run and record the data-quality checks.
5. Refresh the PostgreSQL `gold` and `warehouse` tables.

The DAG allows one active run at a time and does not backfill missed schedules
automatically. Tasks retry twice with an exponential delay, starting at five
minutes and capped at 30 minutes. If ingestion or Spark itself fails, Airflow
does not run the downstream tasks. Task and DAG failure callbacks log the
relevant run, task, and error details.

## Data quality and duplicate handling

The quality checks look for missing datasets or columns, unexpected column
types, empty data, null required values, duplicate keys, and invalid numeric
values. Each result is written to `data/quality/latest_report.json` and to a
run-specific file in `data/quality/reports/`. The same results appear in the
Airflow task logs.

Quality findings are warnings: a failed check is recorded in the report but
does not stop the PostgreSQL refresh. The report status is `WARN` if any checks
fail and `PASS` if they all pass. Ingestion, Spark runtime, and database errors
are still real task failures and follow the normal retry policy.

Before validation, Spark consolidates repeated KAMIS observations for the same
market, commodity, classification, and survey date. It averages the reported
prices and sums the supply volumes. For operational datasets, Spark keeps one
deterministically selected row for each business key, including transaction
IDs and transaction-line keys, and logs how many duplicate keys it found.

If KNBS cannot be reached because of a TLS or network error, ingestion warns
and reuses the newest complete KNBS batch already in the shared landing
directory. If there is no complete batch to use, ingestion fails and the
downstream steps do not run.

## PostgreSQL

After Spark and the quality-report task finish, the loader refreshes the
PostgreSQL gold marts and warehouse dimensions and facts in a transaction.
In Docker Compose, Airflow connects to the PostgreSQL server on the host at
`host.docker.internal:5432`, using the `DATABASE_*` settings from the project
root `.env` file.

The containerized deployment shares the `data/` directory between Airflow
services. This keeps landing batches, Parquet outputs, and quality reports
available across tasks and container restarts.

## Backfills

To run a manual backfill, provide both `start_date` and `end_date` in
`YYYY-MM-DD` format. The dates are inclusive; a single run can cover at most
31 days and cannot end in the future. For example:

```json
{
  "start_date": "2026-10-01",
  "end_date": "2026-10-07"
}
```

KAMIS is fetched for the requested range. Operational data is rebuilt as a
complete snapshot from the configured `START_DATE` through today, even for a
historical backfill. KNBS ingestion uses the latest published report rather
than a historical month. Longer KAMIS backfills should be split into ranges of
31 days or less.

## Runtime requirements

The Airflow worker image needs this project, the packages in
`spark/requirements.txt`, and Java 17 or newer to run Spark. If you run Spark
directly on Windows instead of in the container, install the Hadoop utilities
described in the [Spark README](../spark/README.md). Keep `.env` private and
make its PostgreSQL settings available to the Airflow task runtime.
