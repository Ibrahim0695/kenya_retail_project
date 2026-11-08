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

The DAG allows one active run at a time, starts on 2026-11-05 in the
`Africa/Nairobi` timezone, and sets `catchup=True`. When the Airflow scheduler
is running and parses the updated DAG, it schedules missed daily intervals from
that start date. This bounds automatic catchup to recent runs rather than
replaying history from 2025. The scheduler may start those runs automatically;
no separate trigger is required. Tasks retry twice with an exponential delay,
starting at five minutes and capped at 30 minutes. If ingestion or Spark itself
fails, Airflow does not run the downstream tasks. Task and DAG failure
callbacks log the relevant run, task, and error details.

## Run evidence

The local Airflow history includes a successful run,
`scheduled__2026-10-27T21:00:00+00:00` (execution date 2026-10-27). All seven
tasks finished successfully. Excerpts from its task logs show KAMIS recovering
after a network failure and the medallion build succeeding on its second
attempt:

```text
[2026-10-29T06:41:00.297+0000] Marking task as UP_FOR_RETRY ... task_id=ingest_kamis
[2026-10-29T06:41:00.302+0000] Retrying pipeline task: ... task_id=ingest_kamis try_number=1
[2026-10-29T06:48:51.018+0000] [kamis] saved 692 rows -> .../2026-10-29_064850
[2026-10-29T06:48:51.067+0000] Marking task as SUCCESS ... task_id=ingest_kamis
```

The same run's Spark build first failed on conflicting KAMIS business keys,
then completed and marked the task successful on its next attempt:

```text
spark.data_quality.DataQualityError: Conflicting duplicate business keys found in KAMIS
[2026-10-29T07:50:22.435+0000] [spark] medallion outputs written under /opt/retail/data
[2026-10-29T07:50:23.821+0000] Marking task as SUCCESS ... task_id=build_bronze_silver_gold
```

The run evidence above predates the 2026-11-05 start date and catchup
configuration. It demonstrates retry recovery, not an automatically caught-up
run.

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

## Manual backfills

Manual backfills remain available when a specific range is needed: trigger the
DAG with both `start_date` and `end_date` in `YYYY-MM-DD` format. The dates are
inclusive; a single run can cover at most 31 days and cannot end in the future.
For example:

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
