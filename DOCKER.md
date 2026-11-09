# Local containerized platform

The Compose stack runs Airflow's scheduler and web UI, the Airflow metadata
PostgreSQL database, and Kafka. The retail warehouse remains the existing host
PostgreSQL service at `localhost:5432`; containers connect to it as
`host.docker.internal:5432`.

The application image includes Java 17, Airflow, PySpark, and project
dependencies. The host `data/` directory is bind-mounted into all Airflow
services, so landed source files, Parquet outputs, and quality reports are
shared and persist across container rebuilds. The DAG and source-code folders
are bind-mounted into the scheduler and webserver so local code changes are
used without reinstalling the large Spark dependency layer.

## Deploy

Build and start the services:

```powershell
docker compose --env-file .env.docker up -d --build
docker compose --env-file .env.docker ps
```

The commands require private local `.env` and `.env.docker` files; do not commit
them. Your host PostgreSQL must accept TCP connections from Docker. It cannot
be restricted to loopback only; its `listen_addresses` and `pg_hba.conf` must
allow the Docker host/network.

Airflow initialization migrates its metadata database, creates the admin user
if absent, and unpauses `kenya_retail_daily_pipeline`. Sign in at
`http://localhost:8080` using the configured admin credentials. Successful
daily runs ingest sources, build bronze/silver/gold, validate data quality, and
refresh the host PostgreSQL gold and warehouse schemas.

## Verification evidence

The test suite was run on 2026-11-08 with Python 3.12.1 in the configured
project environment:

```text
python -m unittest discover tests
............
Ran 12 tests in 83.268s

OK
```

The `docker compose --env-file .env.docker ps` snapshot captured on 2026-11-08
showed all long-running services healthy:

```text
SERVICE             STATE     HEALTH
airflow-db          running   healthy
airflow-scheduler   running   healthy
airflow-webserver   running   healthy
kafka               running   healthy
```

The one-shot `airflow-init` container exited with code 0 after initialization.
The Airflow webserver health endpoint returned HTTP 200 and reported both the
metadatabase and scheduler as healthy.

The scheduler was briefly started to verify an Airflow execution, but its
persisted `DagModel.next_dagrun` still pointed to
`2026-10-28T21:00:00+00:00`, before the DAG's configured `2026-11-05` start
date. With `catchup=True`, it created that older run. The observed state when
the scheduler was stopped was:

```text
run_id: scheduled__2026-10-28T21:00:00+00:00
run state: running
validate_run_configuration: success
ingest_kamis: success
ingest_knbs_latest_report: success
ingest_operational_snapshot: success
build_bronze_silver_gold: running
validate_data_quality: not started
load_postgres_warehouse: not started
```

Thus, this is evidence of a partial execution, not a successful end-to-end
DAG run. The ingestion tasks created local landing batches, and the Spark
build had started; quality validation and the warehouse refresh had not run
when the scheduler was first stopped. After the scheduler was restarted, this
run resumed: its Spark build is running on attempt 2, while quality validation
and PostgreSQL loading have not yet started. The run is not yet a successful
end-to-end DAG execution. The scheduler's stored next-run pointer still refers
to the October 28 interval, before the configured November 5 DAG start date;
monitor the active run and resolve the stale scheduling pointer carefully,
since catchup may schedule intervals outside the intended window.

Airflow's earlier stored history includes a successful
`kenya_retail_daily_pipeline` run (`scheduled__2026-10-27T21:00:00+00:00`),
but that is separate from the interrupted run above.

## PostgreSQL and pipeline monitoring

Local database clients connect to `localhost:5432` with the `DATABASE_*`
settings in `.env`. Containers use `host.docker.internal:5432`. The current
quality report is written to `data/quality/latest_report.json` and reports are
retained in `data/quality/reports/`. Airflow task logs also show individual
checks; failed checks are warnings and are recorded without blocking the
warehouse refresh. Ingestion, Spark runtime, and database connection errors
still follow Airflow's retry/failure policy.

## Backfills and operations

Manual DAG runs accept an inclusive `start_date`/`end_date` range in
`YYYY-MM-DD` format, limited to 31 days and not beyond today.

```json
{
  "start_date": "2026-10-01",
  "end_date": "2026-10-07"
}
```

Useful commands:

```powershell
docker compose --env-file .env.docker logs -f airflow-scheduler
docker compose --env-file .env.docker logs -f airflow-webserver
docker compose --env-file .env.docker exec airflow-webserver airflow dags list
docker compose --env-file .env.docker down
```

`docker compose down` preserves named Airflow volumes and the project `data/`
directory. `docker compose down -v` deletes Airflow metadata/log volumes, but
does not delete the host PostgreSQL database or project data.

This is a local development deployment, not an internet-facing production
configuration. Published service ports bind to loopback; keep `.env` and
`.env.docker` private.

The latest test, service-health, and Airflow UI evidence is in
[`evidence/deployment/2026-11-09/`](evidence/deployment/2026-11-09/).
