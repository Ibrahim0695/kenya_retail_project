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

## Configure and deploy

Keep the project-root `.env` with the host PostgreSQL connection settings:
`DATABASE_NAME`, `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_USER`, and
`DATABASE_PASSWORD`. Keep it private.

Create the Airflow-specific environment file and fill its local secrets:

```powershell
Copy-Item .env.docker.example .env.docker
```

Set distinct values for `AIRFLOW_DB_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`,
`AIRFLOW_FERNET_KEY`, and `AIRFLOW_WEBSERVER_SECRET_KEY`; set
`AIRFLOW_ADMIN_USER` as desired. The metadata database password must be
URL-safe because Compose places it in Airflow's SQLAlchemy URL. Generate a
Fernet key with:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Your host PostgreSQL must accept TCP connections from Docker. It cannot be
restricted to loopback only; its `listen_addresses` and `pg_hba.conf` must allow
the Docker host/network.

Build and start the services:

```powershell
docker compose --env-file .env.docker up -d --build
docker compose --env-file .env.docker ps
```

Airflow initialization migrates its metadata database, creates the admin user
if absent, and unpauses `kenya_retail_daily_pipeline`. Sign in at
`http://localhost:8080` using the configured admin credentials. Successful
daily runs ingest sources, build bronze/silver/gold, validate data quality, and
refresh the host PostgreSQL gold and warehouse schemas.

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
