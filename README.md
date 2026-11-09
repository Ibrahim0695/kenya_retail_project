# Kenya Retail Data Platform

This project brings together retail data from three sources: market prices
from KAMIS, inflation reports from KNBS, and operational data such as sales and
inventory. It turns those inputs into datasets for analysis and loads them
into PostgreSQL.

## Quick start (PowerShell)

### Prerequisites

- Python 3.10 or newer and Java 17 or newer (required by PySpark).
- PostgreSQL, with connection settings in a private project-root `.env` file.
- Docker Desktop with Docker Compose, and the private `.env.docker` file, for
  the local Kafka broker.
- Network access to KAMIS and KNBS for live source ingestion.

Activate the project environment and install the project and Spark
dependencies:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -r spark\requirements.txt
```

Run the batch path in order from the project root:

```powershell
python run_ingestion.py
python -m spark.pipeline
python -m spark.load_gold_to_postgres
```

The loader uses the PostgreSQL settings in `.env`; keep that file private.
For the orchestrated Docker deployment, including Airflow configuration, see
[`DOCKER.md`](DOCKER.md).

To run the independent Kafka streaming demo, start Kafka and create its topics:

```powershell
docker compose --env-file .env.docker up -d kafka
python -m streaming.topics
```

Start each consumer in its own terminal:

```powershell
python -m streaming.consumer sales
```

```powershell
python -m streaming.consumer inventory
```

Then replay up to 100 events from another terminal:

```powershell
python -m streaming.producer --max-events 100
```

### Results

- Ingested CSV batches and manifests: [`data/landing/`](data/landing/)
- Spark Parquet layers: [`data/bronze/`](data/bronze/),
  [`data/silver/`](data/silver/), and [`data/gold/`](data/gold/)
- Latest quality result and saved reports:
  [`data/quality/latest_report.json`](data/quality/latest_report.json) and
  [`data/quality/reports/`](data/quality/reports/)
- Published PostgreSQL tables: `gold` and `warehouse` schemas
- Example warehouse schema and queries:
  [`spark/warehouse_model.md`](spark/warehouse_model.md) and
  [`spark/warehouse_queries.sql`](spark/warehouse_queries.sql)

## How it works

```mermaid
flowchart LR
    K[KAMIS market prices] --> L[CSV landing batches]
    N[KNBS inflation report] --> L
    O[Operational retail data] --> L
    L --> B[Bronze: raw Parquet]
    B --> S[Silver: cleaned and typed data]
    S --> G[Gold: analytics-ready datasets]
    G --> P[PostgreSQL: gold and warehouse schemas]
    A[Airflow schedules and coordinates the batch pipeline] -.-> L
    A -.-> B
    A -.-> P

    R[Operational CSVs] --> PR[Kafka replay producer]
    PR --> T[Versioned Kafka topics]
    T --> C[Sales and inventory consumers]
```

The scheduled Airflow pipeline downloads KAMIS prices, gets the latest KNBS
report, and builds an operational snapshot. Each source is saved as a
timestamped CSV batch in `data/landing/`. Spark then creates three Parquet
layers: bronze keeps the source data, silver cleans and types it, and gold
provides datasets ready for reporting and analysis.

Before publishing, the pipeline records data-quality checks, then refreshes
the PostgreSQL tables. Quality issues are reported as warnings; failures in
ingestion, Spark, or PostgreSQL stop the run.

The `gold` schema holds the published datasets. The `warehouse` schema holds
the dimensions and facts used for more detailed analysis.

There is also a small Kafka demo. It replays operational CSV data to versioned
topics, where separate consumers process sales and inventory events. This is
an independent local workflow, not part of the scheduled batch pipeline; its
consumer state is not persisted between runs.

## Example landing batches

Raw batches are written under `data/landing/<source>/<timestamp>/`.

| Source | Batch folder | Date range | Rows |
| --- | --- | --- | ---: |
| KAMIS | [`data/landing/kamis/2026-10-29_064850/`](data/landing/kamis/2026-10-29_064850/) | 2026-10-27 to 2026-10-28 | 692 |
| KNBS CPI | [`data/landing/knbs_pdf/2026-10-26_122403/`](data/landing/knbs_pdf/2026-10-26_122403/) | 2026-09 | 14 |
| Operational | [`data/landing/operational/2026-10-29_064112/`](data/landing/operational/2026-10-29_064112/) | 2025-04-01 to 2026-10-29 | 6 CSV tables |

The KAMIS and KNBS folders each contain a CSV and `manifest.json`. For example,
the [KAMIS manifest](data/landing/kamis/2026-10-29_064850/manifest.json)
records `source: kamis`, `rows: 692`, the date range above, and
`file: kamis_prices.csv`. Its [CSV data](data/landing/kamis/2026-10-29_064850/kamis_prices.csv)
starts with this header and a sample row:

```csv
market,commodity,classification,grade,sex,wholesale_price,retail_price,unit,supply_volume,county,survey_date,source
Vanga,Octopus (Pweza),Fresh,-,-,500.0,700.0,Kg,69.0,Kwale,2026-10-27,KAMIS
```

The [operational batch](data/landing/operational/2026-10-29_064112/) has one
manifest and six CSVs:
`store_master.csv` (6 rows), `product_master.csv` (12), `supplier_master.csv`
(5), `pos_transactions.csv` (22,550), `pos_transaction_lines.csv` (56,512),
and `inventory_movements.csv` (33,314).

## Where to look

- [`sources/`](sources/) contains the KAMIS, KNBS, and operational ingestors.
- [`dags/`](dags/) contains the Airflow pipeline and
  [its documentation](dags/README.md).
- [`spark/`](spark/) contains the transformations, quality checks, and
  PostgreSQL loader. See the [Spark documentation](spark/README.md).
- [`streaming/`](streaming/) contains the Kafka producer and consumers. See
  the [streaming documentation](streaming/README.md).
- [`data/`](data/) holds landed source batches, generated datasets, and
  quality reports.

For instructions on running the project with Docker, see [DOCKER.md](DOCKER.md).

## Running platform

The [Airflow DAG overview](evidence/deployment/2026-11-09/airflow-dags-overview.png)
and [Airflow Grid screenshot](evidence/deployment/2026-11-09/airflow-dag-running.png)
show the platform and a manual pipeline run active during Spark processing. The
[Compose service snapshot](evidence/deployment/2026-11-09/docker-compose-ps.txt)
records the scheduler, webserver, metadata database, and Kafka broker healthy.
See the [deployment evidence index](evidence/deployment/2026-11-09/README.md)
for test results and the run's task states. The screenshot and run record show
an in-progress execution, not a successful end-to-end run.

![Airflow DAG overview showing the retail pipeline active](evidence/deployment/2026-11-09/airflow-dags-overview.png)

![Airflow Grid showing the pipeline run during Spark processing](evidence/deployment/2026-11-09/airflow-dag-running.png)

## What I would do next

- Complete and capture a successful end-to-end Airflow run against PostgreSQL.
- Deploy to the intended hosting platform and move PostgreSQL, secrets, and
  persistent storage to managed services.
- Persist Kafka consumer state and add production monitoring and alerting.
