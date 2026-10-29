# Kenya Retail Data Platform

This project brings together retail data from three sources: market prices
from KAMIS, inflation reports from KNBS, and operational data such as sales and
inventory. It turns those inputs into datasets for analysis and loads them
into PostgreSQL.

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
