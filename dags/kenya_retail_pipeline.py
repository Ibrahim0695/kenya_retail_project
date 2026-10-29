"""Scheduled ingestion and medallion pipeline for Kenya Retail School."""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

LOGGER = logging.getLogger("airflow.task")
MAX_BACKFILL_DAYS = 31


def _date_range_from_context(context: dict) -> tuple[date, date]:
    """Resolve an inclusive source date range from a scheduled interval or run conf."""
    dag_run = context["dag_run"]
    run_conf = dag_run.conf or {}
    configured_start = run_conf.get("start_date")
    configured_end = run_conf.get("end_date")

    if bool(configured_start) != bool(configured_end):
        raise AirflowFailException(
            "Provide both start_date and end_date in the DAG run configuration."
        )

    if configured_start and configured_end:
        try:
            start = date.fromisoformat(configured_start)
            end = date.fromisoformat(configured_end)
        except (TypeError, ValueError) as exc:
            raise AirflowFailException(
                "DAG run start_date and end_date must use YYYY-MM-DD."
            ) from exc
    else:
        interval_start = context.get("data_interval_start")
        interval_end = context.get("data_interval_end")
        if interval_start is None or interval_end is None:
            raise AirflowFailException(
                "No data interval; provide start_date and end_date in DAG run conf."
            )
        start = interval_start.date()
        end = (interval_end - timedelta(microseconds=1)).date()

    if start > end:
        raise AirflowFailException("start_date must be on or before end_date.")
    if (end - start).days + 1 > MAX_BACKFILL_DAYS:
        raise AirflowFailException(
            f"Process at most {MAX_BACKFILL_DAYS} days per DAG run."
        )
    if end > pendulum.today("Africa/Nairobi").date():
        raise AirflowFailException("end_date cannot be in the future.")
    return start, end


def _on_dag_failure(context: dict) -> None:
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    LOGGER.error(
        "Pipeline run failed: dag_id=%s run_id=%s error=%r",
        getattr(dag_run, "dag_id", "unknown"),
        getattr(dag_run, "run_id", "unknown"),
        exception,
    )


def _on_task_failure(context: dict) -> None:
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    LOGGER.error(
        "Pipeline task failed: dag_id=%s task_id=%s run_id=%s error=%r",
        getattr(task_instance, "dag_id", "unknown"),
        getattr(task_instance, "task_id", "unknown"),
        context.get("run_id", "unknown"),
        exception,
    )


def _on_task_retry(context: dict) -> None:
    task_instance = context.get("task_instance")
    LOGGER.warning(
        "Retrying pipeline task: dag_id=%s task_id=%s try_number=%s",
        getattr(task_instance, "dag_id", "unknown"),
        getattr(task_instance, "task_id", "unknown"),
        getattr(task_instance, "try_number", "unknown"),
    )


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": _on_task_failure,
    "on_retry_callback": _on_task_retry,
}


@dag(
    dag_id="kenya_retail_daily_pipeline",
    description="Ingest retail sources and build the Kenya Retail medallion layers.",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 4, 1, tz="Africa/Nairobi"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=_on_dag_failure,
    tags=["kenya-retail", "medallion", "postgres"],
)
def kenya_retail_daily_pipeline():
    @task(task_id="validate_run_configuration")
    def validate_run_configuration() -> None:
        start, end = _date_range_from_context(get_current_context())
        LOGGER.info("Validated inclusive processing range: %s through %s", start, end)

    @task(
        task_id="ingest_kamis",
        execution_timeout=timedelta(hours=1),
    )
    def ingest_kamis() -> None:
        start, end = _date_range_from_context(get_current_context())
        from sources.kamis import ingest

        LOGGER.info("Ingesting KAMIS prices for %s through %s", start, end)
        ingest(start_date=start.isoformat(), end_date=end.isoformat())

    @task(
        task_id="ingest_knbs_latest_report",
        execution_timeout=timedelta(minutes=30),
    )
    def ingest_knbs_latest_report() -> None:
        from sources.knbs_pdf import ingest

        LOGGER.info("Ingesting the latest published KNBS CPI report")
        ingest()

    @task(
        task_id="ingest_operational_snapshot",
        execution_timeout=timedelta(hours=1),
    )
    def ingest_operational_snapshot() -> None:
        from config import START_DATE
        from sources.operational import ingest

        _, requested_end = _date_range_from_context(get_current_context())
        operational_end = max(
            requested_end,
            pendulum.today("Africa/Nairobi").date(),
        )
        LOGGER.info(
            "Building full operational snapshot from %s through %s",
            START_DATE,
            operational_end,
        )
        ingest(start=START_DATE, end=operational_end.isoformat())

    @task(
        task_id="build_bronze_silver_gold",
        execution_timeout=timedelta(hours=3),
    )
    def build_medallion_layers() -> None:
        from spark.pipeline import run_pipeline

        LOGGER.info("Building bronze, silver, and gold Parquet datasets")
        run_pipeline()

    @task(
        task_id="validate_data_quality",
        execution_timeout=timedelta(hours=1),
        retries=0,
    )
    def validate_data_quality() -> None:
        from config import DATA_DIR
        from pyspark.sql import SparkSession
        from spark.data_quality import validate_data_quality as validate_datasets

        context = get_current_context()
        spark = SparkSession.builder.appName("KenyaRetailDataQuality").getOrCreate()
        try:
            report = validate_datasets(
                spark,
                DATA_DIR,
                run_id=context.get("run_id", "unknown"),
            )
            LOGGER.info(
                "Data quality report status=%s failed_checks=%d",
                report["status"],
                report["failed_checks"],
            )
        finally:
            spark.stop()

    @task(
        task_id="load_postgres_warehouse",
        execution_timeout=timedelta(hours=2),
    )
    def load_postgres_warehouse() -> None:
        context = get_current_context()
        from spark.load_gold_to_postgres import load_gold_to_postgres

        LOGGER.info(
            "Refreshing local PostgreSQL gold and warehouse tables for run %s",
            context.get("run_id", "unknown"),
        )
        load_gold_to_postgres()

    validation = validate_run_configuration()
    kamis = ingest_kamis()
    knbs = ingest_knbs_latest_report()
    operational = ingest_operational_snapshot()
    medallion = build_medallion_layers()
    quality = validate_data_quality()
    warehouse = load_postgres_warehouse()

    validation >> [kamis, knbs, operational]
    [kamis, knbs, operational] >> medallion >> quality >> warehouse


dag = kenya_retail_daily_pipeline()
