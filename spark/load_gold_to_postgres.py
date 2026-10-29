"""Load the generated gold Parquet datasets into PostgreSQL."""

import os
import re
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)
from psycopg import sql
from pyspark.sql import functions as F

from spark.warehouse_model import build_warehouse_model

PROJECT_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_DIR / "data" / "gold"
SILVER_DIR = PROJECT_DIR / "data" / "silver"
TARGET_SCHEMA = "gold"
REQUIRED_ENV_VARS = (
    "WAREHOUSE_DB_NAME",
    "WAREHOUSE_DB_USER",
    "WAREHOUSE_DB_PASSWORD",
)
DATABASE_ENV_VARS = {
    "WAREHOUSE_DB_NAME": "DATABASE_NAME",
    "WAREHOUSE_DB_HOST": "DATABASE_HOST",
    "WAREHOUSE_DB_PORT": "DATABASE_PORT",
    "WAREHOUSE_DB_USER": "DATABASE_USER",
    "WAREHOUSE_DB_PASSWORD": "DATABASE_PASSWORD",
}


def _postgres_type(data_type: object) -> str:
    """Map supported Spark Parquet types to PostgreSQL column types."""
    if isinstance(data_type, StringType):
        return "TEXT"
    if isinstance(data_type, BinaryType):
        return "BYTEA"
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, (ByteType, ShortType, IntegerType, LongType)):
        return "BIGINT"
    if isinstance(data_type, (FloatType, DoubleType)):
        return "DOUBLE PRECISION"
    if isinstance(data_type, DecimalType):
        return f"NUMERIC({data_type.precision}, {data_type.scale})"
    if isinstance(data_type, DateType):
        return "DATE"
    if type(data_type).__name__ in ("TimestampType", "TimestampNTZType"):
        return "TIMESTAMP"
    raise TypeError(f"Unsupported Spark column type: {data_type}")


def _table_name(dataset_path: Path) -> str:
    name = dataset_path.name
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise ValueError(f"Invalid gold dataset name for a PostgreSQL table: {name!r}")
    return name


def _prepare_table(cursor: psycopg.Cursor, table_name: str, frame: DataFrame) -> None:
    if not frame.schema.fields:
        raise ValueError(f"Gold dataset {table_name!r} has no columns")

    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(TARGET_SCHEMA),
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                sql.SQL("{} {}").format(
                    sql.Identifier(field.name),
                    sql.SQL(_postgres_type(field.dataType)),
                )
                for field in frame.schema.fields
            ),
        )
    )
    for field in frame.schema.fields:
        cursor.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                sql.Identifier(TARGET_SCHEMA),
                sql.Identifier(table_name),
                sql.Identifier(field.name),
                sql.SQL(_postgres_type(field.dataType)),
            )
        )
    cursor.execute(
        sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(TARGET_SCHEMA),
            sql.Identifier(table_name),
        )
    )


def _copy_rows(
    cursor: psycopg.Cursor,
    table_name: str,
    frame: DataFrame,
) -> int:
    columns = sql.SQL(", ").join(
        sql.Identifier(column) for column in frame.columns
    )
    copy_statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(TARGET_SCHEMA),
        sql.Identifier(table_name),
        columns,
    )

    row_count = 0
    with cursor.copy(copy_statement) as copy:
        for row in frame.toLocalIterator():
            copy.write_row(tuple(row))
            row_count += 1
    return row_count


def _stage_frame(
    cursor: psycopg.Cursor,
    table_name: str,
    create_statement: str,
    frame: DataFrame,
) -> int:
    cursor.execute(create_statement)
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in frame.columns)
    copy_statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table_name),
        columns,
    )
    row_count = 0
    with cursor.copy(copy_statement) as copy:
        for row in frame.toLocalIterator():
            copy.write_row(tuple(row))
            row_count += 1
    print(f"[postgres] staged {row_count:,} rows from {table_name}")
    return row_count


def _stage_warehouse_sources(cursor: psycopg.Cursor, spark: SparkSession) -> None:
    operational_dir = SILVER_DIR / "operational"
    kamis_path = SILVER_DIR / "kamis" / "prices"
    knbs_path = SILVER_DIR / "knbs_pdf" / "cpi_inflation"
    required_paths = (
        operational_dir / "transaction_lines",
        operational_dir / "transactions",
        kamis_path,
        knbs_path,
    )
    missing = [str(path) for path in required_paths if not path.is_dir()]
    if missing:
        raise FileNotFoundError(
            "Required silver datasets for dimensional facts are missing: "
            + ", ".join(missing)
        )

    transactions = spark.read.parquet(
        str(operational_dir / "transactions")
    ).select("transaction_id", "store_id", "transaction_ts")
    transaction_lines = spark.read.parquet(
        str(operational_dir / "transaction_lines")
    )
    sales_lines = (
        transaction_lines.join(transactions, "transaction_id")
        .select(
            "transaction_id",
            "line_number",
            "store_id",
            "product_id",
            F.to_date("transaction_ts").alias("sale_date"),
            F.col("quantity").alias("quantity_sold"),
            "unit_price",
            F.col("line_total").alias("total_amount"),
        )
        .dropna(
            subset=[
                "transaction_id",
                "line_number",
                "store_id",
                "product_id",
                "sale_date",
                "quantity_sold",
                "unit_price",
                "total_amount",
            ]
        )
    )
    _stage_frame(
        cursor,
        "stage_sales_lines",
        """
        CREATE TEMP TABLE stage_sales_lines (
            transaction_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            store_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            sale_date DATE NOT NULL,
            quantity_sold BIGINT NOT NULL,
            unit_price DOUBLE PRECISION NOT NULL,
            total_amount DOUBLE PRECISION NOT NULL
        ) ON COMMIT DROP
        """,
        sales_lines,
    )

    market_prices = spark.read.parquet(str(kamis_path)).select(
        "market",
        "county",
        "commodity",
        "classification",
        "grade",
        "sex",
        "unit",
        "survey_date",
        "wholesale_price",
        "retail_price",
        "supply_volume",
    )
    _stage_frame(
        cursor,
        "stage_market_prices",
        """
        CREATE TEMP TABLE stage_market_prices (
            market TEXT,
            county TEXT,
            commodity TEXT,
            classification TEXT,
            grade TEXT,
            sex TEXT,
            unit TEXT,
            survey_date DATE,
            wholesale_price DOUBLE PRECISION,
            retail_price DOUBLE PRECISION,
            supply_volume DOUBLE PRECISION
        ) ON COMMIT DROP
        """,
        market_prices,
    )

    macro_indicators = spark.read.parquet(str(knbs_path)).select(
        "series_id",
        "series_name",
        "period_date",
        "weight_pct",
        "month_on_month_pct",
        "yoy_inflation_pct",
    )
    _stage_frame(
        cursor,
        "stage_macro_indicators",
        """
        CREATE TEMP TABLE stage_macro_indicators (
            series_id TEXT NOT NULL,
            series_name TEXT,
            period_date DATE NOT NULL,
            weight_pct DOUBLE PRECISION,
            month_on_month_pct DOUBLE PRECISION,
            yoy_inflation_pct DOUBLE PRECISION
        ) ON COMMIT DROP
        """,
        macro_indicators,
    )


def load_gold_to_postgres() -> None:
    """Replace PostgreSQL gold tables with the current Parquet datasets."""
    load_dotenv(PROJECT_DIR / ".env")
    load_dotenv(PROJECT_DIR / ".env.docker")
    settings = {
        name: os.environ.get(database_name) or os.environ.get(name)
        for name, database_name in DATABASE_ENV_VARS.items()
    }
    missing = [name for name in REQUIRED_ENV_VARS if settings[name] is None]
    if missing:
        raise RuntimeError(
            "Missing required local warehouse environment variables: "
            + ", ".join(missing)
        )

    port_value = settings["WAREHOUSE_DB_PORT"] or "5433"
    try:
        port = int(port_value)
    except ValueError as exc:
        raise ValueError("WAREHOUSE_DB_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("WAREHOUSE_DB_PORT must be between 1 and 65535")

    if not GOLD_DIR.is_dir():
        raise FileNotFoundError(f"Gold Parquet directory not found: {GOLD_DIR}")
    datasets = sorted(path for path in GOLD_DIR.iterdir() if path.is_dir())
    if not datasets:
        raise FileNotFoundError(f"No gold Parquet datasets found under {GOLD_DIR}")

    spark = SparkSession.builder.appName("KenyaRetailGoldToPostgres").getOrCreate()
    try:
        with psycopg.connect(
            dbname=settings["WAREHOUSE_DB_NAME"],
            host=settings["WAREHOUSE_DB_HOST"] or "localhost",
            user=settings["WAREHOUSE_DB_USER"],
            password=settings["WAREHOUSE_DB_PASSWORD"],
            port=port,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(TARGET_SCHEMA)
                    )
                )
                for dataset_path in datasets:
                    table_name = _table_name(dataset_path)
                    frame = spark.read.parquet(str(dataset_path))
                    print(f"[postgres] loading {TARGET_SCHEMA}.{table_name}")
                    _prepare_table(cursor, table_name, frame)
                    row_count = _copy_rows(cursor, table_name, frame)
                    print(
                        f"[postgres] loaded {row_count:,} rows into "
                        f"{TARGET_SCHEMA}.{table_name}"
                    )
                _stage_warehouse_sources(cursor, spark)
                build_warehouse_model(cursor)
                print("[postgres] refreshed warehouse dimensions and fact tables")
    finally:
        spark.stop()


if __name__ == "__main__":
    load_gold_to_postgres()
