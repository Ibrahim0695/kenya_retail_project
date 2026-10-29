"""Transform landed KAMIS, KNBS, and operational data into medallion layers."""

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import DATA_DIR, LANDING_DIR

LOGGER = logging.getLogger(__name__)

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

OPERATIONAL_FILES = {
    "stores": "store_master.csv",
    "products": "product_master.csv",
    "suppliers": "supplier_master.csv",
    "transactions": "pos_transactions.csv",
    "transaction_lines": "pos_transaction_lines.csv",
    "inventory_movements": "inventory_movements.csv",
}


def _batches(source: str) -> list[Path]:
    source_dir = LANDING_DIR / source
    if not source_dir.is_dir():
        return []
    return sorted(path for path in source_dir.iterdir() if path.is_dir())


def _read_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Read a CSV while preserving its source values as strings."""
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .csv(str(path))
    )


def _write_parquet(df: DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(path))


def _configure_windows_hadoop() -> Path | None:
    """Configure the installed Hadoop utilities before the Spark JVM starts."""
    if os.name != "nt":
        return None

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        local_app_data = os.environ.get("LOCALAPPDATA")
        if not local_app_data:
            raise RuntimeError(
                "HADOOP_HOME is unset and LOCALAPPDATA is unavailable; "
                "configure a Windows Hadoop installation before running Spark."
            )
        hadoop_home = str(Path(local_app_data) / "hadoop" / "hadoop-3.3.6")

    hadoop_home_path = Path(hadoop_home).expanduser().resolve()
    hadoop_bin = hadoop_home_path / "bin"
    if not (hadoop_bin / "winutils.exe").is_file():
        raise FileNotFoundError(
            f"Windows Hadoop utilities were not found under {hadoop_bin}. "
            "Install winutils.exe or set HADOOP_HOME to a valid Hadoop directory."
        )

    os.environ["HADOOP_HOME"] = str(hadoop_home_path)
    path_entries = os.environ.get("PATH", "").split(os.pathsep)
    if not any(
        os.path.normcase(os.path.normpath(entry)) == os.path.normcase(str(hadoop_bin))
        for entry in path_entries
        if entry
    ):
        os.environ["PATH"] = os.pathsep.join([str(hadoop_bin), *path_entries])

    return hadoop_home_path


def _write_bronze(spark: SparkSession) -> None:
    """Copy every landed source file to a source-tagged Parquet bronze table."""
    sources = {
        "kamis": [(batch, batch / "kamis_prices.csv") for batch in _batches("kamis")],
        "knbs_pdf": [
            (batch, batch / "knbs_cpi_inflation.csv")
            for batch in _batches("knbs_pdf")
        ],
    }
    for source, batches in sources.items():
        _write_bronze_files(spark, source, source, batches)

    operational_batches = _batches("operational")
    for table, filename in OPERATIONAL_FILES.items():
        _write_bronze_files(
            spark,
            "operational",
            table,
            [
                (batch, batch / filename)
                for batch in operational_batches
                if (batch / filename).is_file()
            ],
        )


def _write_bronze_files(
    spark: SparkSession,
    source: str,
    table: str,
    files: list[tuple[Path, Path]],
) -> None:
    frames = []
    for batch, path in files:
        if not path.is_file():
            continue
        frames.append(
            _read_csv(spark, path)
            .withColumn("_source", F.lit(source))
            .withColumn("_source_batch", F.lit(batch.name))
            .withColumn("_source_file", F.lit(path.name))
            .withColumn("_ingested_at", F.current_timestamp())
        )
    if frames:
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame)
        _write_parquet(combined, BRONZE_DIR / source / table)
        print(f"[spark:bronze] {source}/{table}: {combined.count():,} rows")


def _clean_strings(df: DataFrame) -> DataFrame:
    for name, data_type in df.dtypes:
        if data_type == "string":
            value = F.trim(F.col(name))
            df = df.withColumn(
                name,
                F.when(
                    F.lower(value).isin("", "-", "nan", "none", "null"),
                    F.lit(None),
                ).otherwise(value),
            )
    return df


def _cast(df: DataFrame, types: dict[str, object]) -> DataFrame:
    for name, data_type in types.items():
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast(data_type))
    return df


def _latest_batch(source: str) -> Path:
    batches = _batches(source)
    if not batches:
        raise FileNotFoundError(f"No landing batches found for source {source!r}")
    return batches[-1]


def _deduplicate_business_keys(
    frame: DataFrame,
    source: str,
    key: list[str],
) -> DataFrame:
    """Keep one deterministic row per business key and report discarded duplicates."""
    duplicate_keys = (
        frame.groupBy(*key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_keys:
        LOGGER.warning(
            "[silver] deduplicating %d duplicate business keys in %s",
            duplicate_keys,
            source,
        )
        ordering = [
            F.col(name).asc_nulls_last()
            for name in frame.columns
            if name not in key
        ]
        if not ordering:
            ordering = [F.lit(1)]
        frame = (
            frame.withColumn(
                "_dedupe_row",
                F.row_number().over(
                    Window.partitionBy(*key).orderBy(*ordering)
                ),
            )
            .filter(F.col("_dedupe_row") == 1)
            .drop("_dedupe_row")
        )
    return frame


def _aggregate_kamis_observations(frame: DataFrame, key: list[str]) -> DataFrame:
    """Consolidate multiple KAMIS observations into the market/day grain."""
    aggregations = [
        F.avg("wholesale_price").alias("wholesale_price"),
        F.avg("retail_price").alias("retail_price"),
        F.sum("supply_volume").alias("supply_volume"),
    ]
    if "source" in frame.columns:
        aggregations.append(F.first("source", ignorenulls=True).alias("source"))
    return frame.groupBy(*key).agg(*aggregations)


def _silver_operational(spark: SparkSession) -> dict[str, DataFrame]:
    """Clean the newest operational snapshot and preserve invalid rows for QA."""
    batch = _latest_batch("operational")
    stores = _clean_strings(_read_csv(spark, batch / OPERATIONAL_FILES["stores"]))

    products = _clean_strings(_read_csv(spark, batch / OPERATIONAL_FILES["products"]))
    products = _cast(products, {"base_price": DoubleType()})

    suppliers = _clean_strings(
        _read_csv(spark, batch / OPERATIONAL_FILES["suppliers"])
    )

    transactions = _clean_strings(
        _read_csv(spark, batch / OPERATIONAL_FILES["transactions"])
    )
    transactions = transactions.withColumn(
        "transaction_ts", F.to_timestamp("transaction_ts")
    )

    lines = _clean_strings(
        _read_csv(spark, batch / OPERATIONAL_FILES["transaction_lines"])
    )
    lines = _cast(
        lines,
        {
            "line_number": IntegerType(),
            "quantity": IntegerType(),
            "unit_price": DoubleType(),
            "line_total": DoubleType(),
        },
    )

    movements = _clean_strings(
        _read_csv(spark, batch / OPERATIONAL_FILES["inventory_movements"])
    )
    movements = _cast(
        movements,
        {"quantity": IntegerType(), "unit_cost": DoubleType()},
    ).withColumn("movement_ts", F.to_timestamp("movement_ts"))

    result = {
        "stores": _deduplicate_business_keys(
            stores, "operational/stores", ["store_id"]
        ),
        "products": _deduplicate_business_keys(
            products, "operational/products", ["product_id"]
        ),
        "suppliers": _deduplicate_business_keys(
            suppliers, "operational/suppliers", ["supplier_id"]
        ),
        "transactions": _deduplicate_business_keys(
            transactions, "operational/transactions", ["transaction_id"]
        ),
        "transaction_lines": _deduplicate_business_keys(
            lines,
            "operational/transaction_lines",
            ["transaction_id", "line_number"],
        ),
        "inventory_movements": _deduplicate_business_keys(
            movements, "operational/inventory_movements", ["movement_id"]
        ),
    }
    for table, frame in result.items():
        _write_parquet(frame, SILVER_DIR / "operational" / table)
        print(f"[spark:silver] operational/{table}: {frame.count():,} rows")
    return result


def _silver_kamis(spark: SparkSession) -> DataFrame:
    frames = [
        _read_csv(spark, batch / "kamis_prices.csv")
        for batch in _batches("kamis")
        if (batch / "kamis_prices.csv").is_file()
    ]
    if not frames:
        raise FileNotFoundError(f"No KAMIS CSV data found under {LANDING_DIR}")
    prices = frames[0]
    for frame in frames[1:]:
        prices = prices.unionByName(frame)
    prices = _clean_strings(prices)
    prices = _cast(
        prices,
        {
            "wholesale_price": DoubleType(),
            "retail_price": DoubleType(),
            "supply_volume": DoubleType(),
        },
    ).withColumn("survey_date", F.to_date("survey_date"))
    key = [
        "market",
        "commodity",
        "classification",
        "grade",
        "sex",
        "unit",
        "county",
        "survey_date",
    ]
    source_rows = prices.count()
    prices = _aggregate_kamis_observations(prices, key)
    aggregated_rows = prices.count()
    print(
        "[spark:silver] KAMIS observations consolidated: "
        f"{source_rows:,} rows -> {aggregated_rows:,} market/day rows"
    )
    _write_parquet(prices, SILVER_DIR / "kamis" / "prices")
    print(f"[spark:silver] kamis/prices: {prices.count():,} rows")
    return prices


def _silver_knbs(spark: SparkSession) -> DataFrame:
    frames = [
        _read_csv(spark, batch / "knbs_cpi_inflation.csv")
        for batch in _batches("knbs_pdf")
        if (batch / "knbs_cpi_inflation.csv").is_file()
    ]
    if not frames:
        raise FileNotFoundError(f"No KNBS PDF CSV data found under {LANDING_DIR}")
    cpi = frames[0]
    for frame in frames[1:]:
        cpi = cpi.unionByName(frame)
    cpi = _clean_strings(cpi)
    cpi = _cast(
        cpi,
        {
            "weight_pct": DoubleType(),
            "month_on_month_pct": DoubleType(),
            "yoy_inflation_pct": DoubleType(),
        },
    ).withColumn("period_date", F.to_date(F.concat("period", F.lit("-01"))))
    key = ["series_id", "period_date"]
    cpi = _deduplicate_business_keys(cpi, "KNBS CPI", key)
    _write_parquet(cpi, SILVER_DIR / "knbs_pdf" / "cpi_inflation")
    print(f"[spark:silver] knbs_pdf/cpi_inflation: {cpi.count():,} rows")
    return cpi


def _write_gold(
    operational: dict[str, DataFrame],
    kamis: DataFrame,
    knbs: DataFrame,
) -> None:
    """Create business-facing daily sales, stock, market-price, and CPI marts."""
    transactions = operational["transactions"].alias("t")
    lines = operational["transaction_lines"].alias("l")
    products = operational["products"].alias("p")
    stores = operational["stores"].alias("s")

    sales = (
        lines.join(transactions, "transaction_id")
        .join(products, "product_id")
        .join(stores, "store_id")
        .withColumn("sale_date", F.to_date("transaction_ts"))
    )
    sales_by_store = sales.groupBy(
        "sale_date",
        "store_id",
        "store_name",
        "county",
        "payment_method",
    ).agg(
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("gross_sales"),
    )
    sales_by_product = sales.groupBy(
        "sale_date",
        "product_id",
        "product_name",
        "category",
    ).agg(
        F.countDistinct("transaction_id").alias("transaction_count"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("gross_sales"),
    )
    stock = operational["inventory_movements"].groupBy(
        "store_id", "product_id"
    ).agg(
        F.sum("quantity").alias("on_hand_quantity"),
        F.sum(
            F.when(F.col("movement_type") == "DELIVERY", F.col("quantity"))
            .otherwise(F.lit(0))
        ).alias("delivered_quantity"),
        F.sum(
            F.when(F.col("movement_type") == "SALE", -F.col("quantity"))
            .otherwise(F.lit(0))
        ).alias("sold_quantity"),
    )

    latest_kamis_date = kamis.agg(F.max("survey_date").alias("latest_date"))
    market_prices = kamis.join(
        latest_kamis_date,
        kamis.survey_date == latest_kamis_date.latest_date,
        "inner",
    ).drop("latest_date")
    latest_knbs_period = knbs.agg(F.max("period_date").alias("latest_period"))
    latest_cpi = knbs.join(
        latest_knbs_period,
        knbs.period_date == latest_knbs_period.latest_period,
        "inner",
    ).drop("latest_period")

    marts = {
        "sales_daily_by_store": sales_by_store,
        "sales_daily_by_product": sales_by_product,
        "inventory_position": stock,
        "kamis_latest_market_prices": market_prices,
        "knbs_latest_cpi_inflation": latest_cpi,
    }
    for name, frame in marts.items():
        _write_parquet(frame, GOLD_DIR / name)
        print(f"[spark:gold] {name}: {frame.count():,} rows")


def run_pipeline() -> None:
    """Run the batch transformations from landing CSVs into Parquet layers."""
    for source in ("kamis", "knbs_pdf", "operational"):
        if not _batches(source):
            raise FileNotFoundError(
                f"No landed data for {source!r} under {LANDING_DIR}"
            )

    hadoop_home = _configure_windows_hadoop()
    spark = (
        SparkSession.builder.appName("KenyaRetailMedallionPipeline")
        .config("spark.sql.session.timeZone", "Africa/Nairobi")
    )
    if hadoop_home is not None:
        spark = spark.config("spark.hadoop.hadoop.home.dir", str(hadoop_home))
    spark = spark.getOrCreate()
    try:
        _write_bronze(spark)
        operational = _silver_operational(spark)
        kamis = _silver_kamis(spark)
        knbs = _silver_knbs(spark)
        _write_gold(operational, kamis, knbs)
        print(f"[spark] medallion outputs written under {DATA_DIR}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run_pipeline()


if __name__ == "__main__":
    main()
