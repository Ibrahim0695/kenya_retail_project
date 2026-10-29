"""Schema, completeness, duplicate, and value checks for medallion outputs."""

import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetContract:
    columns: tuple[str, ...]
    key: tuple[str, ...]
    required: tuple[str, ...]
    types: dict[str, str]
    rules: tuple[tuple[str, str], ...] = ()


CONTRACTS = {
    "silver/operational/stores": DatasetContract(
        ("store_id", "store_name", "county", "store_format"),
        ("store_id",),
        ("store_id", "store_name"),
        {
            "store_id": "string",
            "store_name": "string",
            "county": "string",
            "store_format": "string",
        },
    ),
    "silver/operational/products": DatasetContract(
        ("product_id", "product_name", "category", "base_price"),
        ("product_id",),
        ("product_id", "product_name", "base_price"),
        {
            "product_id": "string",
            "product_name": "string",
            "category": "string",
            "base_price": "double",
        },
        (("base_price_nonnegative", "base_price >= 0"),),
    ),
    "silver/operational/suppliers": DatasetContract(
        ("supplier_id", "supplier_name", "county"),
        ("supplier_id",),
        ("supplier_id", "supplier_name"),
        {
            "supplier_id": "string",
            "supplier_name": "string",
            "county": "string",
        },
    ),
    "silver/operational/transactions": DatasetContract(
        ("transaction_id", "store_id", "transaction_ts", "cashier_name", "payment_method"),
        ("transaction_id",),
        ("transaction_id", "store_id", "transaction_ts"),
        {
            "transaction_id": "string",
            "store_id": "string",
            "transaction_ts": "timestamp",
            "cashier_name": "string",
            "payment_method": "string",
        },
    ),
    "silver/operational/transaction_lines": DatasetContract(
        ("transaction_id", "line_number", "product_id", "quantity", "unit_price", "line_total"),
        ("transaction_id", "line_number"),
        ("transaction_id", "line_number", "product_id", "quantity", "unit_price", "line_total"),
        {
            "transaction_id": "string",
            "line_number": "integer",
            "product_id": "string",
            "quantity": "integer",
            "unit_price": "double",
            "line_total": "double",
        },
        (
            ("quantity_positive", "quantity > 0"),
            ("unit_price_nonnegative", "unit_price >= 0"),
            ("line_total_nonnegative", "line_total >= 0"),
        ),
    ),
    "silver/operational/inventory_movements": DatasetContract(
        ("movement_id", "movement_ts", "store_id", "product_id", "movement_type", "quantity", "unit_cost"),
        ("movement_id",),
        ("movement_id", "movement_ts", "store_id", "product_id", "movement_type", "quantity"),
        {
            "movement_id": "string",
            "movement_ts": "timestamp",
            "store_id": "string",
            "product_id": "string",
            "movement_type": "string",
            "quantity": "integer",
            "unit_cost": "double",
        },
        (("unit_cost_nonnegative", "unit_cost >= 0"),),
    ),
    "silver/kamis/prices": DatasetContract(
        ("market", "commodity", "classification", "grade", "sex", "wholesale_price", "retail_price", "unit", "supply_volume", "county", "survey_date", "source"),
        ("market", "commodity", "classification", "grade", "sex", "unit", "county", "survey_date"),
        ("commodity", "survey_date"),
        {
            "market": "string",
            "commodity": "string",
            "classification": "string",
            "grade": "string",
            "sex": "string",
            "wholesale_price": "double",
            "retail_price": "double",
            "unit": "string",
            "supply_volume": "double",
            "county": "string",
            "survey_date": "date",
            "source": "string",
        },
        (
            ("wholesale_price_nonnegative", "wholesale_price IS NULL OR wholesale_price >= 0"),
            ("retail_price_nonnegative", "retail_price IS NULL OR retail_price >= 0"),
        ),
    ),
    "silver/knbs_pdf/cpi_inflation": DatasetContract(
        ("series_id", "series_name", "period", "weight_pct", "month_on_month_pct", "yoy_inflation_pct", "source", "period_date"),
        ("series_id", "period_date"),
        ("series_id", "period_date"),
        {
            "series_id": "string",
            "series_name": "string",
            "period": "string",
            "weight_pct": "double",
            "month_on_month_pct": "double",
            "yoy_inflation_pct": "double",
            "source": "string",
            "period_date": "date",
        },
    ),
    "gold/sales_daily_by_store": DatasetContract(
        ("sale_date", "store_id", "store_name", "county", "payment_method", "transaction_count", "units_sold", "gross_sales"),
        ("sale_date", "store_id", "store_name", "county", "payment_method"),
        ("sale_date", "store_id", "transaction_count", "units_sold", "gross_sales"),
        {
            "sale_date": "date",
            "store_id": "string",
            "store_name": "string",
            "county": "string",
            "payment_method": "string",
            "transaction_count": "long",
            "units_sold": "long",
            "gross_sales": "double",
        },
        (
            ("transaction_count_positive", "transaction_count > 0"),
            ("units_sold_positive", "units_sold > 0"),
            ("gross_sales_nonnegative", "gross_sales >= 0"),
        ),
    ),
    "gold/sales_daily_by_product": DatasetContract(
        ("sale_date", "product_id", "product_name", "category", "transaction_count", "units_sold", "gross_sales"),
        ("sale_date", "product_id", "product_name", "category"),
        ("sale_date", "product_id", "transaction_count", "units_sold", "gross_sales"),
        {
            "sale_date": "date",
            "product_id": "string",
            "product_name": "string",
            "category": "string",
            "transaction_count": "long",
            "units_sold": "long",
            "gross_sales": "double",
        },
        (
            ("transaction_count_positive", "transaction_count > 0"),
            ("units_sold_positive", "units_sold > 0"),
            ("gross_sales_nonnegative", "gross_sales >= 0"),
        ),
    ),
    "gold/inventory_position": DatasetContract(
        ("store_id", "product_id", "on_hand_quantity", "delivered_quantity", "sold_quantity"),
        ("store_id", "product_id"),
        ("store_id", "product_id", "on_hand_quantity", "delivered_quantity", "sold_quantity"),
        {
            "store_id": "string",
            "product_id": "string",
            "on_hand_quantity": "long",
            "delivered_quantity": "long",
            "sold_quantity": "long",
        },
    ),
    "gold/kamis_latest_market_prices": DatasetContract(
        ("market", "commodity", "classification", "grade", "sex", "wholesale_price", "retail_price", "unit", "supply_volume", "county", "survey_date", "source"),
        ("market", "commodity", "classification", "grade", "sex", "unit", "county", "survey_date"),
        ("commodity", "survey_date"),
        {
            "market": "string",
            "commodity": "string",
            "classification": "string",
            "grade": "string",
            "sex": "string",
            "wholesale_price": "double",
            "retail_price": "double",
            "unit": "string",
            "supply_volume": "double",
            "county": "string",
            "survey_date": "date",
            "source": "string",
        },
        (
            ("wholesale_price_nonnegative", "wholesale_price IS NULL OR wholesale_price >= 0"),
            ("retail_price_nonnegative", "retail_price IS NULL OR retail_price >= 0"),
        ),
    ),
    "gold/knbs_latest_cpi_inflation": DatasetContract(
        ("series_id", "series_name", "period", "weight_pct", "month_on_month_pct", "yoy_inflation_pct", "source", "period_date"),
        ("series_id", "period_date"),
        ("series_id", "period_date"),
        {
            "series_id": "string",
            "series_name": "string",
            "period": "string",
            "weight_pct": "double",
            "month_on_month_pct": "double",
            "yoy_inflation_pct": "double",
            "source": "string",
            "period_date": "date",
        },
    ),
}


def _check(
    dataset: str,
    name: str,
    passed: bool,
    observed: Any,
    expected: Any,
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "check": name,
        "status": "PASS" if passed else "FAIL",
        "observed": observed,
        "expected": expected,
    }


def evaluate_frame(frame: DataFrame, dataset: str, contract: DatasetContract) -> list[dict[str, Any]]:
    """Evaluate one Spark frame against its strict schema and data contract."""
    actual_columns = set(frame.columns)
    expected_columns = set(contract.columns)
    missing_columns = sorted(expected_columns - actual_columns)
    unexpected_columns = sorted(actual_columns - expected_columns)
    checks = [
        _check(
            dataset,
            "schema_columns",
            not missing_columns and not unexpected_columns,
            {"missing": missing_columns, "unexpected": unexpected_columns},
            {"columns": list(contract.columns)},
        )
    ]

    actual_types = {field.name: field.dataType.typeName() for field in frame.schema.fields}
    wrong_types = {
        name: {"actual": actual_types.get(name), "expected": expected}
        for name, expected in contract.types.items()
        if actual_types.get(name) != expected
    }
    checks.append(
        _check(dataset, "schema_types", not wrong_types, wrong_types, contract.types)
    )

    if missing_columns or wrong_types:
        return checks

    row_count = frame.count()
    checks.append(_check(dataset, "non_empty", row_count > 0, row_count, "> 0"))

    aggregate_expressions = [
        F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(f"null_{index}")
        for index, name in enumerate(contract.required)
    ]
    aggregate_expressions.extend(
        F.sum(F.when(~F.expr(expression), 1).otherwise(0)).alias(f"rule_{index}")
        for index, (_, expression) in enumerate(contract.rules)
    )
    metrics = frame.agg(*aggregate_expressions).first() if aggregate_expressions else None
    if metrics is not None:
        for index, name in enumerate(contract.required):
            null_count = metrics[f"null_{index}"] or 0
            checks.append(
                _check(dataset, f"required_non_null:{name}", null_count == 0, null_count, 0)
            )
        for index, (rule_name, _) in enumerate(contract.rules):
            invalid_count = metrics[f"rule_{index}"] or 0
            checks.append(
                _check(dataset, rule_name, invalid_count == 0, invalid_count, 0)
            )

    duplicate_count = (
        frame.groupBy(*contract.key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    checks.append(
        _check(
            dataset,
            "duplicate_business_keys",
            duplicate_count == 0,
            duplicate_count,
            0,
        )
    )
    return checks


def _write_report(report_path: Path, report: dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{report_path.name}.",
        suffix=".tmp",
        dir=report_path.parent,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, sort_keys=True)
        report_file.write("\n")
    os.replace(temporary_name, report_path)


def validate_data_quality(
    spark: SparkSession,
    data_dir: Path,
    run_id: str = "manual",
) -> dict[str, Any]:
    """Validate all silver and gold outputs and persist a monitoring report."""
    checks: list[dict[str, Any]] = []
    for relative_path, contract in CONTRACTS.items():
        dataset_path = data_dir / relative_path
        if not dataset_path.is_dir():
            checks.append(
                _check(
                    relative_path,
                    "dataset_exists",
                    False,
                    "missing",
                    "Parquet dataset directory",
                )
            )
            continue
        frame = spark.read.parquet(str(dataset_path))
        checks.append(
            _check(relative_path, "dataset_exists", True, "present", "present")
        )
        checks.extend(evaluate_frame(frame, relative_path, contract))

    failed = [check for check in checks if check["status"] == "FAIL"]
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("._") or "manual"
    report = {
        "run_id": run_id,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "WARN" if failed else "PASS",
        "passed_checks": len(checks) - len(failed),
        "failed_checks": len(failed),
        "checks": checks,
    }
    quality_dir = data_dir / "quality"
    run_report = quality_dir / "reports" / f"{safe_run_id}.json"
    _write_report(run_report, report)
    _write_report(quality_dir / "latest_report.json", report)

    for check in checks:
        log_method = LOGGER.warning if check["status"] == "FAIL" else LOGGER.info
        log_method(
            "[quality] %s dataset=%s check=%s observed=%s expected=%s",
            "WARN" if check["status"] == "FAIL" else "PASS",
            check["dataset"],
            check["check"],
            check["observed"],
            check["expected"],
        )
    LOGGER.info(
        "[quality] summary status=%s passed=%d warnings=%d report=%s",
        report["status"],
        report["passed_checks"],
        report["failed_checks"],
        run_report,
    )
    return report
