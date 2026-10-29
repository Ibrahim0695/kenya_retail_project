import unittest
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark import data_quality
from spark.data_quality import CONTRACTS, DatasetContract, evaluate_frame


class DataQualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("KenyaRetailDataQualityTests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_duplicate_bad_batch_fails_duplicate_key_check(self):
        bad_batch = self.spark.createDataFrame(
            [(0,), (1,)],
            ["row"],
        ).select(
            F.lit("TX001").alias("transaction_id"),
            F.lit(1).alias("line_number"),
            F.lit("P01").alias("product_id"),
            F.lit(2).alias("quantity"),
            F.lit(10.0).alias("unit_price"),
            F.lit(20.0).alias("line_total"),
        )

        checks = evaluate_frame(
            bad_batch,
            "silver/operational/transaction_lines",
            CONTRACTS["silver/operational/transaction_lines"],
        )
        duplicate_check = next(
            check
            for check in checks
            if check["check"] == "duplicate_business_keys"
        )

        self.assertEqual(duplicate_check["status"], "FAIL")
        self.assertEqual(duplicate_check["observed"], 1)

    def test_missing_schema_column_fails_schema_check(self):
        frame = self.spark.createDataFrame(
            [(0,)],
            ["product_id"],
        ).select(F.col("product_id"))

        checks = evaluate_frame(
            frame,
            "silver/operational/products",
            CONTRACTS["silver/operational/products"],
        )
        schema_check = next(
            check for check in checks if check["check"] == "schema_columns"
        )

        self.assertEqual(schema_check["status"], "FAIL")
        self.assertIn("product_name", schema_check["observed"]["missing"])

    def test_quality_failures_are_reported_as_warnings_without_blocking(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            dataset_path = data_dir / "sample"
            self.spark.createDataFrame([("A",), ("A",)], ["id"]).write.parquet(
                str(dataset_path)
            )
            contract = DatasetContract(
                columns=("id",),
                key=("id",),
                required=("id",),
                types={"id": "string"},
            )
            with patch.dict(data_quality.CONTRACTS, {"sample": contract}, clear=True):
                report = data_quality.validate_data_quality(
                    self.spark,
                    data_dir,
                    run_id="warning-test",
                )

            self.assertEqual(report["status"], "WARN")
            self.assertEqual(report["failed_checks"], 1)
            saved_report = json.loads(
                (data_dir / "quality" / "latest_report.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(saved_report["status"], "WARN")


if __name__ == "__main__":
    unittest.main()
