import os
import sys
import unittest

from pyspark.sql import SparkSession

from spark.pipeline import (
    _aggregate_kamis_observations,
    _deduplicate_business_keys,
)


class KAMISAggregationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("KenyaRetailKAMISAggregationTests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_multiple_observations_are_averaged_and_supply_is_summed(self):
        observations = self.spark.createDataFrame(
            [
                ("Market A", "Maize", "2026-10-28", 100.0, 120.0, 10.0, "KAMIS"),
                ("Market A", "Maize", "2026-10-28", 140.0, 160.0, 15.0, "KAMIS"),
                ("Market B", "Maize", "2026-10-28", 90.0, 110.0, 7.0, "KAMIS"),
            ],
            [
                "market",
                "commodity",
                "survey_date",
                "wholesale_price",
                "retail_price",
                "supply_volume",
                "source",
            ],
        )

        result = _aggregate_kamis_observations(
            observations,
            ["market", "commodity", "survey_date"],
        )
        rows = {
            (row.market, row.commodity, row.survey_date): row
            for row in result.collect()
        }

        self.assertEqual(len(rows), 2)
        market_a = rows[("Market A", "Maize", "2026-10-28")]
        self.assertEqual(market_a.wholesale_price, 120.0)
        self.assertEqual(market_a.retail_price, 140.0)
        self.assertEqual(market_a.supply_volume, 25.0)
        self.assertEqual(market_a.source, "KAMIS")

    def test_conflicting_transaction_line_keys_are_deduplicated(self):
        lines = self.spark.createDataFrame(
            [
                ("TX001", 1, "P01", 2),
                ("TX001", 1, "P02", 1),
            ],
            ["transaction_id", "line_number", "product_id", "quantity"],
        )

        result = _deduplicate_business_keys(
            lines,
            "operational/transaction_lines",
            ["transaction_id", "line_number"],
        )

        self.assertEqual(result.count(), 1)
        row = result.first()
        self.assertEqual((row.transaction_id, row.line_number), ("TX001", 1))
        self.assertEqual(row.product_id, "P01")


if __name__ == "__main__":
    unittest.main()
