import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from sources import knbs_pdf


class KNBSFallbackTests(unittest.TestCase):
    def test_connection_error_uses_newest_complete_landed_batch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            landing_dir = Path(temp_dir)
            source_dir = landing_dir / "knbs_pdf"
            older_batch = source_dir / "2026-10-25_120000"
            newest_batch = source_dir / "2026-10-26_120000"
            for batch in (older_batch, newest_batch):
                batch.mkdir(parents=True)
                (batch / "knbs_cpi_inflation.csv").write_text(
                    "series_id,period\nCPI_ALL_ITEMS,2026-10\n",
                    encoding="utf-8",
                )
                (batch / "manifest.json").write_text(
                    json.dumps(
                        {
                            "source": "knbs_pdf",
                            "file": "knbs_cpi_inflation.csv",
                        }
                    ),
                    encoding="utf-8",
                )

            output = io.StringIO()
            with (
                patch.object(knbs_pdf, "LANDING_DIR", landing_dir),
                patch.object(
                    knbs_pdf,
                    "find_latest_report",
                    side_effect=requests.exceptions.SSLError("untrusted CA"),
                ),
                patch.object(knbs_pdf, "download_report") as download_report,
                contextlib.redirect_stdout(output),
            ):
                knbs_pdf.ingest()

            self.assertIn(newest_batch.name, output.getvalue())
            self.assertIn("WARNING", output.getvalue())
            download_report.assert_not_called()

    def test_connection_error_without_landed_batch_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                patch.object(knbs_pdf, "LANDING_DIR", Path(temp_dir)),
                patch.object(
                    knbs_pdf,
                    "find_latest_report",
                    side_effect=requests.exceptions.SSLError("untrusted CA"),
                ),
                self.assertRaises(requests.exceptions.SSLError),
            ):
                knbs_pdf.ingest()

    def test_invalid_cached_batch_manifest_is_not_used(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = Path(temp_dir) / "knbs_pdf"
            batch = source_dir / "2026-10-26_120000"
            batch.mkdir(parents=True)
            (batch / "knbs_cpi_inflation.csv").write_text(
                "series_id,period\nCPI_ALL_ITEMS,2026-10\n",
                encoding="utf-8",
            )
            (batch / "manifest.json").write_text(
                json.dumps({"source": "other", "file": "knbs_cpi_inflation.csv"}),
                encoding="utf-8",
            )

            with patch.object(knbs_pdf, "LANDING_DIR", Path(temp_dir)):
                self.assertIsNone(knbs_pdf._latest_landed_batch())


if __name__ == "__main__":
    unittest.main()
