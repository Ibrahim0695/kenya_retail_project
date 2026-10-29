"""Download CPI and inflation figures from a KNBS monthly report PDF."""

import json
import re
from datetime import datetime
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin, urlparse

import truststore

truststore.inject_into_ssl()

import pandas as pd
import pdfplumber
import requests

from config import KNBS_REPORTS_PAGE, KNBS_TIMEOUT, LANDING_DIR
from landing import save_to_landing, save_watermark

HEADERS = {"User-Agent": "kenya-retail-school/1.0"}
_REPORT_PAGE_RE = re.compile(
    r"/reports/consumer-price-indices-and-inflation-rates-"
    r"([a-z]+)-(\d{4})/?$",
    re.IGNORECASE,
)
_REPORT_PDF_RE = re.compile(
    r"consumer-price-indices-and-inflation-rates.*\.pdf$",
    re.IGNORECASE,
)
_RELEASE_DATE_RE = re.compile(
    r"Released on:\s*\d{1,2}(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})",
    re.IGNORECASE,
)
_CPI_ROW_RE = re.compile(
    r"^(?P<division>.+?)\s+"
    r"(?P<weight>[\d,]+\.\d+)\s+"
    r"(?P<monthly>-?[\d,]+\.\d+)\s+"
    r"(?P<yearly>-?[\d,]+\.\d+)$"
)


class _LinkParser(HTMLParser):
    """Collect hyperlinks from a KNBS page."""

    def __init__(self) -> None:
        super().__init__()
        self.links = []
        self._href = None
        self._text = []

    def handle_starttag(self, tag, attrs) -> None:
        if tag == "a":
            self._href = dict(attrs).get("href")
            self._text = []

    def handle_data(self, data) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag) -> None:
        if tag == "a" and self._href is not None:
            self.links.append(
                (self._href, " ".join(" ".join(self._text).split()))
            )
            self._href = None
            self._text = []


def _get_page_links(url: str):
    response = requests.get(url, headers=HEADERS, timeout=KNBS_TIMEOUT)
    response.raise_for_status()
    parser = _LinkParser()
    parser.feed(response.text)
    return parser.links


def find_latest_report() -> tuple[str, str]:
    """Find the newest dated CPI report and its PDF link from the KNBS page."""
    report_pages = []
    for href, _ in _get_page_links(KNBS_REPORTS_PAGE):
        page_url = urljoin(KNBS_REPORTS_PAGE, href)
        match = _REPORT_PAGE_RE.search(urlparse(page_url).path)
        if match:
            month_name, year = match.groups()
            try:
                report_date = datetime.strptime(
                    f"{month_name} {year}", "%B %Y"
                )
            except ValueError:
                continue
            report_pages.append((report_date, page_url))

    if not report_pages:
        raise RuntimeError(
            f"No dated CPI report pages were found at {KNBS_REPORTS_PAGE}"
        )

    report_date, report_page_url = max(report_pages, key=lambda item: item[0])
    pdf_links = [
        urljoin(report_page_url, href)
        for href, _ in _get_page_links(report_page_url)
        if _REPORT_PDF_RE.search(urlparse(urljoin(report_page_url, href)).path)
    ]
    if not pdf_links:
        raise RuntimeError(
            f"No CPI report PDF link was found on {report_page_url}"
        )

    return report_date.strftime("%Y-%m"), pdf_links[0]


def download_report(pdf_url: str) -> bytes:
    """Download the selected KNBS CPI report PDF."""
    response = requests.get(
        pdf_url, headers=HEADERS, timeout=KNBS_TIMEOUT
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"KNBS report URL did not return a PDF: {pdf_url}")
    return response.content


def _latest_landed_batch() -> Path | None:
    """Return the newest complete KNBS landing batch available for fallback."""
    source_dir = LANDING_DIR / "knbs_pdf"
    if not source_dir.is_dir():
        return None

    batches = sorted(
        (path for path in source_dir.iterdir() if path.is_dir()),
        key=lambda path: path.name,
        reverse=True,
    )
    for batch in batches:
        manifest_path = batch / "manifest.json"
        data_path = batch / "knbs_cpi_inflation.csv"
        if not manifest_path.is_file() or not data_path.is_file():
            continue
        with manifest_path.open(encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
        if (
            manifest.get("source") == "knbs_pdf"
            and manifest.get("file") == data_path.name
        ):
            return batch
    return None


def extract_cpi_table(pdf_bytes: bytes) -> pd.DataFrame:
    """Extract the division weights and monthly/yearly changes from Table 1."""
    records = []
    period = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if period is None:
                release = _RELEASE_DATE_RE.search(text)
                if release:
                    period = datetime.strptime(
                        f"{release.group(1)} {release.group(2)}", "%B %Y"
                    ).strftime("%Y-%m")

            normalized_text = " ".join(text.split())
            if (
                "Table 1: One and Twelve-Month Percentage Changes "
                "in the Consumer Price Indices"
                not in normalized_text
            ):
                continue

            for line in text.splitlines():
                match = _CPI_ROW_RE.match(line.strip())
                if match:
                    records.append(match.groupdict())

    if period is None:
        raise ValueError("Could not find the report release month and year.")
    if len(records) != 14:
        raise ValueError(
            f"Expected 13 CPI divisions and a total row in Table 1; "
            f"found {len(records)} matching rows."
        )

    rows = []
    division_number = 0
    for record in records:
        division = record["division"].strip()
        if division.casefold() == "total":
            series_id = "CPI_ALL_ITEMS"
            series_name = "All Items CPI"
        else:
            division_number += 1
            series_id = f"CPI_{division_number:02d}"
            series_name = division

        rows.append(
            {
                "series_id": series_id,
                "series_name": series_name,
                "period": period,
                "weight_pct": float(record["weight"].replace(",", "")),
                "month_on_month_pct": float(
                    record["monthly"].replace(",", "")
                ),
                "yoy_inflation_pct": float(record["yearly"].replace(",", "")),
                "source": "KNBS CPI and Inflation Report PDF",
            }
        )

    return pd.DataFrame(rows)


def ingest() -> None:
    """Download, display, and save the CPI summary from the KNBS PDF."""
    try:
        period, pdf_url = find_latest_report()
        print(f"[knbs-pdf] downloading latest report ({period}): {pdf_url}")
        report = download_report(pdf_url)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
        batch = _latest_landed_batch()
        if batch is None:
            raise
        print(
            "[knbs-pdf] WARNING: live KNBS request failed "
            f"({exc}); reusing landed batch {batch.name}"
        )
        return

    cpi = extract_cpi_table(report)

    print(cpi.to_string(index=False))
    period = cpi["period"].iloc[0]
    save_to_landing(
        cpi,
        "knbs_pdf",
        "knbs_cpi_inflation.csv",
        period,
        period,
    )
    save_watermark("knbs_pdf", period)
    print(
        f"[knbs-pdf] extracted {len(cpi):,} rows for {period}"
    )


def main() -> None:
    ingest()


if __name__ == "__main__":
    main()
