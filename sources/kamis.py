"""KAMIS: real market prices from https://kamis.kilimo.go.ke.

The site can export one day of prices as an Excel file. We ask for one day at a
time (the export is capped at ~100 rows per request) and glue the days together.
"""

import re
import time
from datetime import date, timedelta
from io import BytesIO

import pandas as pd
import requests

from config import (
    KAMIS_DAYS_BACK,
    KAMIS_RETRIES,
    KAMIS_TIMEOUT,
    KAMIS_URL,
)      

from landing import save_to_landing, save_watermark

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
    ),
    "Referer": KAMIS_URL,
}

# Prices look like "250.00/Kg", "250.00/ Kg" or "600.00/Tray(30)".
_PRICE_RE = re.compile(r"^\s*([\d,]+(?:\.\d+)?)\s*/\s*(.+?)\s*$")


def parse_price(text):
    """Split '250.00/Kg' into (250.00, 'Kg'). Return (None, None) if missing."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None, None
    text = str(text).strip()
    if text in ("", "-", "nan", "None"):
        return None, None

    match = _PRICE_RE.match(text)
    if match:
        return float(match.group(1).replace(",", "")), match.group(2)

    try:
        return float(text.replace(",", "")), None
    except ValueError:
        return None, None


def download_day(day: str) -> pd.DataFrame:
    """Download and clean one day of KAMIS prices."""
    params = {"start": day, "end": day, "per_page": 3000, "export": "excel"}
    response = requests.get(
        KAMIS_URL, params=params, headers=HEADERS, timeout=KAMIS_TIMEOUT
    )
    response.raise_for_status()
    raw = pd.read_excel(BytesIO(response.content))

    wholesale = raw["Wholesale"].apply(parse_price)
    retail = raw["Retail"].apply(parse_price)

    clean = pd.DataFrame(
        {
            "market": raw["Market"].astype(str).str.strip(),
            "commodity": raw["Commodity"].astype(str).str.strip(),
            "classification": raw["Classification"].astype(str),
            "grade": raw["Grade"].astype(str),
            "sex": raw["Sex"].astype(str),
            "wholesale_price": [value for value, _ in wholesale],
            "retail_price": [value for value, _ in retail],
            "unit": [unit for _, unit in retail],
            "supply_volume": pd.to_numeric(raw["Supply Volume"], errors="coerce"),
            "county": raw["County"].astype(str).str.strip(),
            "survey_date": pd.to_datetime(raw["Date"], errors="coerce").dt.strftime(
                "%Y-%m-%d"
            ),
            "source": "KAMIS",
        }
    )
    return clean.dropna(subset=["commodity", "survey_date"])


def download_day_with_retries(day: str) -> pd.DataFrame:
    last_error = None
    for attempt in range(1, KAMIS_RETRIES + 1):
        try:
            return download_day(day)
        except Exception as exc:  # network hiccup or slow response
            last_error = exc
            print(f"[kamis] attempt {attempt}/{KAMIS_RETRIES} for {day} failed: {exc}")
            time.sleep(3)
    raise last_error


def ingest(
    days_back: int = KAMIS_DAYS_BACK,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
) -> None:
    """Download a bounded inclusive date range of KAMIS prices."""
    if (start_date is None) != (end_date is None):
        raise ValueError("start_date and end_date must be provided together")

    if start_date is not None and end_date is not None:
        start = (
            date.fromisoformat(start_date)
            if isinstance(start_date, str)
            else start_date
        )
        end = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
        if start > end:
            raise ValueError("start_date must be on or before end_date")
        if (end - start).days + 1 > 31:
            raise ValueError("KAMIS ingestion is limited to 31 days per run")
    else:
        if days_back < 1 or days_back > 31:
            raise ValueError("days_back must be between 1 and 31")
        end = date.today()
        start = end - timedelta(days=days_back - 1)

    total_days = (end - start).days + 1
    frames = []
    failed_days = []
    for offset in range(total_days):
        current_day = start + timedelta(days=offset)
        day = current_day.isoformat()
        try:
            df = download_day_with_retries(day)
        except Exception as exc:
            failed_days.append((day, exc))
            continue
        if not df.empty:
            frames.append(df)
            print(f"[kamis] {day}: {len(df):,} rows")

    if failed_days:
        details = ", ".join(day for day, _ in failed_days)
        raise RuntimeError(
            f"KAMIS failed to download all requested dates: {details}"
        ) from failed_days[0][1]
    if not frames:
        raise RuntimeError("No KAMIS data downloaded. Check your internet connection.")

    all_prices = pd.concat(frames, ignore_index=True).drop_duplicates()
    save_to_landing(
        all_prices,
        "kamis",
        "kamis_prices.csv",
        start.isoformat(),
        end.isoformat(),
    )
    save_watermark("kamis", end.isoformat())
    print(
        f"[kamis] real data: {all_prices['survey_date'].nunique()} days, "
        f"{len(all_prices):,} rows"
    )

def main():
    print("[kamis] starting KAMIS ingestion...")
    ingest()

if __name__ == "__main__":
    main()