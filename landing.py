"""The one landing zone: every source saves its raw data here."""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import LANDING_DIR


def new_run_folder(source: str) -> Path:
    """Make a fresh folder for this run so old runs are never overwritten."""
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    folder = LANDING_DIR / source / stamp
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _write_manifest(folder: Path, manifest: dict) -> None:
    """Saves a small JSON file describing what we just saved."""
    with open(folder / "manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)


def save_to_landing(df, source, filename, date_from, date_to) -> Path:
    """Save ONE table (e.g. KAMIS prices) to the landing zone."""
    folder = new_run_folder(source)
    df.to_csv(folder / filename, index=False)

    manifest = {
        "source": source,
        "saved_at": folder.name,
        "date_from": date_from,
        "date_to": date_to,
        "rows": len(df),
        "columns": list(df.columns),
        "file": filename,
    }
    _write_manifest(folder, manifest)
    print(f"[{source}] saved {len(df):,} rows -> {folder}")
    return folder


def save_many_to_landing(tables, source, date_from, date_to) -> Path:
    """Save SEVERAL tables into ONE run folder (used by operational data)."""
    folder = new_run_folder(source)
    table_rows = {}

    for filename, df in tables.items():
        df.to_csv(folder / filename, index=False)
        table_rows[filename] = len(df)
        print(f"[{source}] saved {len(df):,} rows -> {folder / filename}")

    manifest = {
        "source": source,
        "saved_at": folder.name,
        "date_from": date_from,
        "date_to": date_to,
        "tables": table_rows,
    }
    _write_manifest(folder, manifest)
    return folder


def save_watermark(source, last_date) -> None:
    """Remember the last date we loaded, so next time we can continue."""
    folder = LANDING_DIR / source
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "last_date.txt").write_text(str(last_date), encoding="utf-8")


def read_watermark(source):
    """Read the last loaded date (or None if this is the first run)."""
    path = LANDING_DIR / source / "last_date.txt"
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return None
