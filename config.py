"""All project settings in one place. Change things here, not in the sources."""

from pathlib import Path

# Where everything lives
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LANDING_DIR = DATA_DIR / "landing"

# Operational demo data
START_DATE = "2025-04-01"
SEED = 42  # same seed = same data every time

# ---------- Real source 1: KAMIS market prices ----------
KAMIS_URL = "https://kamis.kilimo.go.ke/site/market_search"
KAMIS_DAYS_BACK = 3      # how many recent days to download
KAMIS_TIMEOUT = 30       # seconds per request
KAMIS_RETRIES = 2        # try each day this many times

# ---------- Real source 2: KNBS CPI (CPI Explorer API) ----------

KNBS_REPORTS_PAGE = "https://www.knbs.or.ke/cpi-and-inflation-rates/"
KNBS_TIMEOUT = 30  # seconds per request
KNBS_RETRIES = 1  # try each request this many times