#!/usr/bin/env bash
# Run the full Milestone 1 ingestion: KAMIS + KNBS + operational data.
#
# Usage:
#   bash run_all.sh                       # uses python3
#   bash run_all.sh /path/to/python       # uses a specific interpreter
#   PYTHON=/path/to/python bash run_all.sh
set -e
cd "$(dirname "$0")"

PYTHON="${1:-${PYTHON:-python3}}"
echo "Using Python: $PYTHON"
"$PYTHON" -u run_ingestion.py
