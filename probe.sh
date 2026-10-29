#!/usr/bin/env bash
set -u
echo "--- WSL python3 ---"
python3 - <<'PY'
try:
    import sys, pandas, numpy
    print("wsl python", sys.version.split()[0], "pandas", pandas.__version__)
except Exception as exc:
    print("wsl python missing deps:", exc)
PY
echo "--- Windows venv python ---"
WIN="/mnt/c/Users/Ibrahim Njuguna/Desktop/astronomer/kenya-retail/.venv/Scripts/python.exe"
if [ -x "$WIN" ]; then
  "$WIN" - <<'PY'
import sys, pandas, numpy
print("win venv python", sys.version.split()[0], "pandas", pandas.__version__)
PY
else
  echo "no windows venv at expected path"
fi
