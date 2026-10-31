#!/bin/bash
# Runner script for Project 2: Enterprise Customer 360 Pipeline
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 1. Check virtual environment
if [ -f "../.venv/bin/activate" ]; then
    source ../.venv/bin/activate
elif [ -f "./.venv/bin/activate" ]; then
    source ./.venv/bin/activate
fi

# 2. Select python interpreter with pyspark
if [ -f "/opt/anaconda3/envs/spark_env/bin/python" ]; then
    PY_CMD="/opt/anaconda3/envs/spark_env/bin/python"
elif command -v python3 >/dev/null 2>&1 && python3 -c "import pyspark" >/dev/null 2>&1; then
    PY_CMD="python3"
elif [ -f "/opt/anaconda3/bin/python" ]; then
    PY_CMD="/opt/anaconda3/bin/python"
else
    PY_CMD="python3"
fi

echo "=========================================================================="
echo "Running Project 2: Enterprise Customer 360 Pipeline (23BTRDC002)"
echo "Using interpreter: $PY_CMD"
echo "=========================================================================="
$PY_CMD main.py
