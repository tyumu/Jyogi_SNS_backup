#!/usr/bin/env bash
set -euo pipefail

# プロジェクトルートへ移動
cd "$(dirname "$0")/.."

mkdir -p artifacts/logs

# venv があれば優先利用、なければシステム Python を利用
if [[ -x "venv/bin/python" ]]; then
  PYTHON_BIN="venv/bin/python"
else
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" main.py >> artifacts/logs/main_nightly.log 2>&1
