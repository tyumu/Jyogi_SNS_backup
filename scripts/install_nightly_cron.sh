#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUNNER="$PROJECT_DIR/scripts/run_main_nightly.sh"

chmod +x "$RUNNER"

# 毎晩 02:00 実行
CRON_LINE="0 2 * * * $RUNNER"

TMP_CRON="$(mktemp)"
crontab -l 2>/dev/null | grep -v "$RUNNER" > "$TMP_CRON" || true
echo "$CRON_LINE" >> "$TMP_CRON"
crontab "$TMP_CRON"
rm -f "$TMP_CRON"

echo "cron 登録完了: $CRON_LINE"
echo "確認: crontab -l"
