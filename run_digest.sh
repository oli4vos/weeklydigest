#!/bin/bash
cd "$HOME/Projects/weeklydigest" || exit 1
LOG_FILE="$HOME/Projects/weeklydigest/logs/weekly.log"
"$HOME/Projects/weeklydigest/.venv/bin/python" scripts/run_weekly_digest.py --user-id 1 --days 7 --force >> "$LOG_FILE" 2>&1
