#!/usr/bin/env bash
set -euo pipefail

sleep 20

python3 Flinkjobs/intent_detector.py &
app_pid=$!

tail_internal_logs() {
  declare -A tailed_files=()
  while kill -0 "$app_pid" 2>/dev/null; do
    for log_file in /opt/flink/log/flink-flink-python-*.log /tmp/python-dist-*/flink-python-udf-boot.log; do
      if [ -f "$log_file" ] && [ -z "${tailed_files[$log_file]:-}" ]; then
        tailed_files[$log_file]=1
        tail -n +1 -F "$log_file" 2>/dev/null &
      fi
    done
    sleep 5
  done
}

tail_internal_logs &
tail_pid=$!

set +e
wait "$app_pid"
app_status=$?
set -e

kill "$tail_pid" 2>/dev/null || true
wait "$tail_pid" 2>/dev/null || true

exit "$app_status"