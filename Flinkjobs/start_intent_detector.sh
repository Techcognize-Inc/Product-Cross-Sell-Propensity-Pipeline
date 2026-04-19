#!/usr/bin/env bash
set -euo pipefail

sleep 20

# Submit as a real Flink job to the shared JobManager so it is visible in the Flink UI.
exec /opt/flink/bin/flink run \
  -m flink-jobmanager:8081 \
  -py Flinkjobs/intent_detector.py