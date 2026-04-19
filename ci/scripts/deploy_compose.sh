#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-}"
DEPLOY_PATH="${2:-/opt/product-cross-sell}"
COMPOSE_FILE="${3:-docker-compose.yml}"
DEPLOY_STACK="${4:-airflow-webserver airflow-scheduler streamlit producer broadcast_publisher realtime_offer_writer}"
DEPLOY_BRANCH="${5:-main}"

run_local() {
  echo "Deploy mode: local"
  docker compose -f "$COMPOSE_FILE" pull || true
  docker compose -f "$COMPOSE_FILE" up -d --build $DEPLOY_STACK
}

run_remote() {
  echo "Deploy mode: remote ($HOST)"

  ssh -o StrictHostKeyChecking=no "$HOST" \
    "set -euo pipefail; \
    cd '$DEPLOY_PATH'; \
    git fetch --all --prune; \
    git checkout '$DEPLOY_BRANCH'; \
    git pull --ff-only origin '$DEPLOY_BRANCH'; \
    docker compose -f '$COMPOSE_FILE' pull || true; \
    docker compose -f '$COMPOSE_FILE' up -d --build $DEPLOY_STACK"
}

if [[ -z "$HOST" ]]; then
  run_local
else
  run_remote
fi
