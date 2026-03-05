#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

KEEP_UP=0
LIVE_MODE=0

for arg in "$@"; do
  case "${arg}" in
    --keep-up)
      KEEP_UP=1
      ;;
    --live)
      LIVE_MODE=1
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      echo "Usage: scripts/e2e_test.sh [--keep-up] [--live]" >&2
      exit 1
      ;;
  esac
done

cleanup() {
  if [[ "${KEEP_UP}" -eq 0 ]]; then
    echo "Stopping stack (docker compose down -v)..."
    docker compose down -v || true
  else
    echo "Leaving stack running because --keep-up was set."
  fi
}
trap cleanup EXIT

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

echo "Starting stack..."
docker compose up -d --build

wait_for_postgres() {
  local retries=60
  local sleep_seconds=2
  local user="${POSTGRES_USER:-loader}"
  local database="${POSTGRES_DB:-commodity_lakehouse}"

  for ((i=1; i<=retries; i++)); do
    if docker compose exec -T postgres pg_isready -U "${user}" -d "${database}" >/dev/null 2>&1; then
      echo "Postgres is ready."
      return 0
    fi
    sleep "${sleep_seconds}"
  done
  echo "Postgres did not become ready in time." >&2
  return 1
}

wait_for_http() {
  local label="$1"
  local url="$2"
  local retries=60
  local sleep_seconds=2

  for ((i=1; i<=retries; i++)); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${label} is ready (${url})."
      return 0
    fi
    sleep "${sleep_seconds}"
  done
  echo "${label} did not become ready in time: ${url}" >&2
  return 1
}

wait_for_postgres
wait_for_http "Airflow" "http://localhost:8080/health"
wait_for_http "Marquez" "http://localhost:5000/api/v1/namespaces"

echo "Seeding deterministic raw-only sample data..."
docker compose exec -T airflow-webserver python /opt/airflow/scripts/seed_sample_data.py --raw-only

echo "Running e2e pytest assertions..."
docker compose exec -T airflow-webserver pytest /opt/airflow/tests/test_e2e.py -v

if [[ "${LIVE_MODE}" -eq 1 ]]; then
  echo "Running optional live DAG smoke test (commodity_etl)..."
  docker compose exec -T airflow-webserver airflow dags test commodity_etl "$(date +%F)"
fi

echo "E2E integration test completed successfully."
