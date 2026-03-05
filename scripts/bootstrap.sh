#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

set -a
source .env
set +a

docker compose up -d --build

echo "Waiting for Postgres to become healthy..."
until docker compose exec -T postgres \
  pg_isready -U "${POSTGRES_USER:-loader}" -d "${POSTGRES_DB:-commodity_lakehouse}" >/dev/null 2>&1; do
  sleep 2
done

echo "Local stack is ready."
echo "Airflow UI:  http://localhost:8080 (airflow / airflow)"
echo "Marquez UI:  http://localhost:3000"
echo "Seed sample data: python3 scripts/seed_sample_data.py"
echo "Run GX checkpoint: scripts/run_gx_checkpoint.sh"
