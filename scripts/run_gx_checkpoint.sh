#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

CHECKPOINT_NAME="${1:-raw_validation}"
GX_ROOT="${GREAT_EXPECTATIONS_HOME:-${ROOT_DIR}/great_expectations}"

python3 -m great_expectations checkpoint run "${CHECKPOINT_NAME}" --config "${GX_ROOT}/great_expectations.yml"
