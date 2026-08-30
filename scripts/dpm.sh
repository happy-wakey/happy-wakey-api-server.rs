#!/usr/bin/env bash
set -euo pipefail

readonly expected_revision="027c81892cafc3693b550eaa74940a17b1705235"
readonly schema_path="${1:-schema/schema.sql}"
readonly dpm_bin="${DPM_BIN:-declarative-postgres-migrate}"

if [[ "${HAPPY_WAKEY_DPM_REVISION:-}" != "${expected_revision}" ]]; then
  echo "Refusing migration review: HAPPY_WAKEY_DPM_REVISION must equal ${expected_revision}" >&2
  exit 64
fi

exec "${dpm_bin}" plan --desired "${schema_path}"
