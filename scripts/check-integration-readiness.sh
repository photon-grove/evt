#!/usr/bin/env bash
set -euo pipefail

endpoint="${AWS_ENDPOINT_URL:-http://localhost:4566}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --endpoint)
      endpoint="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if ! curl -fsS "$endpoint/" >/dev/null 2>&1; then
  echo "local AWS emulator is not reachable at $endpoint" >&2
  echo "start Moto or LocalStack, then run: terraform -chdir=infra/local apply -auto-approve" >&2
  exit 1
fi

echo "local AWS emulator is reachable at $endpoint"
